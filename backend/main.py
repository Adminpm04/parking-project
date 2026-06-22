import logging
logging.basicConfig(level=logging.INFO)

from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Optional
import asyncio
import json
import os

from database import get_db, init_db, ParkingSession, Abonement, Blacklist, OpenType, PaymentStatus
from jetqr import create_invoice, check_invoice
from dahua import open_entry_barrier, open_exit_barrier
from config import settings

app = FastAPI(title="Parking System")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# WebSocket connections for real-time updates
exit_terminal_clients: list[WebSocket] = []

# Plates currently being processed for exit payment (prevents duplicate invoices)
_exit_in_progress: set[str] = set()

@app.on_event("startup")
async def startup():
    init_db()

# ─── DAHUA CAMERA WEBHOOK ────────────────────────────────────────────────────

def _log_structure(data, prefix=""):
    """Log data structure without image content for debugging."""
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, str) and len(v) > 100:
                logging.info(f"{prefix}{k}: [STRING len={len(v)}]")
            elif isinstance(v, (dict, list)):
                logging.info(f"{prefix}{k}:")
                _log_structure(v, prefix + "  ")
            else:
                logging.info(f"{prefix}{k}: {v}")
    elif isinstance(data, list):
        for i, item in enumerate(data[:3]):
            logging.info(f"{prefix}[{i}]:")
            _log_structure(item, prefix + "  ")


def _find_in_dict(data, target_keys, max_depth=6):
    """Recursively search for any of target_keys in nested dict/list."""
    if max_depth == 0:
        return None
    if isinstance(data, dict):
        for key in target_keys:
            val = data.get(key)
            if val and isinstance(val, str) and 2 <= len(val) <= 20:
                return val
        for v in data.values():
            if isinstance(v, (dict, list)):
                result = _find_in_dict(v, target_keys, max_depth - 1)
                if result:
                    return result
    elif isinstance(data, list):
        for item in data:
            result = _find_in_dict(item, target_keys, max_depth - 1)
            if result:
                return result
    return None


def extract_plate(data: dict) -> str:
    """Extract plate number from Dahua ITSAPI V1.19 format."""
    plate_keys = ("PlateNumber", "Plate", "plate", "LicensePlate", "AutoPlate")
    val = _find_in_dict(data, plate_keys)
    if val:
        return str(val).upper().strip()

    # Last resort: parse from PicName like "0300TB10-20260622094113.jpg"
    picname = _find_in_dict(data, ("PicName",))
    if picname and "-" in str(picname):
        candidate = str(picname).split("-")[0]
        if 3 <= len(candidate) <= 15:
            return candidate.upper().strip()

    return ""


# Dahua ITSAPI heartbeat
@app.post("/NotificationInfo/KeepAlive")
async def itsapi_keepalive(data: dict):
    return {"Response": "OK"}


async def _handle_entry(plate: str, db: Session):
    if not plate:
        return {"Response": "OK"}

    blocked = db.query(Blacklist).filter(Blacklist.plate == plate).first()
    if blocked:
        return {"action": "deny", "reason": "blacklisted"}

    abonement = db.query(Abonement).filter(
        Abonement.plate == plate,
        Abonement.is_active == True,
        Abonement.valid_until > datetime.utcnow()
    ).first()

    session = ParkingSession(
        plate=plate,
        entry_time=datetime.utcnow(),
        entry_type=OpenType.auto,
        payment_status=PaymentStatus.free if abonement else PaymentStatus.pending,
    )
    db.add(session)
    db.commit()

    await open_entry_barrier()
    return {"action": "open", "session_id": session.id}


async def _handle_exit(plate: str, db: Session):
    if not plate:
        return {"Response": "OK"}

    session = db.query(ParkingSession).filter(
        ParkingSession.plate == plate,
        ParkingSession.is_active == True,
    ).order_by(ParkingSession.entry_time.desc()).first()

    if not session:
        session = ParkingSession(
            plate=plate,
            entry_time=datetime.utcnow(),
            entry_type=OpenType.manual,
        )
        db.add(session)
        db.commit()

    abonement = db.query(Abonement).filter(
        Abonement.plate == plate,
        Abonement.is_active == True,
        Abonement.valid_until > datetime.utcnow()
    ).first()

    if abonement:
        session.exit_time = datetime.utcnow()
        session.exit_type = OpenType.auto
        session.payment_status = PaymentStatus.free
        session.is_active = False
        db.commit()
        await open_exit_barrier()
        return {"action": "open", "reason": "abonement"}

    # Deduplicate: camera sends 3 triggers per detection
    if session.invoice_id or plate in _exit_in_progress:
        return {"action": "wait_payment", "invoice_id": session.invoice_id}

    _exit_in_progress.add(plate)
    asyncio.create_task(_exit_payment_flow(plate, session.id))
    return {"action": "processing"}


@app.post("/api/camera/entry")
async def camera_entry(data: dict, db: Session = Depends(get_db)):
    plate = extract_plate(data)
    logging.info(f"camera/entry plate='{plate}'")
    if not plate:
        _log_structure(data)
    return await _handle_entry(plate, db)


@app.post("/api/camera/exit")
async def camera_exit(data: dict, db: Session = Depends(get_db)):
    plate = extract_plate(data)
    logging.info(f"camera/exit plate='{plate}'")
    if not plate:
        _log_structure(data)
    return await _handle_exit(plate, db)


# Fallback: camera sends to default /NotificationInfo/TollgateInfo path
@app.post("/NotificationInfo/TollgateInfo")
async def itsapi_tollgate(request: Request, data: dict, db: Session = Depends(get_db)):
    logging.info(f"TollgateInfo from {request.client.host}: {data}")
    plate = extract_plate(data)
    client_ip = request.client.host
    params = data.get("Params", {})
    direction = str(params.get("Direction", "")).lower()
    is_exit = client_ip == settings.CAMERA_EXIT_IP or "leave" in direction
    if is_exit:
        return await _handle_exit(plate, db)
    return await _handle_entry(plate, db)


# ─── PAYMENT POLLING ─────────────────────────────────────────────────────────

async def _exit_payment_flow(plate: str, session_id: int):
    """Create invoice and start polling — runs in background so camera gets instant response."""
    from database import SessionLocal
    try:
        result = await create_invoice(plate)
        if not result["success"]:
            logging.error(f"Failed to create invoice for {plate}")
            return

        db = SessionLocal()
        try:
            session = db.query(ParkingSession).filter(ParkingSession.id == session_id).first()
            if session:
                session.invoice_id = result["invoice_id"]
                session.mis_payment_id = result["mis_payment_id"]
                session.amount = settings.JETQR_AMOUNT
                db.commit()
        finally:
            db.close()

        await notify_exit_terminal({
            "type": "show_payment",
            "plate": plate,
            "amount": settings.JETQR_AMOUNT,
            "invoice_id": result["invoice_id"],
            "session_id": session_id,
        })

        await poll_payment(session_id, result["invoice_id"])
    finally:
        _exit_in_progress.discard(plate)


async def poll_payment(session_id: int, invoice_id: str):
    """Poll JetQR every 2 seconds until payment confirmed."""
    from database import SessionLocal
    max_attempts = 150  # 5 minutes

    for i in range(max_attempts):
        await asyncio.sleep(1 if i == 0 else 2)
        result = await check_invoice(invoice_id)

        if result.get("paid"):
            db = SessionLocal()
            try:
                session = db.query(ParkingSession).filter(
                    ParkingSession.id == session_id
                ).first()
                if session:
                    session.exit_time = datetime.utcnow()
                    session.exit_type = OpenType.auto
                    session.payment_status = PaymentStatus.paid
                    session.phone_number = result.get("phone")
                    session.is_active = False
                    db.commit()

                    await open_exit_barrier()
                    await notify_exit_terminal({
                        "type": "payment_success",
                        "session_id": session_id,
                        "plate": session.plate,
                    })
            finally:
                db.close()
            return

        if result.get("error"):
            break


# ─── GUARD PANEL ─────────────────────────────────────────────────────────────

@app.post("/api/guard/open-entry")
async def guard_open_entry(data: dict, db: Session = Depends(get_db)):
    """Guard manually opens entry barrier (cash payment)."""
    session = ParkingSession(
        plate=data.get("plate", "MANUAL"),
        entry_time=datetime.utcnow(),
        entry_type=OpenType.manual,
        payment_status=PaymentStatus.paid,
        amount=settings.JETQR_AMOUNT,
    )
    db.add(session)
    db.commit()
    await open_entry_barrier()
    return {"success": True, "session_id": session.id}


@app.post("/api/guard/open-exit")
async def guard_open_exit(data: dict, db: Session = Depends(get_db)):
    """Guard manually opens exit barrier (cash payment)."""
    plate = data.get("plate", "MANUAL")
    session = db.query(ParkingSession).filter(
        ParkingSession.plate == plate,
        ParkingSession.is_active == True,
    ).order_by(ParkingSession.entry_time.desc()).first()

    if session:
        session.exit_time = datetime.utcnow()
        session.exit_type = OpenType.manual
        session.payment_status = PaymentStatus.paid
        session.amount = settings.JETQR_AMOUNT
        session.is_active = False
        db.commit()

    await open_exit_barrier()
    return {"success": True}


# ─── ADMIN API ────────────────────────────────────────────────────────────────

@app.get("/api/admin/sessions")
async def get_sessions(db: Session = Depends(get_db)):
    sessions = db.query(ParkingSession).order_by(
        ParkingSession.entry_time.desc()
    ).limit(100).all()
    return [
        {
            "id": s.id,
            "plate": s.plate,
            "entry_time": s.entry_time.isoformat() if s.entry_time else None,
            "exit_time": s.exit_time.isoformat() if s.exit_time else None,
            "entry_type": s.entry_type,
            "exit_type": s.exit_type,
            "payment_status": s.payment_status,
            "amount": s.amount,
            "is_active": s.is_active,
            "phone_number": s.phone_number,
        }
        for s in sessions
    ]


@app.get("/api/admin/stats")
async def get_stats(db: Session = Depends(get_db)):
    from sqlalchemy import func
    today = datetime.utcnow().date()

    total_today = db.query(ParkingSession).filter(
        func.date(ParkingSession.entry_time) == today
    ).count()

    paid_electronic = db.query(ParkingSession).filter(
        func.date(ParkingSession.entry_time) == today,
        ParkingSession.payment_status == PaymentStatus.paid,
        ParkingSession.exit_type == OpenType.auto,
    ).count()

    paid_cash = db.query(ParkingSession).filter(
        func.date(ParkingSession.entry_time) == today,
        ParkingSession.payment_status == PaymentStatus.paid,
        ParkingSession.exit_type == OpenType.manual,
    ).count()

    currently_inside = db.query(ParkingSession).filter(
        ParkingSession.is_active == True
    ).count()

    revenue_electronic = paid_electronic * settings.JETQR_AMOUNT
    revenue_cash = paid_cash * settings.JETQR_AMOUNT

    return {
        "total_today": total_today,
        "currently_inside": currently_inside,
        "paid_electronic": paid_electronic,
        "paid_cash": paid_cash,
        "revenue_electronic": revenue_electronic,
        "revenue_cash": revenue_cash,
        "revenue_total": revenue_electronic + revenue_cash,
    }


@app.get("/api/admin/active")
async def get_active(db: Session = Depends(get_db)):
    sessions = db.query(ParkingSession).filter(
        ParkingSession.is_active == True
    ).order_by(ParkingSession.entry_time.desc()).all()
    return [
        {
            "id": s.id,
            "plate": s.plate,
            "entry_time": s.entry_time.isoformat(),
            "duration_minutes": int((datetime.utcnow() - s.entry_time).total_seconds() / 60),
        }
        for s in sessions
    ]


# ─── ABONEMENT ───────────────────────────────────────────────────────────────

@app.post("/api/admin/abonement")
async def add_abonement(data: dict, db: Session = Depends(get_db)):
    from datetime import timedelta
    existing = db.query(Abonement).filter(Abonement.plate == data["plate"]).first()
    valid_until = datetime.utcnow() + timedelta(days=data.get("days", 30))

    if existing:
        existing.valid_until = valid_until
        existing.owner_name = data.get("owner_name")
        existing.is_active = True
    else:
        ab = Abonement(
            plate=data["plate"].upper(),
            owner_name=data.get("owner_name"),
            valid_until=valid_until,
        )
        db.add(ab)
    db.commit()
    return {"success": True}


# ─── BLACKLIST ────────────────────────────────────────────────────────────────

@app.post("/api/admin/blacklist")
async def add_blacklist(data: dict, db: Session = Depends(get_db)):
    bl = Blacklist(plate=data["plate"].upper(), reason=data.get("reason"))
    db.add(bl)
    db.commit()
    return {"success": True}


# ─── WEBSOCKET (exit terminal real-time) ─────────────────────────────────────

@app.websocket("/ws/exit-terminal")
async def exit_terminal_ws(websocket: WebSocket):
    await websocket.accept()
    exit_terminal_clients.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        exit_terminal_clients.remove(websocket)


async def notify_exit_terminal(data: dict):
    dead = []
    for ws in exit_terminal_clients:
        try:
            await ws.send_text(json.dumps(data))
        except Exception:
            dead.append(ws)
    for ws in dead:
        exit_terminal_clients.remove(ws)


# ─── BARRIER TEST ────────────────────────────────────────────────────────────

@app.get("/api/test/barrier/{direction}")
async def test_barrier(direction: str):
    """Test barrier open directly and return detailed result."""
    from dahua import open_barrier
    import httpx

    ip = settings.BARRIER_ENTRY_IP if direction == "entry" else settings.BARRIER_EXIT_IP
    results = []

    endpoints = [
        "/cgi-bin/accessControl.cgi?action=openDoor&channel=1&UserID=0",
        "/cgi-bin/barrierControl.cgi?action=open&channel=0",
        "/cgi-bin/gateControl.cgi?action=open",
        "/cgi-bin/trafficBarrier.cgi?action=open",
    ]

    async with httpx.AsyncClient(verify=False, auth=httpx.DigestAuth(settings.CAMERA_USER, settings.CAMERA_PASSWORD)) as client:
        for ep in endpoints:
            try:
                r = await client.get(f"https://{ip}{ep}", timeout=5)
                results.append({"endpoint": ep, "status": r.status_code, "response": r.text[:100]})
            except Exception as e:
                results.append({"endpoint": ep, "error": str(e)})

    return {"ip": ip, "results": results}


# ─── QR CODE GENERATOR ───────────────────────────────────────────────────────

@app.get("/api/qr/{invoice_id}")
async def generate_qr(invoice_id: str):
    import qrcode
    import io
    from fastapi.responses import StreamingResponse

    qr = qrcode.QRCode(version=1, box_size=10, border=2)
    qr.add_data(invoice_id)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return StreamingResponse(buf, media_type="image/png")


# ─── FRONTEND PAGES ──────────────────────────────────────────────────────────

frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")

@app.get("/", response_class=HTMLResponse)
async def exit_terminal():
    return FileResponse(os.path.join(frontend_dir, "exit_terminal.html"))

@app.get("/admin", response_class=HTMLResponse)
async def admin():
    return FileResponse(os.path.join(frontend_dir, "admin.html"))

@app.get("/guard", response_class=HTMLResponse)
async def guard():
    return FileResponse(os.path.join(frontend_dir, "guard.html"))
