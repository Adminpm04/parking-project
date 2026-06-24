import logging
import time
logging.basicConfig(level=logging.INFO)

from fastapi import FastAPI, Depends, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from datetime import datetime
import asyncio
import json
import os

from database import get_db, init_db, ParkingSession, Abonement, Blacklist, OpenType, PaymentStatus
from jetqr import create_invoice, check_invoice
from dahua import open_entry_barrier, open_exit_barrier
from config import settings

app = FastAPI(title="Parking System")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

exit_terminal_clients: list[WebSocket] = []
_exit_in_progress: set[str] = set()
_exit_lock = asyncio.Lock()

# Blacklist + active abonements cached in memory, TTL 60s
# Avoids 2 DB round-trips on every camera trigger.
_plate_cache: dict = {"blacklist": set(), "abonements": set(), "ts": 0.0}


def _invalidate_cache() -> None:
    _plate_cache["ts"] = 0.0


def _cache_refresh_sync(db: Session) -> None:
    """Reload blacklist/abonement sets from DB.  Must run inside run_in_executor."""
    if time.time() - _plate_cache["ts"] < 60:
        return
    _plate_cache["blacklist"] = {r[0] for r in db.query(Blacklist.plate).all()}
    _plate_cache["abonements"] = {r[0] for r in db.query(Abonement.plate).filter(
        Abonement.is_active == True,
        Abonement.valid_until > datetime.utcnow()
    ).all()}
    _plate_cache["ts"] = time.time()


async def _db(fn):
    """Run a synchronous SQLAlchemy call off the event loop (thread pool)."""
    return await asyncio.get_event_loop().run_in_executor(None, fn)


# ─── STARTUP ─────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    init_db()
    asyncio.create_task(_ws_heartbeat())


async def _ws_heartbeat():
    """Ping every 25 s — keeps the terminal WebSocket alive through proxies and NAT."""
    while True:
        await asyncio.sleep(25)
        dead = []
        for ws in list(exit_terminal_clients):
            try:
                await ws.send_text('{"type":"ping"}')
            except Exception:
                dead.append(ws)
        for ws in dead:
            if ws in exit_terminal_clients:
                exit_terminal_clients.remove(ws)


# ─── DAHUA HELPERS ───────────────────────────────────────────────────────────

def _log_structure(data, prefix=""):
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
    if max_depth == 0:
        return None
    if isinstance(data, dict):
        for key in target_keys:
            val = data.get(key)
            if val and isinstance(val, str) and 4 <= len(val) <= 20:
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
    plate_keys = ("PlateNumber", "Plate", "plate", "LicensePlate", "AutoPlate")
    val = _find_in_dict(data, plate_keys)
    if val:
        return str(val).upper().strip()
    picname = _find_in_dict(data, ("PicName",))
    if picname and "-" in str(picname):
        candidate = str(picname).split("-")[0]
        if 3 <= len(candidate) <= 15:
            return candidate.upper().strip()
    return ""


@app.post("/NotificationInfo/KeepAlive")
async def itsapi_keepalive(data: dict):
    return {"Response": "OK"}


# ─── ENTRY ───────────────────────────────────────────────────────────────────

async def _handle_entry(plate: str, db: Session):
    if not plate:
        return {"Response": "OK"}

    def _run():
        _cache_refresh_sync(db)
        if plate in _plate_cache["blacklist"]:
            return None, True  # (session_id, blocked)
        has_ab = plate in _plate_cache["abonements"]
        s = ParkingSession(
            plate=plate,
            entry_time=datetime.utcnow(),
            entry_type=OpenType.auto,
            payment_status=PaymentStatus.free if has_ab else PaymentStatus.pending,
        )
        db.add(s)
        db.commit()
        db.refresh(s)
        return s.id, False

    session_id, blocked = await _db(_run)
    if blocked:
        return {"action": "deny", "reason": "blacklisted"}

    await open_entry_barrier()
    return {"action": "open", "session_id": session_id}


@app.post("/api/camera/entry")
async def camera_entry(data: dict, db: Session = Depends(get_db)):
    plate = extract_plate(data)
    logging.info(f"camera/entry plate='{plate}'")
    if not plate:
        _log_structure(data)
    return await _handle_entry(plate, db)


# ─── EXIT ────────────────────────────────────────────────────────────────────

async def _handle_exit(plate: str, db: Session):
    if not plate:
        return {"Response": "OK"}

    def _load():
        _cache_refresh_sync(db)
        s = db.query(ParkingSession).filter(
            ParkingSession.plate == plate,
            ParkingSession.is_active == True,
        ).order_by(ParkingSession.entry_time.desc()).first()
        if not s:
            s = ParkingSession(plate=plate, entry_time=datetime.utcnow(), entry_type=OpenType.manual)
            db.add(s)
            db.commit()
            db.refresh(s)
        return s.id, s.invoice_id, plate in _plate_cache["abonements"]

    session_id, invoice_id, has_abonement = await _db(_load)

    if has_abonement:
        def _close():
            s = db.query(ParkingSession).filter(ParkingSession.id == session_id).first()
            if s:
                s.exit_time = datetime.utcnow()
                s.exit_type = OpenType.auto
                s.payment_status = PaymentStatus.free
                s.is_active = False
                db.commit()
        await _db(_close)
        await open_exit_barrier()
        return {"action": "open", "reason": "abonement"}

    async with _exit_lock:
        if invoice_id or plate in _exit_in_progress:
            return {"action": "wait_payment", "invoice_id": invoice_id}
        _exit_in_progress.add(plate)

    asyncio.create_task(_exit_payment_flow(plate, session_id))
    return {"action": "processing"}


@app.post("/api/camera/exit")
async def camera_exit(data: dict, db: Session = Depends(get_db)):
    plate = extract_plate(data)
    logging.info(f"camera/exit plate='{plate}'")
    if not plate:
        _log_structure(data)
    return await _handle_exit(plate, db)


@app.post("/NotificationInfo/TollgateInfo")
async def itsapi_tollgate(request: Request, data: dict, db: Session = Depends(get_db)):
    logging.info(f"TollgateInfo from {request.client.host}: {data}")
    plate = extract_plate(data)
    params = data.get("Params", {})
    direction = str(params.get("Direction", "")).lower()
    is_exit = request.client.host == settings.CAMERA_EXIT_IP or "leave" in direction
    if is_exit:
        return await _handle_exit(plate, db)
    return await _handle_entry(plate, db)


# ─── PAYMENT ─────────────────────────────────────────────────────────────────

async def _exit_payment_flow(plate: str, session_id: int):
    from database import SessionLocal
    try:
        result = await create_invoice(plate)
        if not result["success"]:
            logging.error(f"Invoice creation failed for {plate} after retries")
            await notify_exit_terminal({"type": "payment_error", "plate": plate})
            return

        def _save_invoice():
            db = SessionLocal()
            try:
                s = db.query(ParkingSession).filter(ParkingSession.id == session_id).first()
                if s:
                    s.invoice_id = result["invoice_id"]
                    s.mis_payment_id = result["mis_payment_id"]
                    s.amount = settings.JETQR_AMOUNT
                    db.commit()
            finally:
                db.close()

        await _db(_save_invoice)
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


async def _save_paid_session(session_id: int, result: dict):
    def _run():
        from database import SessionLocal
        db = SessionLocal()
        try:
            s = db.query(ParkingSession).filter(ParkingSession.id == session_id).first()
            if s:
                s.exit_time = datetime.utcnow()
                s.exit_type = OpenType.auto
                s.payment_status = PaymentStatus.paid
                s.phone_number = result.get("phone")
                s.is_active = False
                db.commit()
        finally:
            db.close()
    await _db(_run)


async def poll_payment(session_id: int, invoice_id: str):
    """Poll JetQR every 2 s until payment confirmed (max 5 min). Docs warn: faster polling blocks the terminal."""
    max_attempts = 150  # 5 min at 2s
    paid = False
    partial = False

    for _ in range(max_attempts):
        await asyncio.sleep(2)
        result = await check_invoice(invoice_id)

        if result.get("paid"):
            amount_paid = float(result.get("amount") or 0)
            if amount_paid < settings.JETQR_AMOUNT:
                partial = True
                logging.warning(f"Partial payment session {session_id}: paid {amount_paid}, required {settings.JETQR_AMOUNT}")
                await notify_exit_terminal({
                    "type": "partial_payment",
                    "paid": amount_paid,
                    "required": settings.JETQR_AMOUNT,
                    "session_id": session_id,
                })
                break
            paid = True
            await asyncio.gather(
                open_exit_barrier(),
                _save_paid_session(session_id, result),
            )
            def _get_plate():
                from database import SessionLocal
                db = SessionLocal()
                try:
                    s = db.query(ParkingSession).filter(ParkingSession.id == session_id).first()
                    return s.plate if s else ""
                finally:
                    db.close()
            plate = await _db(_get_plate)
            await notify_exit_terminal({"type": "payment_success", "session_id": session_id, "plate": plate})
            return

        if result.get("error"):
            break

    if not paid:
        def _reset():
            from database import SessionLocal
            db = SessionLocal()
            try:
                s = db.query(ParkingSession).filter(ParkingSession.id == session_id).first()
                if s:
                    s.invoice_id = None
                    s.mis_payment_id = None
                    db.commit()
                    return s.id
            finally:
                db.close()
        sid = await _db(_reset)
        if partial:
            logging.info(f"Invoice reset after partial payment, session {sid}")
        else:
            if sid:
                logging.info(f"Payment timeout/error for session {sid}, invoice reset")
            await notify_exit_terminal({"type": "payment_timeout", "session_id": session_id})


# ─── GUARD ───────────────────────────────────────────────────────────────────

@app.post("/api/guard/open-entry")
async def guard_open_entry(data: dict, db: Session = Depends(get_db)):
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


# ─── ADMIN ───────────────────────────────────────────────────────────────────

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
    currently_inside = db.query(ParkingSession).filter(ParkingSession.is_active == True).count()

    return {
        "total_today": total_today,
        "currently_inside": currently_inside,
        "paid_electronic": paid_electronic,
        "paid_cash": paid_cash,
        "revenue_electronic": paid_electronic * settings.JETQR_AMOUNT,
        "revenue_cash": paid_cash * settings.JETQR_AMOUNT,
        "revenue_total": (paid_electronic + paid_cash) * settings.JETQR_AMOUNT,
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
        db.add(Abonement(
            plate=data["plate"].upper(),
            owner_name=data.get("owner_name"),
            valid_until=valid_until,
        ))
    db.commit()
    _invalidate_cache()
    return {"success": True}


# ─── BLACKLIST ────────────────────────────────────────────────────────────────

@app.post("/api/admin/blacklist")
async def add_blacklist(data: dict, db: Session = Depends(get_db)):
    db.add(Blacklist(plate=data["plate"].upper(), reason=data.get("reason")))
    db.commit()
    _invalidate_cache()
    return {"success": True}


# ─── WEBSOCKET ───────────────────────────────────────────────────────────────

@app.websocket("/ws/exit-terminal")
async def exit_terminal_ws(websocket: WebSocket):
    await websocket.accept()
    exit_terminal_clients.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        if websocket in exit_terminal_clients:
            exit_terminal_clients.remove(websocket)


async def notify_exit_terminal(data: dict):
    dead = []
    for ws in list(exit_terminal_clients):
        try:
            await ws.send_text(json.dumps(data))
        except Exception:
            dead.append(ws)
    for ws in dead:
        if ws in exit_terminal_clients:
            exit_terminal_clients.remove(ws)


# ─── BARRIER TEST ────────────────────────────────────────────────────────────

@app.get("/api/test/barrier/{direction}")
async def test_barrier(direction: str):
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


# ─── QR GENERATOR ────────────────────────────────────────────────────────────

@app.get("/api/qr/{invoice_id}")
async def generate_qr(invoice_id: str):
    import qrcode, io
    qr = qrcode.QRCode(version=1, box_size=10, border=2)
    qr.add_data(invoice_id)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return StreamingResponse(buf, media_type="image/png")


# ─── FRONTEND ────────────────────────────────────────────────────────────────

frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")
app.mount("/static", StaticFiles(directory=frontend_dir), name="static")

@app.get("/", response_class=HTMLResponse)
async def exit_terminal():
    return FileResponse(os.path.join(frontend_dir, "exit_terminal.html"))

@app.get("/admin", response_class=HTMLResponse)
async def admin():
    return FileResponse(os.path.join(frontend_dir, "admin.html"))

@app.get("/guard", response_class=HTMLResponse)
async def guard():
    return FileResponse(os.path.join(frontend_dir, "guard.html"))
