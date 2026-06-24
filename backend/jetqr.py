import httpx
import uuid
import asyncio
import logging
from config import settings

logger = logging.getLogger(__name__)

# Persistent client with connection pooling — reuses TCP connections
_client: httpx.AsyncClient | None = None

def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=6,
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        )
    return _client


async def create_invoice(plate: str) -> dict:
    mis_payment_id = f"PRK-{plate}-{uuid.uuid4().hex[:8].upper()}"
    payload = {
        "merchant_id": settings.JETQR_MERCHANT_ID,
        "store_id": settings.JETQR_STORE_ID,
        "terminal_id": settings.JETQR_TERMINAL_ID,
        "mis_terminal_id": settings.JETQR_MIS_TERMINAL_ID,
        "mis_payment_id": mis_payment_id,
        "mis_amount": settings.JETQR_AMOUNT,
    }
    headers = {
        "X-Api-Key": settings.JETQR_API_KEY,
        "Content-Type": "application/json",
    }

    # Retry up to 3 times on failure
    for attempt in range(3):
        try:
            response = await get_client().post(
                f"{settings.JETQR_BASE_URL}/api/v1/merchant/invoice",
                headers=headers,
                json=payload,
            )
            data = response.json()
            if data.get("type") == "SUCCESS":
                return {
                    "success": True,
                    "invoice_id": data["invoice_id"],
                    "mis_payment_id": mis_payment_id,
                }
            logger.warning(f"create_invoice attempt {attempt+1} failed: {data}")
        except Exception as e:
            logger.warning(f"create_invoice attempt {attempt+1} error: {e}")
        if attempt < 2:
            await asyncio.sleep(1)

    return {"success": False, "error": "max retries exceeded"}


async def check_invoice(invoice_id: str) -> dict:
    for attempt in range(2):
        try:
            response = await get_client().get(
                f"{settings.JETQR_BASE_URL}/api/v1/merchant/invoice",
                headers={"X-Api-Key": settings.JETQR_API_KEY},
                params={"invoiceId": invoice_id},
            )
            data = response.json()
        except Exception as e:
            logger.warning(f"check_invoice attempt {attempt+1} error: {e}")
            return {"paid": False, "pending": True}

        code = data.get("code")
        if code == 200:
            return {
                "paid": True,
                "phone": data.get("phone_number"),
                "amount": data.get("amount_arrived"),
                "bank": data.get("bank_name"),
            }
        elif code == 202:
            return {"paid": False, "pending": True}
        elif response.status_code >= 500 and attempt == 0:
            await asyncio.sleep(0.5)
            continue
        else:
            return {"paid": False, "pending": False, "error": True}
    return {"paid": False, "pending": False, "error": True}
