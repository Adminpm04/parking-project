import httpx
import uuid
from config import settings

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

    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{settings.JETQR_BASE_URL}/api/v1/merchant/invoice",
            headers={
                "X-Api-Key": settings.JETQR_API_KEY,
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=10,
        )
        data = response.json()

    if data.get("type") == "SUCCESS":
        return {
            "success": True,
            "invoice_id": data["invoice_id"],
            "mis_payment_id": mis_payment_id,
        }
    return {"success": False, "error": data.get("message")}


async def check_invoice(invoice_id: str) -> dict:
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.JETQR_BASE_URL}/api/v1/merchant/invoice",
            headers={"X-Api-Key": settings.JETQR_API_KEY},
            params={"invoiceId": invoice_id},
            timeout=10,
        )
        data = response.json()

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
    else:
        return {"paid": False, "pending": False, "error": True}
