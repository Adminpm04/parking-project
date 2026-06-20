import httpx
from config import settings

async def open_barrier(camera_ip: str) -> bool:
    """Send open command to Dahua barrier via camera controller."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"http://{camera_ip}/cgi-bin/accessControl.cgi",
                params={"action": "openDoor", "channel": 1, "UserID": "0"},
                auth=(settings.CAMERA_USER, settings.CAMERA_PASSWORD),
                timeout=5,
            )
            return response.status_code == 200
    except Exception:
        return False

async def open_entry_barrier() -> bool:
    return await open_barrier(settings.BARRIER_ENTRY_IP)

async def open_exit_barrier() -> bool:
    return await open_barrier(settings.BARRIER_EXIT_IP)
