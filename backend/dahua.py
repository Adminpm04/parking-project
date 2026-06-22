import ssl
import socket
import json
import hashlib
import time
import logging
from config import settings

logger = logging.getLogger(__name__)

def _md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest().upper()

def _raw_post(ssock, ip: str, path: str, body_dict: dict) -> dict:
    body = json.dumps(body_dict)
    req = (
        f"POST {path} HTTP/1.1\r\n"
        f"Host: {ip}\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: keep-alive\r\n\r\n"
        f"{body}"
    )
    ssock.sendall(req.encode())
    ssock.settimeout(5)

    # Read until we have the full HTTP response headers
    raw = b""
    while b"\r\n\r\n" not in raw:
        chunk = ssock.recv(4096)
        if not chunk:
            break
        raw += chunk

    if b"\r\n\r\n" not in raw:
        return {}

    headers_raw, _, rbody = raw.partition(b"\r\n\r\n")

    # Parse Content-Length to avoid waiting for connection close
    content_length = None
    for line in headers_raw.split(b"\r\n"):
        if line.lower().startswith(b"content-length:"):
            try:
                content_length = int(line.split(b":", 1)[1].strip())
            except Exception:
                pass

    is_chunked = b"chunked" in headers_raw.lower()

    if content_length is not None:
        # Read exactly content_length bytes
        while len(rbody) < content_length:
            need = content_length - len(rbody)
            try:
                chunk = ssock.recv(min(need, 4096))
                if not chunk:
                    break
                rbody += chunk
            except Exception:
                break
    elif is_chunked:
        # Read remaining chunks
        try:
            while b"0\r\n\r\n" not in rbody:
                chunk = ssock.recv(4096)
                if not chunk:
                    break
                rbody += chunk
        except Exception:
            pass

    if is_chunked:
        result = b""
        buf = rbody
        while buf:
            nl = buf.find(b"\r\n")
            if nl < 0:
                break
            chunk_size = int(buf[:nl], 16)
            if chunk_size == 0:
                break
            result += buf[nl + 2 : nl + 2 + chunk_size]
            buf = buf[nl + 2 + chunk_size + 2 :]
        rbody = result

    try:
        return json.loads(rbody)
    except Exception:
        return {}


def _open_barrier_rpc2(camera_ip: str) -> bool:
    """Open Dahua ITC barrier via RPC2 JSON-RPC over HTTPS."""
    user = settings.CAMERA_USER
    password = settings.CAMERA_PASSWORD

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.set_ciphers("ALL:@SECLEVEL=0")

    try:
        with socket.create_connection((camera_ip, 443), timeout=8) as sock:
            with ctx.wrap_socket(sock) as ssock:
                # Step 1: login challenge
                r1 = _raw_post(ssock, camera_ip, "/RPC2_Login", {
                    "method": "global.login",
                    "id": 1,
                    "session": 0,
                    "params": {"userName": user, "password": "", "clientType": "Web3.0"},
                })
                if not r1 or "params" not in r1:
                    logger.error(f"Barrier {camera_ip}: login challenge failed: {r1}")
                    return False

                realm = r1["params"]["realm"]
                random_str = r1["params"]["random"]
                session_id = r1["session"]

                hash1 = _md5(f"{user}:{realm}:{password}")
                hash2 = _md5(f"{user}:{random_str}:{hash1}")

                # Step 2: authenticate
                r2 = _raw_post(ssock, camera_ip, "/RPC2_Login", {
                    "method": "global.login",
                    "id": 2,
                    "session": session_id,
                    "params": {
                        "userName": user,
                        "password": hash2,
                        "clientType": "Web3.0",
                        "authorizeType": "MD5",
                    },
                })
                if not r2 or not r2.get("result"):
                    logger.error(f"Barrier {camera_ip}: login failed: {r2}")
                    return False

                session_id = r2["session"]

                # Step 3: open strobe (barrier)
                r3 = _raw_post(ssock, camera_ip, "/RPC2", {
                    "method": "trafficSnap.openStrobe",
                    "id": 3,
                    "session": session_id,
                    "object": 0,
                    "params": {"info": {"location": 0}},
                })
                success = r3.get("result") is True
                logger.info(f"Barrier {camera_ip} openStrobe: {r3}")
                return success

    except Exception as e:
        logger.error(f"Barrier {camera_ip} error: {e}")
        return False


async def open_barrier(camera_ip: str) -> bool:
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _open_barrier_rpc2, camera_ip)

async def open_entry_barrier() -> bool:
    return await open_barrier(settings.BARRIER_ENTRY_IP)

async def open_exit_barrier() -> bool:
    return await open_barrier(settings.BARRIER_EXIT_IP)
