from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = "postgresql://parking:parking123@localhost/parking_db"

    # JetQR API
    JETQR_BASE_URL: str = "https://dev-jetqr.aliftech.net/test"
    JETQR_API_KEY: str = "e9e6f8df-7f09-4a39-b249-652342001edd"
    JETQR_MERCHANT_ID: str = "test_prk_1"
    JETQR_STORE_ID: str = "test_prk_store"
    JETQR_TERMINAL_ID: str = "test_prk_terminal"
    JETQR_MIS_TERMINAL_ID: str = "MIS-T-001"
    JETQR_AMOUNT: float = 5.0

    # Dahua Cameras
    CAMERA_ENTRY_IP: str = "192.168.15.107"
    CAMERA_EXIT_IP: str = "192.168.15.108"
    CAMERA_USER: str = "admin"
    CAMERA_PASSWORD: str = "admin"

    # Barrier (controlled via camera controller)
    BARRIER_ENTRY_IP: str = "192.168.15.107"
    BARRIER_EXIT_IP: str = "192.168.15.108"

    class Config:
        env_file = ".env"

settings = Settings()
