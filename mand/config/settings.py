import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    PG_DSN: str = os.getenv(
        "PG_DSN",
        "postgresql+psycopg2://postgres:Bl%403e345@localhost:5432/mand"
    )

    SCHEDULER_TIMEZONE: str = os.getenv("SCHEDULER_TIMEZONE", "Europe/Amsterdam")


    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_JSON: bool = os.getenv("LOG_JSON", "true").lower() == "true"
    LOG_TO_FILE: bool = os.getenv("LOG_TO_FILE", "true").lower() == "true"
    LOG_FILE: str = os.getenv("LOG_FILE", "logs/mand.log")

    # Rotation mode: "size" (RotatingFileHandler) or "time" (TimedRotatingFileHandler)
    LOG_ROTATE: str = os.getenv("LOG_ROTATE", "size").lower()

    # Size-based rotation
    LOG_MAX_BYTES: int = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))  # 10 MB
    LOG_BACKUP_COUNT: int = int(os.getenv("LOG_BACKUP_COUNT", "5"))

    # Time-based rotation
    LOG_WHEN: str = os.getenv("LOG_WHEN", "midnight")   # e.g., 'S','M','H','D','midnight','W0'-'W6'
    LOG_INTERVAL: int = int(os.getenv("LOG_INTERVAL", "1"))  # rotate every N units
    

    AH_WORKERS: int = int(os.getenv("AH_WORKERS", "16"))
    AH_FETCH_DETAILS: bool = os.getenv("AH_FETCH_DETAILS", "true").lower() == "true"
    AH_MAX_PAGES_PER_CATEGORY: str = os.getenv("AH_MAX_PAGES_PER_CATEGORY", "")

    
    @property
    def ah_max_pages(self):
        return int(self.AH_MAX_PAGES_PER_CATEGORY) if self.AH_MAX_PAGES_PER_CATEGORY.isdigit() else None

    PROMETHEUS_PORT: int = int(os.getenv("PROMETHEUS_PORT", "9101"))
    ENABLE_PROMETHEUS: bool = os.getenv("ENABLE_PROMETHEUS", "false").lower() == "true"

settings = Settings()
