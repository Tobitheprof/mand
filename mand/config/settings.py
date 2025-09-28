import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    PG_DSN: str = os.getenv(
        "PG_DSN",
        "postgresql+postgresql://postgres:3EcNO32CT9dB@db.nuodavvfvkvenxbjeyzn.supabase.co:5432/postgres"
    )

    SCHEDULER_TIMEZONE: str = os.getenv("SCHEDULER_TIMEZONE", "Europe/Amsterdam")


    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_JSON: bool = os.getenv("LOG_JSON", "true").lower() == "true"
    LOG_TO_FILE: bool = os.getenv("LOG_TO_FILE", "true").lower() == "true"
    LOG_FILE: str = os.getenv("LOG_FILE", "logs/mand.log")

    LOG_ROTATE: str = os.getenv("LOG_ROTATE", "size").lower()

    LOG_MAX_BYTES: int = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))  # 10 MB
    LOG_BACKUP_COUNT: int = int(os.getenv("LOG_BACKUP_COUNT", "5"))

    LOG_WHEN: str = os.getenv("LOG_WHEN", "midnight")
    LOG_INTERVAL: int = int(os.getenv("LOG_INTERVAL", "1"))


    LLM_CATEGORY_ENABLED = os.getenv("MAND_LLM_CATEGORY_ENABLED", "false").lower() in {"1","true","yes","on"}
    LLM_CATEGORY_MODEL = os.getenv("MAND_LLM_CATEGORY_MODEL", "qwen/qwen-2.5-7b-instruct")
    OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
    OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
    LLM_CATEGORY_TIMEOUT_S = int(os.getenv("MAND_LLM_CATEGORY_TIMEOUT_S", "30"))
    LLM_CATEGORY_MAX_RETRIES = int(os.getenv("MAND_LLM_CATEGORY_MAX_RETRIES", "2"))
    LLM_CATEGORY_CACHE_SIZE = int(os.getenv("MAND_LLM_CATEGORY_CACHE_SIZE", "500"))
    

    AH_WORKERS: int = int(os.getenv("AH_WORKERS", "16"))
    AH_FETCH_DETAILS: bool = os.getenv("AH_FETCH_DETAILS", "true").lower() == "true"
    AH_MAX_PAGES_PER_CATEGORY: str = os.getenv("AH_MAX_PAGES_PER_CATEGORY", "")

    
    @property
    def ah_max_pages(self):
        return int(self.AH_MAX_PAGES_PER_CATEGORY) if self.AH_MAX_PAGES_PER_CATEGORY.isdigit() else None

    PROMETHEUS_PORT: int = int(os.getenv("PROMETHEUS_PORT", "9101"))
    ENABLE_PROMETHEUS: bool = os.getenv("ENABLE_PROMETHEUS", "false").lower() == "true"

settings = Settings()
