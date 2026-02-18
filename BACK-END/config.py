"""
Centralized configuration loaded from environment variables (.env).

This avoids hardcoding secrets (DB password, JWT secret, SMTP password) in code.
"""

from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from BACK-END directory (same folder as this file)
_ENV_PATH = Path(__file__).parent / ".env"
if _ENV_PATH.exists():
    load_dotenv(dotenv_path=_ENV_PATH)
else:
    # Also allow loading from project root .env (optional)
    load_dotenv()


def _get(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v


# -------- JWT / App --------
APP_BASE_URL = _get("APP_BASE_URL", "http://127.0.0.1:8000")
JWT_SECRET_KEY = _get("JWT_SECRET_KEY", "CHANGE_ME_IN_.env")
JWT_ALGORITHM = _get("JWT_ALGORITHM", "HS256")
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = int(_get("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))  # 24h


# -------- Database --------
DB_USER = _get("DB_USER", "postgres")
DB_PASS = _get("DB_PASS", "Jeeva@123")
DB_HOST = _get("DB_HOST", "localhost")
DB_NAME = _get("DB_NAME", "data_model_2026")
DB_PORT = _get("DB_PORT", "5432")


def build_database_url() -> str:
    # URL-encode '@' in password at minimum; keep simple for your current setup.
    safe_pass = (DB_PASS or "").replace("@", "%40")
    return f"postgresql+psycopg2://{DB_USER}:{safe_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# -------- SMTP Email --------
SMTP_HOST = _get("SMTP_HOST")
SMTP_PORT = int(_get("SMTP_PORT", "587"))
SMTP_USER = _get("SMTP_USER")
SMTP_PASS = _get("SMTP_PASS")
SMTP_FROM = _get("SMTP_FROM", SMTP_USER or "")
SMTP_TLS = (_get("SMTP_TLS", "true") or "true").lower() in ("1", "true", "yes", "y")


