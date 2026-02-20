"""
Centralized configuration loaded from environment variables (.env).

This avoids hardcoding secrets (DB password, JWT secret, SMTP password) in code.
"""

from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment in a dev-friendly order:
# - Load BACK-END/sample.env first (defaults, override=False)
# - Then load BACK-END/.env (local overrides, override=True)
# - Finally, allow project-root .env (override=True)
_BACKEND_DIR = Path(__file__).parent
_SAMPLE_ENV_PATH = _BACKEND_DIR / "sample.env"
_ENV_PATH = _BACKEND_DIR / ".env"

if _SAMPLE_ENV_PATH.exists():
    load_dotenv(dotenv_path=_SAMPLE_ENV_PATH, override=False)

if _ENV_PATH.exists():
    # override=True so local .env wins over sample.env and any existing process env
    load_dotenv(dotenv_path=_ENV_PATH, override=True)
else:
    # Also allow loading from project root .env (optional)
    load_dotenv(override=True)


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

# Convenience + safer defaults:
# Many providers (including Gmail) authenticate with the account email, which often
# matches SMTP_FROM. If SMTP_USER is omitted but SMTP_FROM exists, fall back to it.
if (not SMTP_USER) and SMTP_FROM:
    SMTP_USER = SMTP_FROM

# If a password is provided but user is missing, email will fail anyway; warn early.
if SMTP_PASS and not SMTP_USER:
    print("[Config] Warning: SMTP_PASS is set but SMTP_USER is missing. SMTP login will fail.")

# -------- Google OAuth 2.0 --------
# These MUST be set in environment variables / BACK-END/.env
# Create them in Google Cloud Console -> APIs & Services -> Credentials (OAuth 2.0 Client ID).
GOOGLE_CLIENT_ID = _get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = _get("GOOGLE_CLIENT_SECRET")

# Optional: the email that should be treated as admin after Google login
GOOGLE_ADMIN_EMAIL = _get("GOOGLE_ADMIN_EMAIL", "ponjeevabsccs@gmail.com")


# -------- GitHub OAuth 2.0 --------
# These are used when enabling "Login with GitHub" alongside Google OAuth.
# Create them in: https://github.com/settings/developers -> OAuth Apps.
GITHUB_CLIENT_ID = _get("GITHUB_CLIENT_ID")
GITHUB_CLIENT_SECRET = _get("GITHUB_CLIENT_SECRET")
# Optional explicit redirect URI; if not set, backend will construct from request.base_url.
GITHUB_REDIRECT_URI = _get("GITHUB_REDIRECT_URI")


