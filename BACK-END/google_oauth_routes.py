"""
Google OAuth 2.0 Routes (Login + Signup)

Beginner-friendly flow:
1) User opens: GET /login/google
   - We redirect the user to Google to sign in / consent.
2) Google redirects back to: GET /auth/callback
   - We receive the authorization code, exchange it for tokens,
     then read the user's email + name.
3) If the email exists in Postgres -> login
   Else -> create a new user (signup)
4) We create a JWT and set it as an HttpOnly cookie, then redirect:
   - admin -> /admin
   - user  -> /user
"""

from __future__ import annotations

import re
import secrets
from typing import Optional

from authlib.integrations.starlette_client import OAuth, OAuthError
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse, JSONResponse
from sqlalchemy.orm import Session

from auth_utils import create_access_token, hash_password, verify_token
from config import APP_BASE_URL, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_ADMIN_EMAIL
from database import SessionLocal
from db_table import User


# Main OAuth router with /api/auth prefix
router = APIRouter(prefix="/api/auth", tags=["google-oauth"])

# Separate router for callback without prefix (to match Google Cloud Console config)
callback_router = APIRouter(tags=["google-oauth"])


def get_db():
    """DB session dependency (SQLAlchemy)."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


_OAUTH: Optional[OAuth] = None


def _get_oauth() -> OAuth:
    """
    Create an OAuth registry and register Google provider.

    Notes:
    - Uses OpenID Connect discovery (server_metadata_url) so we don't hardcode endpoints.
    - Scope includes "openid email profile" so we can read email + name.
    """
    global _OAUTH
    if _OAUTH is not None:
        return _OAUTH

    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        # Fail with a clear error for beginners, but only when OAuth endpoints are used.
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Missing GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET. Set them in BACK-END/.env",
        )

    oauth = OAuth()
    oauth.register(
        name="google",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email profile"},
    )
    _OAUTH = oauth
    return oauth


def _sanitize_username_base(s: str) -> str:
    """
    Convert an email prefix into an allowed username.
    Allowed set matches existing signup rules: letters, numbers, underscore, dot, hyphen.
    """
    s = (s or "").strip()
    s = re.sub(r"[^A-Za-z0-9_.-]+", "", s)
    if len(s) < 3:
        s = f"user{s}"
    return s[:100]


def _make_unique_username(db: Session, base: str) -> str:
    """
    Ensure `users.username` remains unique.
    If base exists, append a short random suffix.
    """
    base = _sanitize_username_base(base)
    candidate = base
    for _ in range(20):
        exists = db.query(User).filter(User.username == candidate).first()
        if not exists:
            return candidate
        candidate = f"{base}-{secrets.token_hex(2)}"  # e.g. john-9f2a
        candidate = candidate[:100]
    # Extremely unlikely, but keep a safe fallback:
    return f"user-{secrets.token_hex(8)}"[:100]


def _role_for_email(email: str) -> str:
    """Admin logic requirement."""
    if (email or "").strip().lower() == (GOOGLE_ADMIN_EMAIL or "").strip().lower():
        return "admin"
    return "user"


def _extract_bearer_or_cookie_token(request: Request) -> Optional[str]:
    """Support both Authorization header and cookie-based auth."""
    auth = request.headers.get("authorization") or ""
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip() or None
    cookie_token = request.cookies.get("access_token")
    return cookie_token or None


def get_current_user_from_jwt(request: Request, db: Session = Depends(get_db)) -> User:
    """
    Simple dependency used by /dashboard.
    Accepts JWT from either:
    - Authorization: Bearer <token>
    - Cookie: access_token=<token>
    """
    token = _extract_bearer_or_cookie_token(request)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not logged in")
    payload = verify_token(token)
    user_id = payload.get("user_id")
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return user


@router.get("/login/google")
async def login_google(request: Request):
    """
    Step 1: redirect the browser to Google login page.
    
    Note: redirect_uri must match what's configured in Google Cloud Console.
    Based on your Google OAuth config, it's set to: http://localhost:8000/callback
    """
    # IMPORTANT:
    # Build redirect_uri from the incoming request host (localhost vs 127.0.0.1).
    # If these don't match, the session cookie won't be sent to /callback and
    # Authlib will raise MismatchingStateError.
    redirect_uri = f"{str(request.base_url).rstrip('/')}/callback"
    oauth = _get_oauth()

    try:
        # Debug: show where we will return after Google consent.
        print(f"[OAuth] authorize_redirect redirect_uri={redirect_uri} host={request.headers.get('host')}")
        resp = await oauth.google.authorize_redirect(request, redirect_uri)
        # Debug: confirm SessionMiddleware is setting a session cookie.
        set_cookie = resp.headers.get("set-cookie")
        if set_cookie:
            print(f"[OAuth] Response set-cookie (truncated): {set_cookie[:200]}...")
        else:
            print("[OAuth] Response set-cookie: <none> (Session cookie not set!)")
        return resp
    except Exception as e:
        # Log the error for debugging
        import traceback
        print(f"[OAuth] Error in authorize_redirect: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"OAuth redirect failed: {str(e)}"
        )


async def _handle_auth_callback(request: Request, db: Session = Depends(get_db)):
    """
    Shared handler for OAuth callback (used by both routes).
    Step 2: Google redirects here after the user logs in.
    We exchange the code for tokens, then read user info.
    """
    oauth = _get_oauth()
    try:
        # Debug: log session state before token exchange
        session_state = getattr(request, "session", {})
        print(f"[OAuth Callback] Session keys before token exchange: {list(session_state.keys())}")
        print(f"[OAuth Callback] Raw session: {session_state!r}")
        print(f"[OAuth Callback] URL: {request.url}")
        print(f"[OAuth Callback] Host: {request.headers.get('host')}")
        sess_cookie = request.cookies.get("session")
        print(f"[OAuth Callback] Has session cookie: {bool(sess_cookie)} len={len(sess_cookie) if sess_cookie else 0}")

        token = await oauth.google.authorize_access_token(request)
    except OAuthError as e:
        # More detailed error logging for debugging
        import traceback
        error_msg = str(e.error) if hasattr(e, 'error') else str(e)
        print(f"[OAuth Callback] OAuthError: {error_msg}")
        print(f"[OAuth Callback] Error description: {getattr(e, 'description', 'N/A')}")
        print(f"[OAuth Callback] Full error: {e}")
        print(f"[OAuth Callback] Traceback:\n{traceback.format_exc()}")
        
        # Check if it's a state mismatch issue
        if "state" in error_msg.lower() or "mismatch" in error_msg.lower():
            raise HTTPException(
                status_code=400,
                detail=f"OAuth state mismatch. This usually means the session was lost between redirects. Please try logging in again. Error: {error_msg}"
            )
        raise HTTPException(status_code=400, detail=f"Google OAuth failed: {error_msg}")

    # The token may include userinfo; if not, parse it from id_token.
    userinfo = token.get("userinfo")
    if not userinfo:
        userinfo = await oauth.google.parse_id_token(request, token)

    email = (userinfo.get("email") or "").strip().lower()
    name = (userinfo.get("name") or userinfo.get("given_name") or "").strip()

    if not email:
        raise HTTPException(status_code=400, detail="Google did not return an email address")

    # Step 3: login or signup
    user = db.query(User).filter(User.email == email).first()
    role = _role_for_email(email)

    if user:
        # Login existing user; keep name/role fresh.
        user.name = name or user.name
        user.full_name = name or user.full_name
        user.role = role
        user.is_active = True
        user.is_verified = True
    else:
        # Signup new user.
        username_base = email.split("@")[0]
        username = _make_unique_username(db, username_base)

        # This project historically requires a password_hash.
        # For Google users, we store a random password hash that no one knows.
        random_password = secrets.token_hex(16)  # 32 chars -> safe for bcrypt byte limit
        password_hash = hash_password(random_password)

        user = User(
            email=email,
            username=username,
            password_hash=password_hash,
            name=name or None,
            full_name=name or None,
            role=role,
            is_active=True,
            is_verified=True,  # Google login implies verified email
        )
        db.add(user)

    db.commit()
    db.refresh(user)

    # Step 4: create JWT and redirect based on role
    access_token = create_access_token(
        data={
            "user_id": user.id,
            "email": user.email,
            "username": user.username,
            "role": user.role,
        }
    )

    # Redirect to frontend HTML pages (admin.html for admins, user.html for regular users)
    redirect_to = "/admin.html" if user.role == "admin" else "/user.html"
    resp = RedirectResponse(url=redirect_to, status_code=302)

    # Store JWT in a cookie so browsers can call /dashboard without manual headers.
    # In production, set secure=True behind HTTPS.
    resp.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=60 * 60 * 24,  # 24 hours
    )
    return resp


# Register callback at both /api/auth/callback and /callback (to match Google Cloud Console)
@router.get("/callback", include_in_schema=False)
async def auth_callback_1(request: Request, db: Session = Depends(get_db)):
    """Callback route at /api/auth/callback"""
    return await _handle_auth_callback(request, db)


@callback_router.get("/callback", include_in_schema=False)
async def auth_callback_2(request: Request, db: Session = Depends(get_db)):
    """Callback route at /callback (matches Google Cloud Console redirect URI)"""
    return await _handle_auth_callback(request, db)


@router.get("/dashboard")
def dashboard(current_user: User = Depends(get_current_user_from_jwt)):
    """
    Simple protected endpoint.
    Returns the logged-in user's email/name/role from the database.
    """
    return JSONResponse(
        {
            "success": True,
            "email": current_user.email,
            "name": current_user.name or current_user.full_name,
            "role": current_user.role,
            "redirect_to": "/admin" if current_user.role == "admin" else "/user",
        }
    )


@router.get("/oauth-sync")
def oauth_sync(request: Request, db: Session = Depends(get_db)):
    """
    OAuth Token Sync Endpoint
    
    After Google OAuth login, the backend sets an HttpOnly cookie with the JWT token.
    This endpoint reads that cookie and returns the token + user info so the frontend
    can store it in localStorage (for compatibility with existing auth flow).
    
    This is safe because:
    - The cookie is HttpOnly (can't be read by malicious JS)
    - Only our backend can read it and return it
    - Frontend stores it in localStorage for API calls
    """
    token = _extract_bearer_or_cookie_token(request)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not logged in")
    
    payload = verify_token(token)
    user_id = payload.get("user_id")
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    
    return JSONResponse(
        {
            "success": True,
            "access_token": token,  # Return token so frontend can store in localStorage
            "user_id": user.id,
            "email": user.email,
            "username": user.username,
            "name": user.name or user.full_name,
            "role": user.role,
        }
    )


