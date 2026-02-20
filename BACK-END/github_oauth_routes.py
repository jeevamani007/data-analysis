"""
GitHub OAuth 2.0 Routes (Login + Signup)

Flow (mirrors google_oauth_routes.py):
1) User opens: GET /api/auth/login/github
   - We redirect the user to GitHub to sign in / consent.
2) GitHub redirects back to: GET /api/auth/github/callback
   - We receive the authorization code, exchange it for tokens,
     then read the user's email + name from the GitHub API.
3) If the email exists in Postgres -> login
   Else -> create a new user (signup)
4) We create a JWT and set it as an HttpOnly cookie, then redirect:
   - admin -> /admin.html
   - user  -> /user.html (which syncs JWT cookie to localStorage)

This is intentionally similar to google_oauth_routes.py so beginners
can compare both providers side by side.
"""

from __future__ import annotations

import secrets
from typing import Optional

from authlib.integrations.starlette_client import OAuth, OAuthError
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse, JSONResponse
from sqlalchemy.orm import Session

from auth_utils import create_access_token, hash_password, verify_token
from config import (
    APP_BASE_URL,
    GITHUB_CLIENT_ID,
    GITHUB_CLIENT_SECRET,
    GITHUB_REDIRECT_URI,
    GOOGLE_ADMIN_EMAIL,
)
from database import SessionLocal
from db_table import User, LoginLog


router = APIRouter(prefix="/api/auth", tags=["github-oauth"])


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
    Create an OAuth registry and register GitHub provider.

    Notes:
    - GitHub uses standard OAuth 2.0 (not OpenID Connect).
    - We request 'user:email' scope to reliably get a primary email.
    """
    global _OAUTH
    if _OAUTH is not None:
        return _OAUTH

    if not GITHUB_CLIENT_ID or not GITHUB_CLIENT_SECRET:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Missing GITHUB_CLIENT_ID / GITHUB_CLIENT_SECRET. Set them in BACK-END/.env",
        )

    oauth = OAuth()
    oauth.register(
        name="github",
        client_id=GITHUB_CLIENT_ID,
        client_secret=GITHUB_CLIENT_SECRET,
        access_token_url="https://github.com/login/oauth/access_token",
        authorize_url="https://github.com/login/oauth/authorize",
        api_base_url="https://api.github.com/",
        client_kwargs={"scope": "read:user user:email"},
    )
    _OAUTH = oauth
    return oauth


def _role_for_email(email: str) -> str:
    """
    Simple admin rule: reuse GOOGLE_ADMIN_EMAIL so the same person
    is treated as admin whether they log in with Google or GitHub.
    """
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
    Simple dependency used by /github/dashboard (optional).
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


def _get_client_ip(request: Request) -> str:
    """Extract client IP address for LoginLog."""
    if request.client:
        return request.client.host
    return "unknown"


@router.get("/login/github")
async def login_github(request: Request):
    """
    Step 1: redirect the browser to GitHub login page.

    Supports two flows:
    - flow=login   (default)  -> existing user login
    - flow=signup           -> signup only; if account exists, show \"already exists\" message
    """
    flow = request.query_params.get("flow") or "login"
    try:
        request.session["oauth_flow"] = flow
    except Exception as e:
        # If SessionMiddleware is missing or misconfigured, log but don't crash
        print(f"[GitHub OAuth] Failed to store oauth_flow in session: {e}")

    oauth = _get_oauth()

    # Prefer explicit GITHUB_REDIRECT_URI if configured, otherwise build from request.
    redirect_uri = (
        GITHUB_REDIRECT_URI
        or f"{str(request.base_url).rstrip('/')}/github/callback"
    )

    try:
        print(f"[GitHub OAuth] authorize_redirect redirect_uri={redirect_uri}")
        resp = await oauth.github.authorize_redirect(request, redirect_uri)
        return resp
    except Exception as e:
        import traceback

        print(f"[GitHub OAuth] Error in authorize_redirect: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"GitHub OAuth redirect failed: {str(e)}",
        )


async def _fetch_github_userinfo(oauth: OAuth, token, request: Request) -> dict:
    """
    Fetch basic user profile + primary email from GitHub.
    - /user returns login, name, and sometimes email.
    - If email is missing, we call /user/emails to find a primary, verified email.
    """
    # 1) Basic profile
    resp = await oauth.github.get("user", token=token)
    profile = resp.json()

    email = (profile.get("email") or "").strip().lower()
    name = (profile.get("name") or profile.get("login") or "").strip()

    # 2) If email missing, try /user/emails
    if not email:
        try:
            emails_resp = await oauth.github.get("user/emails", token=token)
            emails = emails_resp.json() or []
            # Prefer primary & verified; else first verified; else first.
            primary = next(
                (e for e in emails if e.get("primary") and e.get("verified")),
                None,
            )
            if not primary:
                primary = next((e for e in emails if e.get("verified")), None)
            if not primary and emails:
                primary = emails[0]
            if primary and primary.get("email"):
                email = primary["email"].strip().lower()
        except Exception as e:
            print(f"[GitHub OAuth] Failed to fetch emails: {e}")

    return {
        "email": email,
        "name": name,
        "login": profile.get("login"),
    }


async def _handle_github_callback(request: Request, db: Session = Depends(get_db)):
    """
    Shared handler for GitHub OAuth callback.
    Step 2: GitHub redirects here after the user logs in.
    We exchange the code for tokens, then read user info.
    """
    oauth = _get_oauth()
    try:
        token = await oauth.github.authorize_access_token(request)
    except OAuthError as e:
        import traceback

        error_msg = getattr(e, "error", None) or str(e)
        print(f"[GitHub OAuth Callback] OAuthError: {error_msg}")
        print(f"[GitHub OAuth Callback] Traceback:\n{traceback.format_exc()}")
        raise HTTPException(status_code=400, detail=f"GitHub OAuth failed: {error_msg}")
    except Exception as e:
        import traceback

        print(f"[GitHub OAuth Callback] Error in authorize_access_token: {e}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=400,
            detail=f"GitHub OAuth failed: {str(e)}",
        )

    # Determine original flow (login vs signup) from session
    try:
        flow = request.session.pop("oauth_flow", "login")
    except Exception:
        flow = "login"
    print(f"[GitHub OAuth Callback] Flow detected: {flow}")

    userinfo = await _fetch_github_userinfo(oauth, token, request)
    email = (userinfo.get("email") or "").strip().lower()
    name = (userinfo.get("name") or userinfo.get("login") or "").strip()

    if not email:
        raise HTTPException(
            status_code=400,
            detail="GitHub did not return an email address. Please make your email public or verify it on GitHub.",
        )

    user = db.query(User).filter(User.email == email).first()
    role = _role_for_email(email)

    # Log helper
    def _log_login(status: str, user_id: int = 0):
        try:
            ip_address = _get_client_ip(request)
            user_agent = request.headers.get("user-agent", "unknown")
            log = LoginLog(
                user_id=user_id,
                email=email,
                ip_address=ip_address,
                user_agent=user_agent,
                login_status=status,
            )
            db.add(log)
            db.commit()
        except Exception as log_err:
            db.rollback()
            print(f"[GitHub OAuth] Failed to write LoginLog: {log_err}")

    if user:
        if flow == "signup":
            # User clicked \"Sign up with GitHub\" but account already exists.
            redirect_to = "/signin.html?error=account_exists"
            print(f"[GitHub OAuth Callback] Existing user during signup flow, redirecting to {redirect_to}")
            _log_login("failed", user_id=user.id)
            return RedirectResponse(url=redirect_to, status_code=302)

        # Login existing user; keep name/role fresh.
        user.name = name or user.name
        user.full_name = name or user.full_name
        user.role = role
        user.is_active = True
        user.is_verified = True
    else:
        # Signup new user.
        username_base = (userinfo.get("login") or email.split("@")[0] or "github-user").strip()

        # Ensure unique username similar to google_oauth_routes._make_unique_username
        base = username_base[:100]
        candidate = base
        for _ in range(20):
            exists = db.query(User).filter(User.username == candidate).first()
            if not exists:
                break
            candidate = f"{base}-{secrets.token_hex(2)}"[:100]
        username = candidate

        random_password = secrets.token_hex(16)
        password_hash = hash_password(random_password)

        user = User(
            email=email,
            username=username,
            password_hash=password_hash,
            name=name or None,
            full_name=name or None,
            role=role,
            is_active=True,
            is_verified=True,  # GitHub login implies verified email
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

    _log_login("success", user_id=user.id)

    redirect_to = "/admin.html" if user.role == "admin" else "/user.html"
    resp = RedirectResponse(url=redirect_to, status_code=302)

    resp.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=60 * 60 * 24,  # 24 hours
    )
    return resp


@router.get("/github/callback", include_in_schema=False)
async def github_callback(request: Request, db: Session = Depends(get_db)):
    """Callback route at /api/auth/github/callback."""
    return await _handle_github_callback(request, db)


@router.get("/github/dashboard")
def github_dashboard(current_user: User = Depends(get_current_user_from_jwt)):
    """
    Simple protected endpoint (mainly for debugging GitHub JWT flow).
    """
    return JSONResponse(
        {
            "success": True,
            "email": current_user.email,
            "name": current_user.name or current_user.full_name,
            "role": current_user.role,
            "provider": "github",
        }
    )



