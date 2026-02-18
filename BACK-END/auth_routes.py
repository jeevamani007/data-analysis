"""
Authentication Routes
Signup, Login, Logout, and Verification endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, status, Request, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from datetime import datetime, timedelta, timezone
from typing import Optional

from database import SessionLocal
from db_table import User, LoginLog, EmailVerificationToken
from auth_models import (
    UserSignupRequest, UserLoginRequest, TokenResponse, 
    UserResponse, MessageResponse, LoginLogResponse, ResendVerificationRequest, VerifyOtpRequest
)
from auth_utils import (
    hash_password, verify_password, create_access_token, 
    verify_token, get_current_user_id, generate_email_verification_token
)
from config import APP_BASE_URL
from email_service import send_verification_email
import hashlib
import secrets

router = APIRouter(prefix="/api/auth", tags=["authentication"])
security = HTTPBearer()

def _utcnow() -> datetime:
    """Timezone-aware UTC 'now' to safely compare with tz-aware DB datetimes."""
    return datetime.now(timezone.utc)

def _as_utc_aware(dt: datetime) -> datetime:
    """Normalize DB datetime to UTC-aware (handles both naive and aware)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def get_db():
    """Dependency to get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> User:
    """Get current authenticated user"""
    token = credentials.credentials
    payload = verify_token(token)
    user_id = payload.get("user_id")
    
    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is inactive"
        )
    return user


def get_client_ip(request: Request) -> str:
    """Extract client IP address"""
    if request.client:
        return request.client.host
    return "unknown"


@router.post("/signup", response_model=MessageResponse, status_code=status.HTTP_201_CREATED)
async def signup(
    user_data: UserSignupRequest,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Create a new user account
    
    - **email**: User email (must be unique)
    - **username**: Username (must be unique, 3-100 chars)
    - **password**: Password (exactly 4 digits, 0-9 only)
    - **full_name**: Optional full name
    """
    try:
        # Check if email already exists
        existing_user = db.query(User).filter(User.email == user_data.email).first()
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered"
            )
        
        # Validate username format (letters, numbers, underscore, dot, hyphen)
        # Keep this in sync with frontend `signin.html`
        import re
        if not re.fullmatch(r"[A-Za-z0-9_.-]{3,100}", (user_data.username or "")):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username must be 3-100 characters and contain only letters, numbers, underscore (_), dot (.), and hyphen (-)."
            )

        # Check if username already exists
        existing_username = db.query(User).filter(User.username == user_data.username).first()
        if existing_username:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username already taken"
            )
        
        # Validate password: must be exactly 4 digits (numbers only)
        if not user_data.password.isdigit():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Password must contain only numbers (digits 0-9)"
            )
        
        if len(user_data.password) != 4:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Password must be exactly 4 digits"
            )
        
        # Hash password (4 digits = 4 bytes, well within bcrypt limit)
        hashed_password = hash_password(user_data.password)
        new_user = User(
            email=user_data.email,
            username=user_data.username,
            password_hash=hashed_password,
            full_name=user_data.full_name,
            is_active=True,
            is_verified=False
        )
        
        db.add(new_user)
        db.commit()
        db.refresh(new_user)

        # Create verification token
        token = generate_email_verification_token()
        expires_at = _utcnow() + timedelta(hours=24)
        token_row = EmailVerificationToken(
            user_id=new_user.id,
            email=new_user.email,
            token=token,
            expires_at=expires_at,
            is_used=False,
        )
        db.add(token_row)
        db.commit()

        # Create OTP (6 digits) in parallel with verification link
        otp_code = f"{secrets.randbelow(1_000_000):06d}"
        otp_expires_at = _utcnow() + timedelta(minutes=15)
        # Hash OTP only (do not couple to link token), so OTP can be verified independently.
        otp_hash = hashlib.sha256(otp_code.encode("utf-8")).hexdigest()
        try:
            from db_table import EmailOTPToken
            otp_row = EmailOTPToken(
                user_id=new_user.id,
                email=new_user.email,
                code_hash=otp_hash,
                expires_at=otp_expires_at,
                is_used=False,
            )
            db.add(otp_row)
            db.commit()
        except Exception as otp_err:
            # OTP is optional; don't fail signup if OTP storage fails.
            print(f"Warning: Failed to create OTP: {otp_err}")
            otp_code = None

        # Send verification email in background (non-blocking)
        # If email fails, user is still created (email sending is non-critical)
        verify_url = f"{APP_BASE_URL.rstrip('/')}/api/auth/verify-email?token={token}"
        try:
            background_tasks.add_task(send_verification_email, to_email=new_user.email, verify_url=verify_url, otp_code=otp_code)
            email_status = "sent"
        except Exception as email_error:
            # Log email error but don't fail signup
            print(f"Warning: Failed to queue verification email: {email_error}")
            email_status = "failed"

        return MessageResponse(
            success=True,
            message="Account created. Please verify your email before logging in. If you didn’t receive the email, check spam or use “Resend verification email”."
        )
    except HTTPException:
        # Re-raise HTTP exceptions (like 400 Bad Request)
        raise
    except Exception as e:
        # Catch any unexpected errors and return 500 with error details
        import traceback
        error_trace = traceback.format_exc()
        print(f"Signup error: {e}\n{error_trace}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Signup failed: {str(e)}"
        )


@router.post("/login", response_model=TokenResponse)
async def login(
    login_data: UserLoginRequest,
    request: Request,
    db: Session = Depends(get_db)
):
    """
    Login with email and password
    
    - **email**: User email
    - **password**: User password
    """
    # Find user by email
    user = db.query(User).filter(User.email == login_data.email).first()
    
    if not user:
        # Log failed login attempt
        ip_address = get_client_ip(request)
        user_agent = request.headers.get("user-agent", "unknown")
        failed_log = LoginLog(
            user_id=0,  # No user ID for failed login
            email=login_data.email,
            ip_address=ip_address,
            user_agent=user_agent,
            login_status="failed"
        )
        db.add(failed_log)
        db.commit()
        
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )
    
    # Verify password
    # Note: verify_password may raise HTTPException(400) if password exceeds bcrypt byte limit.
    if not verify_password(login_data.password, user.password_hash):
        # Log failed login attempt
        ip_address = get_client_ip(request)
        user_agent = request.headers.get("user-agent", "unknown")
        failed_log = LoginLog(
            user_id=user.id,
            email=user.email,
            ip_address=ip_address,
            user_agent=user_agent,
            login_status="failed"
        )
        db.add(failed_log)
        db.commit()
        
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )
    
    # Check if user is active
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is inactive"
        )

    # Email verification check (mandatory)
    if not user.is_verified:
        ip_address = get_client_ip(request)
        user_agent = request.headers.get("user-agent", "unknown")
        failed_log = LoginLog(
            user_id=user.id,
            email=user.email,
            ip_address=ip_address,
            user_agent=user_agent,
            login_status="failed"
        )
        db.add(failed_log)
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Email not verified. Please verify your email and try again."
        )
    
    # Create access token
    access_token = create_access_token(
        data={"user_id": user.id, "email": user.email, "username": user.username}
    )
    
    # Log successful login
    ip_address = get_client_ip(request)
    user_agent = request.headers.get("user-agent", "unknown")
    
    login_log = LoginLog(
        user_id=user.id,
        email=user.email,
        ip_address=ip_address,
        user_agent=user_agent,
        login_status="success"
    )
    db.add(login_log)
    db.commit()
    
    return TokenResponse(
        access_token=access_token,
        user_id=user.id,
        email=user.email,
        username=user.username,
        expires_in=60 * 24 * 60  # 24 hours in seconds
    )


@router.post("/resend-verification", response_model=MessageResponse)
async def resend_verification(
    req: ResendVerificationRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Resend email verification link (safe response to prevent email enumeration).
    """
    user = db.query(User).filter(User.email == req.email).first()
    if user and user.is_active and not user.is_verified:
        token = generate_email_verification_token()
        expires_at = _utcnow() + timedelta(hours=24)
        token_row = EmailVerificationToken(
            user_id=user.id,
            email=user.email,
            token=token,
            expires_at=expires_at,
            is_used=False,
        )
        db.add(token_row)
        db.commit()
        verify_url = f"{APP_BASE_URL.rstrip('/')}/api/auth/verify-email?token={token}"

        # Also create a fresh OTP
        otp_code = f"{secrets.randbelow(1_000_000):06d}"
        otp_expires_at = _utcnow() + timedelta(minutes=15)
        otp_hash = hashlib.sha256(otp_code.encode("utf-8")).hexdigest()
        try:
            from db_table import EmailOTPToken
            otp_row = EmailOTPToken(
                user_id=user.id,
                email=user.email,
                code_hash=otp_hash,
                expires_at=otp_expires_at,
                is_used=False,
            )
            db.add(otp_row)
            db.commit()
        except Exception as otp_err:
            print(f"Warning: Failed to create OTP: {otp_err}")
            otp_code = None

        background_tasks.add_task(send_verification_email, to_email=user.email, verify_url=verify_url, otp_code=otp_code)

    return MessageResponse(
        success=True,
        message="If the account exists and is not verified, a verification email has been sent."
    )


@router.post("/verify-otp", response_model=MessageResponse)
async def verify_otp(req: VerifyOtpRequest, db: Session = Depends(get_db)):
    """
    Verify an email using a short OTP code (6 digits).
    """
    user = db.query(User).filter(User.email == req.email).first()
    if not user:
        # Avoid account enumeration
        return MessageResponse(success=True, message="If the account exists, it has been verified.")
    if user.is_verified:
        return MessageResponse(success=True, message="Email is already verified. You can login.")

    # Find latest unused OTP for this user/email
    try:
        from db_table import EmailOTPToken
        row = db.query(EmailOTPToken).filter(
            EmailOTPToken.user_id == user.id,
            EmailOTPToken.email == user.email,
            EmailOTPToken.is_used.is_(False),
        ).order_by(EmailOTPToken.created_at.desc()).first()
    except Exception as e:
        print(f"OTP lookup error: {e}")
        raise HTTPException(status_code=500, detail="OTP verification unavailable")

    if not row:
        raise HTTPException(status_code=400, detail="OTP not found. Please resend verification email.")
    if row.expires_at and _as_utc_aware(row.expires_at) < _utcnow():
        raise HTTPException(status_code=400, detail="OTP expired. Please resend verification email.")

    otp_hash = hashlib.sha256(req.otp.encode("utf-8")).hexdigest()
    if otp_hash != row.code_hash:
        raise HTTPException(status_code=400, detail="Invalid OTP. Please try again.")

    # Mark verified
    user.is_verified = True
    row.is_used = True
    row.used_at = _utcnow()
    # Also mark the latest verification-link token (if any) as used to avoid token buildup
    latest_token = db.query(EmailVerificationToken).filter(
        EmailVerificationToken.user_id == user.id,
        EmailVerificationToken.email == user.email,
        EmailVerificationToken.is_used.is_(False),
    ).order_by(EmailVerificationToken.created_at.desc()).first()
    if latest_token:
        latest_token.is_used = True
        latest_token.used_at = _utcnow()
    db.commit()
    return MessageResponse(success=True, message="Email verified successfully. You can login now.")


@router.get("/verify-email", response_class=HTMLResponse)
async def verify_email(token: str, db: Session = Depends(get_db)):
    """
    Verify an email using a one-time token. Returns a simple HTML page.
    """
    row = db.query(EmailVerificationToken).filter(EmailVerificationToken.token == token).first()
    if not row:
        return HTMLResponse(
            content="<h3>Invalid verification link.</h3><p>Please request a new verification email.</p>",
            status_code=400,
        )
    if row.is_used:
        return HTMLResponse(
            content="<h3>This verification link was already used.</h3><p>You can login now.</p>",
            status_code=200,
        )
    if row.expires_at and _as_utc_aware(row.expires_at) < _utcnow():
        return HTMLResponse(
            content="<h3>This verification link has expired.</h3><p>Please request a new verification email.</p>",
            status_code=400,
        )

    user = db.query(User).filter(User.id == row.user_id).first()
    if not user:
        return HTMLResponse(content="<h3>User not found.</h3>", status_code=400)

    user.is_verified = True
    row.is_used = True
    row.used_at = _utcnow()
    db.commit()

    login_url = f"{APP_BASE_URL.rstrip('/')}/login.html"
    html = f"""
    <div style="font-family: Inter, Arial, sans-serif; background:#F1F5F9; padding:24px;">
      <div style="max-width:560px; margin:0 auto; background:#FFFFFF; border:1px solid #E2E8F0; border-radius:16px; padding:24px;">
        <h2 style="margin:0 0 8px; color:#0F172A;">Email verified successfully</h2>
        <p style="margin:0 0 16px; color:#334155;">Your account is now verified. You can log in.</p>
        <a href="{login_url}" style="display:inline-block; background:linear-gradient(135deg,#0F766E,#0D9488); color:#fff; text-decoration:none; padding:12px 16px; border-radius:12px; font-weight:700;">
          Go to Login
        </a>
      </div>
    </div>
    """.strip()
    return HTMLResponse(content=html, status_code=200)


@router.post("/logout", response_model=MessageResponse)
async def logout(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    """
    Logout user and update login log with logout timestamp
    """
    token = credentials.credentials
    user_id = get_current_user_id(token)
    
    if user_id:
        # Find the most recent login log for this user
        login_log = db.query(LoginLog).filter(
            LoginLog.user_id == user_id,
            LoginLog.logout_timestamp.is_(None),
            LoginLog.login_status == "success"
        ).order_by(LoginLog.login_timestamp.desc()).first()
        
        if login_log:
            # Update logout timestamp
            logout_time = _utcnow()
            login_log.logout_timestamp = logout_time
            
            # Calculate session duration
            if login_log.login_timestamp:
                duration = (logout_time - login_log.login_timestamp).total_seconds() / 60
                login_log.session_duration_minutes = int(duration)
            
            db.commit()
    
    return MessageResponse(
        success=True,
        message="Logged out successfully"
    )


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    current_user: User = Depends(get_current_user)
):
    """
    Get current authenticated user information
    """
    return UserResponse(
        id=current_user.id,
        email=current_user.email,
        username=current_user.username,
        full_name=current_user.full_name,
        is_active=current_user.is_active,
        is_verified=current_user.is_verified,
        created_at=current_user.created_at
    )


@router.get("/login-logs", response_model=list[LoginLogResponse])
async def get_login_logs(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    limit: int = 50
):
    """
    Get login logs for current user
    """
    logs = db.query(LoginLog).filter(
        LoginLog.user_id == current_user.id
    ).order_by(LoginLog.login_timestamp.desc()).limit(limit).all()
    
    return [
        LoginLogResponse(
            id=log.id,
            user_id=log.user_id,
            email=log.email,
            login_timestamp=log.login_timestamp,
            ip_address=log.ip_address,
            login_status=log.login_status,
            logout_timestamp=log.logout_timestamp,
            session_duration_minutes=log.session_duration_minutes
        )
        for log in logs
    ]


@router.get("/verify", response_model=MessageResponse)
async def verify_token_endpoint(
    current_user: User = Depends(get_current_user)
):
    """
    Verify if token is valid
    """
    return MessageResponse(
        success=True,
        message=f"Token is valid for user: {current_user.username}"
    )

