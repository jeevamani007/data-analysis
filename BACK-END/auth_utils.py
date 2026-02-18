"""
Authentication Utilities
Password hashing, JWT token generation, and verification
"""

from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import HTTPException, status
import secrets

from config import JWT_SECRET_KEY, JWT_ALGORITHM, JWT_ACCESS_TOKEN_EXPIRE_MINUTES

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# bcrypt only uses the first 72 bytes of the password (and many libs raise if longer).
# Enforce this explicitly to avoid confusing 500 errors.
BCRYPT_MAX_PASSWORD_BYTES = 72

# JWT Configuration
SECRET_KEY = JWT_SECRET_KEY
ALGORITHM = JWT_ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = JWT_ACCESS_TOKEN_EXPIRE_MINUTES


def _ensure_bcrypt_password_length_ok(password: str) -> None:
    """
    bcrypt has a hard limit of 72 *bytes* (not characters).
    passlib/bcrypt may raise at runtime if this is exceeded, which otherwise
    surfaces as a 500. Convert that into a clean 400.
    """
    try:
        byte_len = len((password or "").encode("utf-8"))
    except Exception:
        # Defensive: if encoding fails for some reason, reject as bad input.
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid password encoding",
        )

    if byte_len > BCRYPT_MAX_PASSWORD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Password cannot be longer than {BCRYPT_MAX_PASSWORD_BYTES} bytes",
        )


def hash_password(password: str) -> str:
    """Hash a password using bcrypt"""
    _ensure_bcrypt_password_length_ok(password)
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    _ensure_bcrypt_password_length_ok(plain_password)
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create a JWT access token"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def decode_access_token(token: str) -> Optional[dict]:
    """Decode and verify a JWT token"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None


def get_current_user_id(token: str) -> Optional[int]:
    """Extract user ID from token"""
    payload = decode_access_token(token)
    if payload is None:
        return None
    return payload.get("user_id")


def verify_token(token: str) -> dict:
    """Verify token and return user data"""
    payload = decode_access_token(token)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload


def generate_email_verification_token() -> str:
    """
    Generate a URL-safe token for email verification.
    Stored in DB (hashed token could also be used; token is random enough).
    """
    return secrets.token_urlsafe(32)

