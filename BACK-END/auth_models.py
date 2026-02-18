"""
Pydantic Models for Authentication
Request/Response models for auth endpoints
"""

from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime


class UserSignupRequest(BaseModel):
    """User signup request model"""
    email: EmailStr = Field(..., description="User email address")
    username: str = Field(..., min_length=3, max_length=100, description="Username (3-100 characters)")
    # Password must be exactly 4 digits (numbers only) - avoids bcrypt byte issues
    password: str = Field(..., min_length=4, max_length=4, description="Password (exactly 4 digits: 0-9 only)")
    full_name: Optional[str] = Field(None, max_length=200, description="Full name")


class UserLoginRequest(BaseModel):
    """User login request model"""
    email: EmailStr = Field(..., description="User email address")
    password: str = Field(..., description="User password")


class ResendVerificationRequest(BaseModel):
    """Request to resend email verification"""
    email: EmailStr = Field(..., description="User email address")


class TokenResponse(BaseModel):
    """Token response model"""
    access_token: str
    token_type: str = "bearer"
    user_id: int
    email: str
    username: str
    expires_in: int  # seconds


class UserResponse(BaseModel):
    """User response model"""
    id: int
    email: str
    username: str
    full_name: Optional[str]
    is_active: bool
    is_verified: bool
    created_at: datetime


class LoginLogResponse(BaseModel):
    """Login log response model"""
    id: int
    user_id: int
    email: str
    login_timestamp: datetime
    ip_address: Optional[str]
    login_status: str
    logout_timestamp: Optional[datetime]
    session_duration_minutes: Optional[int]


class MessageResponse(BaseModel):
    """Generic message response"""
    success: bool
    message: str

