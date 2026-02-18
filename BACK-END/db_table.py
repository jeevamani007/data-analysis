"""
Database Models
- Authentication system tables (users, login logs, tokens)
- Upload tracking tables for storing CSV upload metadata and domain classification
"""

from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, ForeignKey
from sqlalchemy.sql import func
from database import Base


class User(Base):
    """User table for storing encrypted user credentials"""
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    username = Column(String(100), unique=True, index=True, nullable=False)
    # Password will be stored as hashed (encrypted)
    password_hash = Column(String(255), nullable=False)
    full_name = Column(String(200), nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    is_verified = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<User(id={self.id}, email={self.email}, username={self.username})>"


class LoginLog(Base):
    """Login logs table for tracking user login timestamps and activities"""
    __tablename__ = "login_logs"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True)
    email = Column(String(255), nullable=False, index=True)
    login_timestamp = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    ip_address = Column(String(45), nullable=True)  # IPv6 support
    user_agent = Column(Text, nullable=True)
    login_status = Column(String(20), nullable=False, default="success")  # success, failed
    logout_timestamp = Column(DateTime(timezone=True), nullable=True)
    session_duration_minutes = Column(Integer, nullable=True)

    def __repr__(self):
        return f"<LoginLog(id={self.id}, user_id={self.user_id}, login_timestamp={self.login_timestamp})>"


class EmailVerificationToken(Base):
    """
    Stores one-time email verification tokens.
    A user can have multiple tokens (e.g. resend), only one needs to be used.
    """
    __tablename__ = "email_verification_tokens"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True)
    email = Column(String(255), nullable=False, index=True)
    token = Column(String(128), unique=True, index=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    is_used = Column(Boolean, default=False, nullable=False)
    used_at = Column(DateTime(timezone=True), nullable=True)

    def __repr__(self):
        return f"<EmailVerificationToken(id={self.id}, user_id={self.user_id}, is_used={self.is_used})>"


class EmailOTPToken(Base):
    """
    Stores short OTP codes for email verification (e.g. 6 digits).
    This is optional and can be used alongside the verification link token.
    """
    __tablename__ = "email_otp_tokens"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True)
    email = Column(String(255), nullable=False, index=True)
    code_hash = Column(String(64), nullable=False, index=True)  # sha256 hex
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    is_used = Column(Boolean, default=False, nullable=False)
    used_at = Column(DateTime(timezone=True), nullable=True)

    def __repr__(self):
        return f"<EmailOTPToken(id={self.id}, user_id={self.user_id}, is_used={self.is_used})>"


class PasswordResetOTPToken(Base):
    """
    Stores OTP codes for password reset (forgot password).

    Notes:
    - We store only a sha256 hash of the OTP (never store OTP plaintext).
    - We record created/verified/used timestamps for auditing.
    """
    __tablename__ = "password_reset_otp_tokens"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True)
    email = Column(String(255), nullable=False, index=True)

    code_hash = Column(String(64), nullable=False, index=True)  # sha256 hex

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)

    verified_at = Column(DateTime(timezone=True), nullable=True)
    is_used = Column(Boolean, default=False, nullable=False)
    used_at = Column(DateTime(timezone=True), nullable=True)

    attempt_count = Column(Integer, default=0, nullable=False)
    request_ip = Column(String(45), nullable=True)  # IPv6 support
    user_agent = Column(Text, nullable=True)

    def __repr__(self):
        return f"<PasswordResetOTPToken(id={self.id}, user_id={self.user_id}, is_used={self.is_used})>"


class UploadSession(Base):
    """
    Stores one record per CSV upload session.

    Fields capture:
    - A human-friendly upload name (typed on the frontend)
    - Generated session_id used by the analysis pipeline
    - File count and high-level info about the upload
    - Detected domain and timestamps when the analysis ran
    """
    __tablename__ = "upload_sessions"

    id = Column(Integer, primary_key=True, index=True)

    # UUID string used by existing /api/upload + /api/analyze endpoints
    session_id = Column(String(64), unique=True, index=True, nullable=False)

    # Who uploaded (authenticated user)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)

    # Name / label entered by the user on the upload page
    upload_name = Column(String(255), nullable=False)

    # Number of files received in this upload
    file_count = Column(Integer, nullable=False, default=0)

    # When the upload was created (stored in DB time zone)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # When analysis was last run for this session
    last_analyzed_at = Column(DateTime(timezone=True), nullable=True)

    # Domain classification result (e.g. Banking, Healthcare, HR)
    domain_name = Column(String(100), nullable=True)
    domain_confidence = Column(Integer, nullable=True)  # store rounded percentage (0‑100)
    domain_detected_at = Column(DateTime(timezone=True), nullable=True)

    def __repr__(self):
        return f"<UploadSession(id={self.id}, session_id={self.session_id}, upload_name={self.upload_name})>"


class UploadFile(Base):
    """
    Stores one record per uploaded file in a session.

    We keep:
    - Original file name
    - File extension / type (e.g. csv)
    - Content type reported by the client (e.g. text/csv)
    - Timestamp when the record was created
    """
    __tablename__ = "upload_files"

    id = Column(Integer, primary_key=True, index=True)

    # Link back to UploadSession.id
    session_id = Column(Integer, ForeignKey("upload_sessions.id"), nullable=False, index=True)

    file_name = Column(String(512), nullable=False)
    file_extension = Column(String(50), nullable=True)
    content_type = Column(String(255), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    def __repr__(self):
        return f"<UploadFile(id={self.id}, session_id={self.session_id}, file_name={self.file_name})>"


# Create all tables
def create_tables():
    """Create all database tables"""
    from database import engine
    # Ensure subscription models are registered in SQLAlchemy metadata
    try:
        import subscription_tables  # noqa: F401
    except Exception as e:
        # Do not prevent app startup; tables can still be created on first successful import
        print(f"[DB] Warning: failed to import subscription_tables: {e}")
    Base.metadata.create_all(bind=engine)

