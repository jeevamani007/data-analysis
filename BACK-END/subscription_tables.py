"""
Subscription / Plan tables

Implements:
- Per-user plan subscription (FREE_TRIAL / PLATINUM / PREMIUM)
- Per-user daily usage counter (used uploads per day)
- Dummy payment transaction history

These models are imported during `create_tables()` so they are registered in
SQLAlchemy metadata even though they live in a separate file.
"""

from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Date, Numeric, UniqueConstraint
from sqlalchemy.sql import func

from database import Base


class UserPlanSubscription(Base):
    """
    Stores the current plan for each user (Free Trial / Platinum / Premium).

    - One active row per user (enforced by unique user_id).
    - daily_upload_limit is stored so limits are stable even if definitions change later.
    """

    __tablename__ = "user_plan_subscriptions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), unique=True, nullable=False, index=True)

    plan_name = Column(String(50), nullable=False)  # FREE_TRIAL, PLATINUM, PREMIUM
    daily_upload_limit = Column(Integer, nullable=False)

    is_active = Column(Boolean, default=True, nullable=False)
    activated_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<UserPlanSubscription(user_id={self.user_id}, plan_name={self.plan_name}, limit={self.daily_upload_limit})>"


class UserDailyUploadUsage(Base):
    """
    Daily uploads usage per user.

    - One row per user per day (unique user_id + usage_date).
    - used_uploads increments once per successful /api/upload call.
    """

    __tablename__ = "user_daily_upload_usage"
    __table_args__ = (
        UniqueConstraint("user_id", "usage_date", name="uq_user_daily_usage"),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)

    usage_date = Column(Date, nullable=False, index=True)

    daily_limit = Column(Integer, nullable=False)
    used_uploads = Column(Integer, nullable=False, default=0)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<UserDailyUploadUsage(user_id={self.user_id}, date={self.usage_date}, used={self.used_uploads}/{self.daily_limit})>"


class DummyPaymentTransaction(Base):
    """
    Stores dummy payment records for plan selection.
    """

    __tablename__ = "dummy_payment_transactions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)

    plan_name = Column(String(50), nullable=False)
    amount = Column(Numeric(10, 2), nullable=False, default=0)
    currency = Column(String(10), nullable=False, default="USD")

    status = Column(String(30), nullable=False, default="success")  # success/failed
    reference = Column(String(100), nullable=True)  # fake reference id

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    def __repr__(self):
        return f"<DummyPaymentTransaction(user_id={self.user_id}, plan_name={self.plan_name}, amount={self.amount}, status={self.status})>"


