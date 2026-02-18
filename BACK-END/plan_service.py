"""
Plan / subscription service helpers.

Rules:
- Default plan for a new user is FREE_TRIAL with 6 uploads/day.
- Each successful /api/upload increments used_uploads by 1.
- Limits reset daily at 12 AM (server local time), and also lazily on-demand.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from subscription_tables import UserPlanSubscription, UserDailyUploadUsage, DummyPaymentTransaction


PLAN_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "FREE_TRIAL": {"daily_limit": 6, "price": Decimal("0.00"), "currency": "USD", "label": "Free Trial"},
    "PLATINUM": {"daily_limit": 20, "price": Decimal("9.99"), "currency": "USD", "label": "Platinum"},
    "PREMIUM": {"daily_limit": 50, "price": Decimal("19.99"), "currency": "USD", "label": "Premium"},
}


def get_plan_definition(plan_name: str) -> Dict[str, Any]:
    plan_name = (plan_name or "").strip().upper()
    if plan_name not in PLAN_DEFINITIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown plan: {plan_name}. Allowed: {', '.join(PLAN_DEFINITIONS.keys())}",
        )
    return PLAN_DEFINITIONS[plan_name]


def _today_local() -> date:
    # Server-local date for "daily" behavior.
    return date.today()


def get_or_create_subscription(db: Session, user_id: int) -> UserPlanSubscription:
    sub = db.query(UserPlanSubscription).filter(UserPlanSubscription.user_id == user_id).first()
    if sub:
        return sub

    d = get_plan_definition("FREE_TRIAL")
    sub = UserPlanSubscription(
        user_id=user_id,
        plan_name="FREE_TRIAL",
        daily_upload_limit=int(d["daily_limit"]),
        is_active=True,
    )
    db.add(sub)
    db.commit()
    db.refresh(sub)
    return sub


def get_or_create_usage_for_date(db: Session, user_id: int, usage_day: date) -> UserDailyUploadUsage:
    sub = get_or_create_subscription(db, user_id)

    usage = (
        db.query(UserDailyUploadUsage)
        .filter(UserDailyUploadUsage.user_id == user_id, UserDailyUploadUsage.usage_date == usage_day)
        .first()
    )
    if usage:
        # Keep daily_limit in-sync with subscription (in case user changed plan)
        if usage.daily_limit != sub.daily_upload_limit:
            usage.daily_limit = sub.daily_upload_limit
            db.commit()
        return usage

    usage = UserDailyUploadUsage(
        user_id=user_id,
        usage_date=usage_day,
        daily_limit=sub.daily_upload_limit,
        used_uploads=0,
    )
    db.add(usage)
    db.commit()
    db.refresh(usage)
    return usage


def get_or_create_usage_today(db: Session, user_id: int) -> UserDailyUploadUsage:
    return get_or_create_usage_for_date(db, user_id, _today_local())


def get_remaining_tokens(db: Session, user_id: int) -> Tuple[UserPlanSubscription, UserDailyUploadUsage, int]:
    sub = get_or_create_subscription(db, user_id)
    usage = get_or_create_usage_today(db, user_id)
    remaining = max(0, int(usage.daily_limit) - int(usage.used_uploads))
    return sub, usage, remaining


def assert_can_upload(db: Session, user_id: int) -> None:
    _, usage, remaining = get_remaining_tokens(db, user_id)
    if remaining <= 0:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Daily upload limit reached ({usage.used_uploads}/{usage.daily_limit}). Please try again after 12:00 AM.",
        )


def increment_upload_usage(db: Session, user_id: int, count: int = 1) -> UserDailyUploadUsage:
    if count <= 0:
        return get_or_create_usage_today(db, user_id)

    assert_can_upload(db, user_id)
    usage = get_or_create_usage_today(db, user_id)

    # Re-check with count
    if usage.used_uploads + count > usage.daily_limit:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Daily upload limit exceeded ({usage.used_uploads}/{usage.daily_limit}). Please try again after 12:00 AM.",
        )

    usage.used_uploads = int(usage.used_uploads) + int(count)
    db.commit()
    db.refresh(usage)
    return usage


def select_plan_with_dummy_payment(db: Session, user_id: int, plan_name: str) -> UserPlanSubscription:
    plan_name = (plan_name or "").strip().upper()
    plan_def = get_plan_definition(plan_name)

    # Create dummy payment record (always success)
    payment = DummyPaymentTransaction(
        user_id=user_id,
        plan_name=plan_name,
        amount=plan_def["price"],
        currency=plan_def["currency"],
        status="success",
        reference=f"DUMMY-{user_id}-{int(datetime.utcnow().timestamp())}",
    )
    db.add(payment)

    # Upsert subscription
    sub = db.query(UserPlanSubscription).filter(UserPlanSubscription.user_id == user_id).first()
    if not sub:
        sub = UserPlanSubscription(
            user_id=user_id,
            plan_name=plan_name,
            daily_upload_limit=int(plan_def["daily_limit"]),
            is_active=True,
        )
        db.add(sub)
    else:
        sub.plan_name = plan_name
        sub.daily_upload_limit = int(plan_def["daily_limit"])
        sub.is_active = True

    db.commit()
    db.refresh(sub)

    # Ensure today's usage row exists with updated limit
    get_or_create_usage_today(db, user_id)
    return sub


def ensure_today_usage_rows_for_all_active_users(db: Session) -> int:
    """
    Midnight job: ensure today's row exists for each subscribed user.
    Returns number of rows created.
    """
    today = _today_local()
    created = 0

    subs = db.query(UserPlanSubscription).filter(UserPlanSubscription.is_active == True).all()  # noqa: E712
    for sub in subs:
        usage = (
            db.query(UserDailyUploadUsage)
            .filter(UserDailyUploadUsage.user_id == sub.user_id, UserDailyUploadUsage.usage_date == today)
            .first()
        )
        if usage:
            if usage.daily_limit != sub.daily_upload_limit:
                usage.daily_limit = sub.daily_upload_limit
            continue

        db.add(
            UserDailyUploadUsage(
                user_id=sub.user_id,
                usage_date=today,
                daily_limit=sub.daily_upload_limit,
                used_uploads=0,
            )
        )
        created += 1

    db.commit()
    return created


