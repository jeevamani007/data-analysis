"""
Admin / Reporting Routes

Provides an aggregated dashboard view for admin users:
- Login timeline for all users
- Plan + daily upload tokens usage
- Payment history
- Upload sessions with domain classification and file details
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Any, Dict, List

from database import SessionLocal
from db_table import User, LoginLog, UploadSession, UploadFile as UploadFileModel, Admin
from subscription_tables import (
    UserPlanSubscription,
    UserDailyUploadUsage,
    DummyPaymentTransaction,
)
from auth_routes import get_current_user


router = APIRouter(prefix="/api/admin", tags=["admin"])


def get_db():
    """Dependency to get database session for admin routes."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _serialize_dt(dt) -> str | None:
    """Safe datetime -> ISO string serialization."""
    if not dt:
        return None
    try:
        return dt.isoformat()
    except Exception:
        return str(dt)


@router.get("/dashboard")
def admin_dashboard(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Aggregated admin dashboard data.

    Access rules:
    - New world: any user with `users.role == 'admin'` (e.g. Google OAuth admin)
    - Legacy world: any user that has a matching row in `admins` table
    """
    # Allow if the authenticated user has role='admin' (set by google_oauth_routes.py)
    if getattr(current_user, "role", None) != "admin":
        # Fallback to legacy admin check (for old databases)
        legacy_admin = (
            db.query(Admin)
            .filter(
                (Admin.email == current_user.email)
                | (Admin.userid == current_user.username)
            )
            .first()
        )
        if not legacy_admin:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin access required",
            )
    # --- Build helper lookup: active plan per user ---
    plan_rows: List[UserPlanSubscription] = (
        db.query(UserPlanSubscription)
        .filter(UserPlanSubscription.is_active.is_(True))
        .all()
    )
    plans_by_user_id: Dict[int, UserPlanSubscription] = {
        r.user_id: r for r in plan_rows
    }

    # --- Login timeline (latest first) ---
    login_rows = (
        db.query(LoginLog, User)
        .join(User, LoginLog.user_id == User.id, isouter=True)
        .order_by(LoginLog.login_timestamp.desc())
        .limit(200)
        .all()
    )

    login_timeline: List[Dict[str, Any]] = []
    for log, user in login_rows:
        plan = plans_by_user_id.get(log.user_id or 0)
        login_timeline.append(
            {
                "user_id": getattr(user, "id", None),
                "username": getattr(user, "username", None),
                "email": getattr(user, "email", log.email),
                "plan_name": getattr(plan, "plan_name", None),
                "login_time": _serialize_dt(log.login_timestamp),
                "logout_time": _serialize_dt(log.logout_timestamp),
                "date": _serialize_dt(log.login_timestamp).split("T")[0]
                if log.login_timestamp
                else None,
                "status": log.login_status,
                "session_minutes": log.session_duration_minutes,
                "ip_address": log.ip_address,
                "user_agent": log.user_agent,
            }
        )

    # --- Daily upload tokens usage (latest dates first) ---
    usage_rows: List[UserDailyUploadUsage] = (
        db.query(UserDailyUploadUsage, User)
        .join(User, UserDailyUploadUsage.user_id == User.id, isouter=True)
        .order_by(UserDailyUploadUsage.usage_date.desc(), UserDailyUploadUsage.user_id.asc())
        .limit(200)
        .all()
    )

    token_usage: List[Dict[str, Any]] = []
    for usage, user in usage_rows:
        plan = plans_by_user_id.get(usage.user_id)
        remaining = int(usage.daily_limit) - int(usage.used_uploads or 0)
        if remaining < 0:
            remaining = 0
        token_usage.append(
            {
                "usage_date": str(usage.usage_date),
                "user_id": usage.user_id,
                "username": getattr(user, "username", None),
                "email": getattr(user, "email", None),
                "plan_name": getattr(plan, "plan_name", None),
                "daily_limit": int(usage.daily_limit),
                "used_uploads": int(usage.used_uploads or 0),
                "remaining_uploads": remaining,
            }
        )

    # --- Payment history (latest first) ---
    payment_rows: List[DummyPaymentTransaction] = (
        db.query(DummyPaymentTransaction, User)
        .join(User, DummyPaymentTransaction.user_id == User.id, isouter=True)
        .order_by(DummyPaymentTransaction.created_at.desc())
        .limit(200)
        .all()
    )

    payments: List[Dict[str, Any]] = []
    for pay, user in payment_rows:
        payments.append(
            {
                "id": pay.id,
                "user_id": pay.user_id,
                "username": getattr(user, "username", None),
                "email": getattr(user, "email", None),
                "plan_name": pay.plan_name,
                "amount": str(pay.amount),
                "currency": pay.currency,
                "status": pay.status,
                "reference": pay.reference,
                "created_at": _serialize_dt(pay.created_at),
            }
        )

    # --- Upload sessions + files + detected domain (latest first) ---
    # NOTE: In some legacy databases, the `upload_sessions` table does not have
    # a `user_id` column, but the SQLAlchemy model includes it. A normal
    # ORM query would therefore try to select `upload_sessions.user_id`
    # and crash with "column does not exist". To stay compatible with
    # such databases, we query only the columns that actually exist
    # using a manual SQL statement and map them into dictionaries.
    uploads_raw = db.execute(
        text(
            """
            SELECT
                id,
                session_id,
                upload_name,
                file_count,
                created_at,
                last_analyzed_at,
                domain_name,
                domain_confidence,
                domain_detected_at
            FROM upload_sessions
            ORDER BY created_at DESC
            LIMIT :limit
            """
        ),
        {"limit": 200},
    ).mappings().all()

    # Collect upload_session PKs so we can fetch files in one query
    upload_session_ids = [row["id"] for row in uploads_raw]
    files_by_session_id: Dict[int, List[UploadFileModel]] = {}
    if upload_session_ids:
        file_rows: List[UploadFileModel] = (
            db.query(UploadFileModel)
            .filter(UploadFileModel.session_id.in_(upload_session_ids))
            .order_by(UploadFileModel.id.asc())
            .all()
        )
        for f in file_rows:
            files_by_session_id.setdefault(f.session_id, []).append(f)

    uploads: List[Dict[str, Any]] = []
    for row in uploads_raw:
        sess_id = row["id"]
        sess_files = files_by_session_id.get(sess_id, [])  # type: ignore[arg-type]
        uploads.append(
            {
                "upload_session_id": sess_id,
                "session_id": row["session_id"],
                # `user_id` / username / email may not exist in older DB schema
                "user_id": None,
                "username": None,
                "email": None,
                "upload_name": row["upload_name"],
                "file_count": row["file_count"],
                "created_at": _serialize_dt(row["created_at"]),
                "last_analyzed_at": _serialize_dt(row["last_analyzed_at"]),
                "domain_name": row["domain_name"],
                "domain_confidence": row["domain_confidence"],
                "domain_detected_at": _serialize_dt(row["domain_detected_at"]),
                "files": [
                    {
                        "file_name": f.file_name,
                        "file_extension": f.file_extension,
                        "content_type": f.content_type,
                        "uploaded_at": _serialize_dt(f.created_at),
                    }
                    for f in sess_files
                ],
            }
        )

    # --- Legacy admins list (for visibility / debugging) ---
    admin_rows: List[Admin] = db.query(Admin).order_by(Admin.id.asc()).all()
    admins: List[Dict[str, Any]] = []
    for a in admin_rows:
        admins.append(
            {
                "id": a.id,
                "userid": a.userid,
                "email": a.email,
            }
        )

    return {
        "success": True,
        "login_timeline": login_timeline,
        "token_usage": token_usage,
        "payments": payments,
        "uploads": uploads,
        "admins": admins,
    }


@router.get("/ping")
def admin_ping(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Simple health-check endpoint for the admin area.
    """
    if not current_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
        )
    # Treat as admin if either:
    # - User has role='admin' (new model), OR
    # - User exists in legacy `admins` table
    is_role_admin = getattr(current_user, "role", None) == "admin"
    if not is_role_admin:
        legacy_admin = (
            db.query(Admin)
            .filter(
                (Admin.email == current_user.email)
                | (Admin.userid == current_user.username)
            )
            .first()
        )
        if not legacy_admin:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin access required",
            )
    return {
        "success": True,
        "message": "Admin API is alive",
        "user": current_user.username,
        "email": current_user.email,
    }


