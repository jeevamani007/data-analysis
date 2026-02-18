"""
Plan / subscription API routes.

Used by `range-users.html` to:
- Show available plans
- Select a plan (dummy payment)
- Show remaining daily uploads (tokens)
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from datetime import date

from database import SessionLocal
from db_table import User
from auth_routes import get_current_user
from plan_service import (
    PLAN_DEFINITIONS,
    get_remaining_tokens,
    select_plan_with_dummy_payment,
)
from subscription_tables import DummyPaymentTransaction


router = APIRouter(prefix="/api/plans", tags=["plans"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("")
def list_plans():
    out = []
    for name, d in PLAN_DEFINITIONS.items():
        out.append(
            {
                "plan_name": name,
                "label": d.get("label"),
                "daily_limit": int(d.get("daily_limit", 0)),
                "price": str(d.get("price")),
                "currency": d.get("currency", "USD"),
            }
        )
    return {"success": True, "plans": out}


@router.get("/me")
def my_plan(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    sub, usage, remaining = get_remaining_tokens(db, current_user.id)

    # Build explanation for today's plan changes based on dummy payments
    today = date.today()
    today_rows = (
        db.query(DummyPaymentTransaction)
        .filter(DummyPaymentTransaction.user_id == current_user.id)
        .filter(DummyPaymentTransaction.created_at >= today)
        .order_by(DummyPaymentTransaction.created_at.asc())
        .all()
    )

    history = []
    for r in today_rows:
        history.append(
            {
                "plan_name": r.plan_name,
                "amount": str(r.amount),
                "currency": r.currency,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
        )

    explanation = ""
    if history:
        parts = []
        for h in history:
            ts = h["created_at"]
            ts_str = ts
            try:
                # Show local friendly time if possible
                from datetime import datetime as _dt
                dt = _dt.fromisoformat(ts.replace("Z", "+00:00")) if ts else None
                if dt:
                    ts_str = dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                pass
            parts.append(f"{ts_str}: switched to {h['plan_name']} ({h['amount']} {h['currency']})")
        explanation = (
            "Today you changed your plan:\n- "
            + "\n- ".join(parts)
            + f"\nCurrent day limit is {int(sub.daily_upload_limit)} uploads. "
            f"You have used {int(usage.used_uploads)} and have {int(remaining)} remaining."
        )

    return {
        "success": True,
        "user_id": current_user.id,
        "plan_name": sub.plan_name,
        "daily_limit": int(sub.daily_upload_limit),
        "usage_date": str(usage.usage_date),
        "used_uploads": int(usage.used_uploads),
        "remaining_uploads": int(remaining),
        "today_payments": history,
        "today_explanation": explanation,
    }


@router.post("/select")
def select_plan(payload: dict, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    plan_name = (payload or {}).get("plan_name")
    sub = select_plan_with_dummy_payment(db, current_user.id, plan_name)
    sub2, usage, remaining = get_remaining_tokens(db, current_user.id)
    return {
        "success": True,
        "message": f"Plan updated to {sub.plan_name}",
        "plan_name": sub2.plan_name,
        "daily_limit": int(sub2.daily_upload_limit),
        "usage_date": str(usage.usage_date),
        "used_uploads": int(usage.used_uploads),
        "remaining_uploads": int(remaining),
    }


@router.get("/payments")
def list_my_dummy_payments(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    rows = (
        db.query(DummyPaymentTransaction)
        .filter(DummyPaymentTransaction.user_id == current_user.id)
        .order_by(DummyPaymentTransaction.id.desc())
        .limit(20)
        .all()
    )
    return {
        "success": True,
        "payments": [
            {
                "id": r.id,
                "plan_name": r.plan_name,
                "amount": str(r.amount),
                "currency": r.currency,
                "status": r.status,
                "reference": r.reference,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ],
    }


