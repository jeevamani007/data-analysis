"""
Midnight reset job.

Runs a lightweight background loop that wakes up at 12:00 AM (server local time)
and ensures today's daily usage rows exist (used_uploads starts at 0).

Even if this job doesn't run (server down), the app also does a "lazy reset"
by creating today's usage row on-demand in plan_service.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

from database import SessionLocal
from plan_service import ensure_today_usage_rows_for_all_active_users


def _seconds_until_next_midnight_local() -> float:
    now = datetime.now()
    next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(1.0, (next_midnight - now).total_seconds())


async def midnight_reset_loop() -> None:
    while True:
        try:
            await asyncio.sleep(_seconds_until_next_midnight_local())
            db = SessionLocal()
            try:
                created = ensure_today_usage_rows_for_all_active_users(db)
                print(f"[Plans] Midnight reset ran. New usage rows created: {created}")
            finally:
                db.close()
        except Exception as e:
            # Never crash the server because of the scheduler; retry in 60 seconds.
            print(f"[Plans] Midnight reset loop error: {e}")
            await asyncio.sleep(60)


