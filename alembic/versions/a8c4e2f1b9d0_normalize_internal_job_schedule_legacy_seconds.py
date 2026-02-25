"""normalize internal job schedule legacy seconds

Revision ID: a8c4e2f1b9d0
Revises: f7a1c9d2e3b4
Create Date: 2026-02-25

"""

from __future__ import annotations

import json
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a8c4e2f1b9d0"
down_revision: Union[str, Sequence[str], None] = "f7a1c9d2e3b4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _seconds_to_cron(seconds: int) -> str | None:
    """Convert a legacy interval (seconds) into a cron expression.

    Conservative rules:
    - 900  => every 15 minutes
    - 1800 => every 30 minutes
    - 3600 => hourly at minute 0
    - 86400 => daily at 00:00
    - 604800 => weekly on Monday at 00:00

    Generic support:
    - multiples of 60 up to 3600 => every N minutes
    - multiples of 3600 up to 86400 => every N hours at minute 0

    Returns None when conversion is ambiguous/not supported.
    """

    if seconds <= 0:
        return None

    if seconds == 604800:
        return "0 0 * * 1"
    if seconds == 86400:
        return "0 0 * * *"
    if seconds == 3600:
        return "0 * * * *"

    if seconds % 60 == 0 and seconds < 3600:
        n = seconds // 60
        if 1 <= n <= 59:
            return f"*/{n} * * * *"

    if seconds % 3600 == 0 and seconds < 86400:
        n = seconds // 3600
        if 1 <= n <= 23:
            return f"0 */{n} * * *"

    return None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("internal_jobs"):
        return

    cols = {c["name"] for c in insp.get_columns("internal_jobs")}
    if "schedule" not in cols:
        return

    # schedule is JSONB. Legacy values can appear as JSON string (e.g. "86400") or JSON number.
    rows = bind.execute(sa.text("SELECT id, schedule FROM internal_jobs WHERE schedule IS NOT NULL")).fetchall()

    updated = 0
    for row in rows:
        job_id = row[0]
        schedule_val = row[1]

        seconds: int | None = None

        if isinstance(schedule_val, (int, float)):
            try:
                seconds = int(schedule_val)
            except Exception:
                seconds = None
        elif isinstance(schedule_val, str):
            v = schedule_val.strip()
            if v.isdigit():
                seconds = int(v)
        elif isinstance(schedule_val, dict):
            # Already in the new object format (or another structured format). Leave it as-is.
            seconds = None
        else:
            seconds = None

        if seconds is None:
            continue

        cron = _seconds_to_cron(seconds)
        if not cron:
            continue

        new_schedule = {"type": "cron", "cron": cron, "timezone": "UTC"}
        bind.execute(
            sa.text("UPDATE internal_jobs SET schedule = :schedule WHERE id = :id"),
            {"id": job_id, "schedule": json.dumps(new_schedule)},
        )
        updated += 1

    # No explicit print/logging: Alembic output stays clean.


def downgrade() -> None:
    # Irreversible: we cannot safely restore the original interval seconds.
    pass
