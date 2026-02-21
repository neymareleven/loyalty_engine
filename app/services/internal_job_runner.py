from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.internal_job import InternalJob
from app.models.transaction import Transaction
from app.schemas.event import EventCreate
from app.services.transaction_service import create_transaction


@dataclass
class InternalJobRunStats:
    processed: int
    created: int
    idempotent_existing: int
    failed: int


def parse_schedule_seconds(schedule: str | None) -> int | None:
    if schedule is None:
        return None
    s = str(schedule).strip()
    if not s:
        return None
    return int(s)


def compute_next_run_at(*, base: datetime, interval_seconds: int) -> datetime:
    return base + timedelta(seconds=int(interval_seconds))


def compute_run_bucket_key(*, now: datetime, interval_seconds: int | None) -> str:
    if not interval_seconds:
        return now.date().isoformat()
    return str(int(now.timestamp()) // int(interval_seconds))


def run_internal_job_once(
    db: Session,
    *,
    job: InternalJob,
    now: datetime | None = None,
) -> InternalJobRunStats:
    if now is None:
        now = datetime.utcnow()

    today: date = now.date()

    q = db.query(Customer)
    if job.brand:
        q = q.filter(Customer.brand == job.brand)

    from app.routes.internal_jobs import _apply_selector

    q = _apply_selector(q, job.selector or {}, today)
    customers = q.all()

    interval_seconds = parse_schedule_seconds(job.schedule)
    bucket_key = compute_run_bucket_key(now=now, interval_seconds=interval_seconds)

    processed = 0
    created = 0
    idempotent_existing = 0
    failed = 0

    for c in customers:
        processed += 1
        event_id = f"job_{job.id}_{bucket_key}_{c.brand}_{c.profile_id}"

        already_exists = (
            db.query(Transaction.id)
            .filter(Transaction.event_id == event_id)
            .first()
        )
        if already_exists:
            idempotent_existing += 1

        payload = job.payload_template or {}
        event = EventCreate(
            brand=c.brand,
            profileId=c.profile_id,
            eventType=job.event_type,
            eventId=event_id,
            source="INTERNAL_JOB",
            payload=payload,
        )

        try:
            create_transaction(db, event)
            if not already_exists:
                created += 1
        except Exception:
            failed += 1

    return InternalJobRunStats(
        processed=processed,
        created=created,
        idempotent_existing=idempotent_existing,
        failed=failed,
    )
