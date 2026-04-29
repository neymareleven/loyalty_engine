from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo
from uuid import UUID

from croniter import croniter

from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.internal_job import InternalJob
from app.models.segment_member import SegmentMember
from app.models.transaction import Transaction
from app.schemas.event import EventCreate
from app.services.transaction_service import create_transaction
from app.services.loyalty_status_service import update_customer_status


@dataclass
class InternalJobRunStats:
    processed: int
    created: int
    idempotent_existing: int
    failed: int


@dataclass
class MaintenanceJobRunStats:
    expired: int


@dataclass
class CustomerRecomputeRunStats:
    processed: int
    updated: int
    finished: bool


@dataclass
class CustomerMetricsRecomputeRunStats:
    processed: int
    touched: int
    finished: bool


@dataclass
class CouponBackfillRunStats:
    processed: int
    created: int
    idempotent_existing: int
    failed: int
    finished: bool


def _as_utc_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(ZoneInfo("UTC"))


def _to_utc_naive(dt: datetime) -> datetime:
    return _as_utc_aware(dt).replace(tzinfo=None)


def compute_next_run_at_from_schedule(*, base_utc: datetime, schedule: dict | None) -> datetime | None:
    if not schedule or not isinstance(schedule, dict):
        return None
    if schedule.get("type") != "cron":
        raise ValueError("Unsupported schedule.type (expected 'cron')")

    cron_expr = schedule.get("cron")
    if not cron_expr:
        raise ValueError("schedule.cron is required")

    tz_name = schedule.get("timezone") or "UTC"
    tz = ZoneInfo(tz_name)

    base_local = _as_utc_aware(base_utc).astimezone(tz)
    it = croniter(cron_expr, base_local)
    next_local: datetime = it.get_next(datetime)
    return _to_utc_naive(next_local)


def compute_run_bucket_key_from_schedule(*, now_utc: datetime, schedule: dict | None) -> str:
    if not schedule or not isinstance(schedule, dict):
        return now_utc.date().isoformat()
    if schedule.get("type") != "cron":
        return now_utc.date().isoformat()

    cron_expr = schedule.get("cron")
    if not cron_expr:
        return now_utc.date().isoformat()
    tz_name = schedule.get("timezone") or "UTC"
    tz = ZoneInfo(tz_name)

    now_local = _as_utc_aware(now_utc).astimezone(tz)
    it = croniter(cron_expr, now_local)
    prev_local: datetime = it.get_prev(datetime)

    # Bucket is identified by the scheduled "window" start instant in UTC.
    prev_utc_naive = _to_utc_naive(prev_local)
    return prev_utc_naive.isoformat()


def run_internal_job_once(
    db: Session,
    *,
    job: InternalJob,
    now: datetime | None = None,
) -> object:
    if now is None:
        now = datetime.utcnow()

    if job.job_key == "MAINT_EXPIRE_REWARDS":
        if not job.brand:
            raise ValueError("MAINT_EXPIRE_REWARDS requires job.brand")

        from app.services.reward_service import expire_rewards

        expired = expire_rewards(db, brand=job.brand)
        return MaintenanceJobRunStats(expired=int(expired))

    if job.job_key == "MAINT_EXPIRE_COUPONS":
        if not job.brand:
            raise ValueError("MAINT_EXPIRE_COUPONS requires job.brand")

        from app.services.coupon_service import expire_coupons

        expired = expire_coupons(db, brand=job.brand)
        return MaintenanceJobRunStats(expired=int(expired))

    if job.job_key == "MAINT_EXPIRE_POINTS":
        if not job.brand:
            raise ValueError("MAINT_EXPIRE_POINTS requires job.brand")

        from app.services.loyalty_validity_service import expire_points

        expired = expire_points(db, brand=job.brand)
        return MaintenanceJobRunStats(expired=int(expired))

    if job.job_key == "MAINT_EXPIRE_LOYALTY_STATUS":
        if not job.brand:
            raise ValueError("MAINT_EXPIRE_LOYALTY_STATUS requires job.brand")

        from app.services.loyalty_validity_service import expire_loyalty_status

        expired = expire_loyalty_status(db, brand=job.brand)
        return MaintenanceJobRunStats(expired=int(expired))

    if job.job_key == "MAINT_RECOMPUTE_CUSTOMERS_LOYALTY_STATUS":
        if not job.brand:
            raise ValueError("MAINT_RECOMPUTE_CUSTOMERS_LOYALTY_STATUS requires job.brand")

        selector = job.selector or {}
        after_id_raw = selector.get("after_id")
        after_id: UUID | None = None
        if after_id_raw:
            try:
                after_id = UUID(str(after_id_raw))
            except Exception:
                after_id = None
        try:
            batch_size = int(selector.get("batch_size") or 500)
        except Exception:
            batch_size = 500
        batch_size = max(1, min(batch_size, 5000))

        q = (
            db.query(Customer)
            .filter(Customer.brand == job.brand)
            .order_by(Customer.id.asc())
        )
        if after_id:
            q = q.filter(Customer.id > after_id)

        customers = q.limit(batch_size).all()

        processed = 0
        updated = 0
        last_id = None
        for c in customers:
            processed += 1
            before = c.loyalty_status
            update_customer_status(
                db,
                c,
                reason="AUTO_TIER_REFRESH",
                source_transaction_id=None,
                depth=0,
                refresh_window=True,
                emit_events=False,
            )
            if c.loyalty_status != before:
                updated += 1
            last_id = c.id

        finished = len(customers) < batch_size
        if finished:
            job.selector = {"batch_size": batch_size}
        else:
            job.selector = {"after_id": str(last_id), "batch_size": batch_size}

        db.flush()
        return CustomerRecomputeRunStats(processed=processed, updated=updated, finished=bool(finished))

    if job.job_key == "MAINT_RECOMPUTE_CUSTOMER_METRICS":
        if not job.brand:
            raise ValueError("MAINT_RECOMPUTE_CUSTOMER_METRICS requires job.brand")

        selector = job.selector or {}
        after_id_raw = selector.get("after_id")
        after_id: UUID | None = None
        if after_id_raw:
            try:
                after_id = UUID(str(after_id_raw))
            except Exception:
                after_id = None
        try:
            batch_size = int(selector.get("batch_size") or 500)
        except Exception:
            batch_size = 500
        batch_size = max(1, min(batch_size, 5000))

        q = db.query(Customer.id).filter(Customer.brand == job.brand).order_by(Customer.id.asc())
        if after_id:
            q = q.filter(Customer.id > after_id)

        ids = [row.id for row in q.limit(batch_size).all()]

        processed = len(ids)
        from app.services.customer_metrics_service import recompute_customer_metrics_for_brand

        touched = 0
        if ids:
            touched = int(recompute_customer_metrics_for_brand(db, brand=job.brand, customer_ids=ids, now_utc=now))

        finished = len(ids) < batch_size
        if finished:
            job.selector = {"batch_size": batch_size}
        else:
            job.selector = {"after_id": str(ids[-1]), "batch_size": batch_size}

        db.flush()
        return CustomerMetricsRecomputeRunStats(processed=int(processed), touched=int(touched), finished=bool(finished))

    if job.job_key == "MAINT_RECOMPUTE_SEGMENTS":
        if not job.brand:
            raise ValueError("MAINT_RECOMPUTE_SEGMENTS requires job.brand")

        from app.services.segment_service import recompute_dynamic_segments_for_brand

        selector = job.selector or {}
        try:
            batch_size = int(selector.get("batch_size") or 500)
        except Exception:
            batch_size = 500
        batch_size = max(1, min(batch_size, 5000))

        stats = recompute_dynamic_segments_for_brand(db, brand=job.brand, now_utc=now, batch_size=batch_size)
        db.flush()
        return stats

    if job.job_key == "MAINT_BACKFILL_COUPONS":
        if not job.brand:
            raise ValueError("MAINT_BACKFILL_COUPONS requires job.brand")

        selector = job.selector or {}
        after_id_raw = selector.get("after_id")
        after_id: UUID | None = None
        if after_id_raw:
            try:
                after_id = UUID(str(after_id_raw))
            except Exception:
                after_id = None
        try:
            batch_size = int(selector.get("batch_size") or 500)
        except Exception:
            batch_size = 500
        batch_size = max(1, min(batch_size, 5000))

        payload = job.payload_template or {}
        coupon_type_id = payload.get("coupon_type_id") or payload.get("couponTypeId")
        if coupon_type_id is not None:
            coupon_type_id = str(coupon_type_id)
        if not coupon_type_id:
            raise ValueError("MAINT_BACKFILL_COUPONS requires payload_template.coupon_type_id")

        frequency = payload.get("frequency") or "ONCE_PER_CALENDAR_YEAR"
        frequency = str(frequency)

        q = db.query(Customer).filter(Customer.brand == job.brand).order_by(Customer.id.asc())
        if after_id:
            q = q.filter(Customer.id > after_id)

        customers = q.limit(batch_size).all()

        bucket_key = compute_run_bucket_key_from_schedule(now_utc=now, schedule=job.schedule)

        processed = 0
        created = 0
        idempotent_existing = 0
        failed = 0
        last_id = None

        from app.services.coupon_service import issue_coupon

        for c in customers:
            processed += 1
            last_id = c.id

            event_id = f"job_{job.id}_{bucket_key}_backfill_coupon_{c.id}_{coupon_type_id}"

            tx = (
                db.query(Transaction)
                .filter(Transaction.brand == c.brand)
                .filter(Transaction.transaction_id == event_id)
                .first()
            )
            if tx:
                idempotent_existing += 1
                continue

            tx = Transaction(
                brand=c.brand,
                profile_id=c.profile_id,
                transaction_type="MAINTENANCE",
                transaction_id=event_id,
                source="INTERNAL_JOB",
                payload={
                    "job_key": job.job_key,
                    "job_id": str(job.id),
                    "coupon_type_id": coupon_type_id,
                    "frequency": frequency,
                },
                status="PROCESSED",
                idempotency_key=None,
            )
            db.add(tx)
            db.flush()

            idem = f"backfill_coupon:{job.id}:{bucket_key}:{c.id}:{coupon_type_id}"
            try:
                issue_coupon(
                    db,
                    customer=c,
                    transaction=tx,
                    coupon_type_id=coupon_type_id,
                    frequency=frequency,
                    rule_id=None,
                    rule_execution_id=None,
                    idempotency_key=idem,
                )
                created += 1
            except Exception:
                failed += 1

        finished = len(customers) < batch_size
        if finished:
            job.selector = {"batch_size": batch_size}
        else:
            job.selector = {"after_id": str(last_id), "batch_size": batch_size}

        db.flush()
        return CouponBackfillRunStats(
            processed=processed,
            created=created,
            idempotent_existing=idempotent_existing,
            failed=failed,
            finished=bool(finished),
        )

    today: date = now.date()

    q = db.query(Customer)
    if job.brand:
        q = q.filter(Customer.brand == job.brand)

    if getattr(job, "segment_id", None) is not None:
        q = q.join(SegmentMember, SegmentMember.customer_id == Customer.id)
        q = q.filter(SegmentMember.segment_id == job.segment_id)

    from app.routes.internal_jobs import _apply_selector

    q = _apply_selector(q, job.selector or {}, today)
    customers = q.all()

    bucket_key = compute_run_bucket_key_from_schedule(now_utc=now, schedule=job.schedule)

    processed = 0
    created = 0
    idempotent_existing = 0
    failed = 0

    for c in customers:
        processed += 1
        transaction_id = f"job_{job.id}_{bucket_key}_{c.brand}_{c.profile_id}"

        already_exists = (
            db.query(Transaction.id)
            .filter(Transaction.transaction_id == transaction_id)
            .filter(Transaction.brand == c.brand)
            .first()
        )
        if already_exists:
            idempotent_existing += 1
            continue

        payload = job.payload_template or {}
        event = EventCreate(
            brand=c.brand,
            profileId=c.profile_id,
            eventType=job.transaction_type,
            eventId=transaction_id,
            source="INTERNAL_JOB",
            payload=payload,
        )

        try:
            create_transaction(db, event)
            created += 1
        except Exception:
            failed += 1

    return InternalJobRunStats(
        processed=processed,
        created=created,
        idempotent_existing=idempotent_existing,
        failed=failed,
    )
