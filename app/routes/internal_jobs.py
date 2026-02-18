from datetime import date, datetime, timedelta
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import extract
from sqlalchemy.orm import Session

from app.db import get_db
from app.models.customer import Customer
from app.models.event_type import EventType
from app.models.internal_job import InternalJob
from app.models.transaction import Transaction
from app.schemas.event import EventCreate
from app.schemas.internal_job import InternalJobCreate, InternalJobOut, InternalJobUpdate
from app.services.transaction_service import create_transaction


router = APIRouter(prefix="/admin/internal-jobs", tags=["admin-internal-jobs"])


def _selector_to_criterion(selector: dict, today: date):
    if not selector:
        return None

    if "all" in selector:
        from sqlalchemy import and_

        parts = []
        for s in selector.get("all") or []:
            c = _selector_to_criterion(s, today)
            if c is not None:
                parts.append(c)
        if not parts:
            return None
        return and_(*parts)

    if "any" in selector:
        from sqlalchemy import or_

        parts = []
        for s in selector.get("any") or []:
            c = _selector_to_criterion(s, today)
            if c is not None:
                parts.append(c)
        if not parts:
            return None
        return or_(*parts)

    if selector.get("birthdate_today") is True:
        return (
            Customer.birthdate.isnot(None)
            & (extract("month", Customer.birthdate) == today.month)
            & (extract("day", Customer.birthdate) == today.day)
        )

    if selector.get("created_anniversary_today") is True:
        return (
            Customer.created_at.isnot(None)
            & (extract("month", Customer.created_at) == today.month)
            & (extract("day", Customer.created_at) == today.day)
        )

    inactive_days_gte = selector.get("inactive_days_gte")
    if inactive_days_gte is not None:
        try:
            days = int(inactive_days_gte)
        except Exception:
            raise HTTPException(status_code=400, detail="inactive_days_gte must be an integer")
        cutoff = datetime.utcnow() - timedelta(days=days)
        return Customer.last_activity_at.isnot(None) & (Customer.last_activity_at <= cutoff)

    status_in = selector.get("status_in")
    if status_in is not None:
        if not isinstance(status_in, list) or not status_in:
            raise HTTPException(status_code=400, detail="status_in must be a non-empty list")
        return Customer.status.in_(status_in)

    loyalty_in = selector.get("loyalty_status_in")
    if loyalty_in is not None:
        if not isinstance(loyalty_in, list) or not loyalty_in:
            raise HTTPException(status_code=400, detail="loyalty_status_in must be a non-empty list")
        return Customer.loyalty_status.in_(loyalty_in)

    lifetime_gte = selector.get("lifetime_points_gte")
    if lifetime_gte is not None:
        return Customer.lifetime_points >= int(lifetime_gte)

    return None


def _apply_selector(q, selector: dict, today: date):
    if not selector:
        return q

    criterion = _selector_to_criterion(selector, today)
    if criterion is None:
        return q
    return q.filter(criterion)


@router.get("", response_model=list[InternalJobOut])
def list_internal_jobs(active: bool | None = None, db: Session = Depends(get_db)):
    q = db.query(InternalJob)
    if active is not None:
        q = q.filter(InternalJob.active.is_(active))
    return q.order_by(InternalJob.created_at.desc()).all()


@router.post("", response_model=InternalJobOut)
def create_internal_job(payload: InternalJobCreate, db: Session = Depends(get_db)):
    q = (
        db.query(EventType.id)
        .filter(
            EventType.key == payload.event_type,
            EventType.active.is_(True),
            EventType.origin == "INTERNAL",
        )
    )
    if payload.brand:
        q = q.filter(EventType.brand == payload.brand)
    else:
        q = q.filter(EventType.brand.is_(None))
    exists = q.first()
    if not exists:
        raise HTTPException(status_code=400, detail="Unknown/inactive event_type or not INTERNAL. Create it in /admin/event-types first.")

    job = InternalJob(
        job_key=payload.job_key,
        brand=payload.brand,
        event_type=payload.event_type,
        selector=payload.selector,
        payload_template=payload.payload_template,
        active=payload.active,
        schedule=payload.schedule,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


@router.get("/{job_id}", response_model=InternalJobOut)
def get_internal_job(job_id: UUID, db: Session = Depends(get_db)):
    job = db.query(InternalJob).filter(InternalJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Internal job not found")
    return job


@router.patch("/{job_id}", response_model=InternalJobOut)
def update_internal_job(job_id: UUID, payload: InternalJobUpdate, db: Session = Depends(get_db)):
    job = db.query(InternalJob).filter(InternalJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Internal job not found")

    data = payload.model_dump(exclude_unset=True)

    if "event_type" in data and data["event_type"]:
        next_brand = data.get("brand", job.brand)
        q = (
            db.query(EventType.id)
            .filter(
                EventType.key == data["event_type"],
                EventType.active.is_(True),
                EventType.origin == "INTERNAL",
            )
        )
        if next_brand:
            q = q.filter(EventType.brand == next_brand)
        else:
            q = q.filter(EventType.brand.is_(None))
        exists = q.first()
        if not exists:
            raise HTTPException(status_code=400, detail="Unknown/inactive event_type or not INTERNAL. Create it in /admin/event-types first.")

    for k, v in data.items():
        setattr(job, k, v)

    db.commit()
    db.refresh(job)
    return job


@router.delete("/{job_id}")
def delete_internal_job(job_id: UUID, db: Session = Depends(get_db)):
    job = db.query(InternalJob).filter(InternalJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Internal job not found")

    db.delete(job)
    db.commit()
    return {"deleted": True}


@router.post("/{job_id}/preview")
def preview_internal_job(job_id: UUID, limit: int = 50, offset: int = 0, db: Session = Depends(get_db)):
    job = db.query(InternalJob).filter(InternalJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Internal job not found")

    if not job.active:
        raise HTTPException(status_code=400, detail="Internal job is inactive")

    today = date.today()

    q = db.query(Customer)
    if job.brand:
        q = q.filter(Customer.brand == job.brand)

    q = _apply_selector(q, job.selector or {}, today)

    total = q.count()

    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    sample = (
        q.order_by(Customer.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )

    return {
        "jobId": str(job.id),
        "jobKey": job.job_key,
        "brand": job.brand,
        "eventType": job.event_type,
        "date": today.isoformat(),
        "count": total,
        "sample": [{"brand": c.brand, "profileId": c.profile_id} for c in sample],
    }


@router.post("/{job_id}/run")
def run_internal_job(job_id: UUID, db: Session = Depends(get_db)):
    job = db.query(InternalJob).filter(InternalJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Internal job not found")

    if not job.active:
        raise HTTPException(status_code=400, detail="Internal job is inactive")

    today = date.today()

    q = db.query(Customer)
    if job.brand:
        q = q.filter(Customer.brand == job.brand)

    q = _apply_selector(q, job.selector or {}, today)

    customers = q.all()

    processed = 0
    created = 0
    idempotent_existing = 0
    failed = 0
    results: list[dict] = []

    for c in customers:
        processed += 1
        event_id = f"job_{job.id}_{today.isoformat()}_{c.brand}_{c.profile_id}"

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
            tx = create_transaction(db, event)
            results.append(
                {
                    "brand": c.brand,
                    "profileId": c.profile_id,
                    "eventId": event_id,
                    "transactionId": str(tx.id),
                    "status": tx.status,
                    "idempotent": bool(already_exists),
                }
            )
            if not already_exists:
                created += 1
        except Exception as e:
            failed += 1
            results.append(
                {
                    "brand": c.brand,
                    "profileId": c.profile_id,
                    "eventId": event_id,
                    "error": str(e),
                }
            )

    return {
        "jobId": str(job.id),
        "jobKey": job.job_key,
        "brand": job.brand,
        "eventType": job.event_type,
        "date": today.isoformat(),
        "targetCustomers": processed,
        "created": created,
        "idempotentExisting": idempotent_existing,
        "failed": failed,
        "results": results,
    }
