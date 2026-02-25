from datetime import date, datetime, timedelta
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import extract
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.customer import Customer
from app.models.customer_tag import CustomerTag
from app.models.event_type import EventType
from app.models.internal_job import InternalJob
from app.models.loyalty_tier import LoyaltyTier
from app.models.transaction import Transaction
from app.schemas.event import EventCreate
from app.schemas.internal_job import InternalJobCreate, InternalJobOut, InternalJobUpdate
from app.schemas.internal_job_selector_catalog import get_internal_job_selector_catalog
from app.schemas.internal_job_type_catalog import get_internal_job_type_catalog
from app.services.internal_job_runner import compute_next_run_at_from_schedule, run_internal_job_once
from app.services.transaction_service import create_transaction


router = APIRouter(prefix="/admin/internal-jobs", tags=["admin-internal-jobs"])


@router.get("/ui-catalog")
def get_internal_jobs_ui_catalog():

    def _model_json_schema(model_cls):
        fn = getattr(model_cls, "model_json_schema", None)
        if callable(fn):
            return fn()
        return model_cls.schema()

    return {
        "job": {
            "jsonSchema": _model_json_schema(InternalJobCreate),
            "uiHints": {
                "job_key": {"widget": "text", "placeholder": "ex: BIRTHDAY_2026"},
                "brand": {"widget": "hidden"},
                "event_type": {
                    "widget": "remote_select",
                    "datasource": {
                        "endpoint": "/admin/ui-options/event-types",
                        "method": "GET",
                        "query": {"origin": "INTERNAL", "active": True},
                        "valueField": "key",
                        "labelField": "name",
                        "brandVia": "X-Brand",
                    },
                },
                "selector": {
                    "widget": "internal_job_selector_builder",
                    "catalog": {"path": "$.selector"},
                },
                "payload_template": {
                    "widget": "json_object",
                },
                "schedule": {
                    "widget": "cron_builder",
                    "help": {"path": "$.cronHelp"},
                },
                "active": {"widget": "switch"},
                "first_run_at": {"widget": "datetime"},
                "start_in_seconds": {"widget": "number"},
            },
        },
        "cronHelp": {
            "timezone": {
                "default": "UTC",
                "examples": ["UTC", "Europe/Paris", "America/New_York"],
                "note": "Timezone is applied when evaluating the cron schedule. The next_run_at stored in DB is UTC.",
            },
            "format": {
                "type": "cron",
                "fields": [
                    {"name": "minute", "range": "0-59", "special": ["*", "*/n", "m,n", "m-n"]},
                    {"name": "hour", "range": "0-23", "special": ["*", "*/n", "h1,h2", "h1-h2"]},
                    {"name": "dayOfMonth", "range": "1-31", "special": ["*", "m,n", "m-n"]},
                    {"name": "month", "range": "1-12", "special": ["*", "m,n", "m-n"]},
                    {"name": "dayOfWeek", "range": "0-6", "aliases": {"0": "SUN", "1": "MON", "2": "TUE", "3": "WED", "4": "THU", "5": "FRI", "6": "SAT"}},
                ],
            },
            "templates": [
                {
                    "id": "every_n_minutes",
                    "label": "Every N minutes",
                    "params": {"n": {"type": "number", "min": 1, "max": 59, "default": 15}},
                    "example": "*/15 * * * *",
                },
                {
                    "id": "every_hour_at_minute",
                    "label": "Every hour at minute M",
                    "params": {"minute": {"type": "number", "min": 0, "max": 59, "default": 0}},
                    "example": "0 * * * *",
                },
                {
                    "id": "daily_at",
                    "label": "Every day at HH:MM",
                    "params": {
                        "hour": {"type": "number", "min": 0, "max": 23, "default": 9},
                        "minute": {"type": "number", "min": 0, "max": 59, "default": 0},
                    },
                    "example": "0 9 * * *",
                },
                {
                    "id": "weekly_at",
                    "label": "Every week on weekday at HH:MM",
                    "params": {
                        "weekday": {"type": "select", "options": ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"], "default": "MON"},
                        "hour": {"type": "number", "min": 0, "max": 23, "default": 3},
                        "minute": {"type": "number", "min": 0, "max": 59, "default": 0},
                    },
                    "example": "0 3 * * 1",
                },
            ],
            "examples": [
                {"cron": "0 3 * * 1", "meaning": "Every Monday at 03:00"},
                {"cron": "*/15 * * * *", "meaning": "Every 15 minutes"},
                {"cron": "0 0 * * *", "meaning": "Every day at midnight"},
            ],
        },
        "jobTypes": get_internal_job_type_catalog(),
        "selector": get_internal_job_selector_catalog(),
        "payloadTemplate": {
            "notes": "payload_template is merged into the INTERNAL event payload per selected customer.",
            "uiHints": {
                "payload_template": {"widget": "json_object"},
            },
        },
    }


@router.get("/selector-catalog")
def get_internal_job_selector_catalog_route():
    return get_internal_job_selector_catalog()


@router.get("/ui-bundle")
def get_internal_jobs_ui_bundle(
    brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    event_types = (
        db.query(EventType)
        .filter(EventType.brand == brand)
        .filter(EventType.origin == "INTERNAL")
        .order_by(EventType.key.asc())
        .all()
    )
    tiers = db.query(LoyaltyTier).filter(LoyaltyTier.brand == brand).order_by(LoyaltyTier.rank.asc()).all()
    tags_q = (
        db.query(CustomerTag.tag)
        .join(Customer, Customer.id == CustomerTag.customer_id)
        .filter(Customer.brand == brand)
        .distinct()
        .order_by(CustomerTag.tag.asc())
    )

    return {
        "brand": brand,
        "uiCatalog": get_internal_jobs_ui_catalog(),
        "uiOptions": {
            "eventTypes": {
                "brand": brand,
                "items": [
                    {
                        "id": str(et.id),
                        "key": et.key,
                        "name": et.name,
                        "origin": et.origin,
                        "active": et.active,
                    }
                    for et in event_types
                ],
            },
            "loyaltyTiers": {
                "brand": brand,
                "items": [
                    {
                        "id": str(t.id),
                        "key": t.key,
                        "name": t.name,
                        "rank": t.rank,
                        "minStatusPoints": t.min_status_points,
                        "active": t.active,
                    }
                    for t in tiers
                ],
            },
            "customerTags": {
                "brand": brand,
                "items": [{"tag": row[0]} for row in tags_q.all()],
            },
        },
    }


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
def list_internal_jobs(
    active_brand: str = Depends(get_active_brand),
    active: bool | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(InternalJob).filter(InternalJob.brand == active_brand)
    if active is not None:
        q = q.filter(InternalJob.active.is_(active))
    return q.order_by(InternalJob.created_at.desc()).all()


@router.post("", response_model=InternalJobOut)
def create_internal_job(
    payload: InternalJobCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if payload.brand is not None and payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")
    q = (
        db.query(EventType.id)
        .filter(
            EventType.key == payload.event_type,
            EventType.active.is_(True),
            EventType.origin == "INTERNAL",
        )
    )
    q = q.filter(EventType.brand == active_brand)
    exists = q.first()
    if not exists:
        raise HTTPException(status_code=400, detail="Unknown/inactive event_type or not INTERNAL. Create it in /admin/event-types first.")

    schedule_dict = payload.schedule.model_dump() if payload.schedule is not None else None

    job = InternalJob(
        job_key=payload.job_key,
        brand=active_brand,
        event_type=payload.event_type,
        selector=payload.selector,
        payload_template=payload.payload_template,
        active=payload.active,
        schedule=schedule_dict,
    )

    if payload.active and schedule_dict is not None:
        now = datetime.utcnow()
        if payload.first_run_at is not None:
            job.next_run_at = payload.first_run_at
        elif payload.start_in_seconds is not None:
            job.next_run_at = now + timedelta(seconds=int(payload.start_in_seconds))
        else:
            job.next_run_at = compute_next_run_at_from_schedule(base_utc=now, schedule=schedule_dict)
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


@router.get("/{job_id}", response_model=InternalJobOut)
def get_internal_job(
    job_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    job = db.query(InternalJob).filter(InternalJob.id == job_id).first()
    if not job or job.brand != active_brand:
        raise HTTPException(status_code=404, detail="Internal job not found")
    return job


@router.patch("/{job_id}", response_model=InternalJobOut)
def update_internal_job(
    job_id: UUID,
    payload: InternalJobUpdate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    job = db.query(InternalJob).filter(InternalJob.id == job_id).first()
    if not job or job.brand != active_brand:
        raise HTTPException(status_code=404, detail="Internal job not found")

    data = payload.model_dump(exclude_unset=True)

    if "schedule" in data and data["schedule"] is not None:
        fn = getattr(data["schedule"], "model_dump", None)
        if callable(fn):
            data["schedule"] = fn()

    if "brand" in data and data["brand"] is not None and data["brand"] != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    if "event_type" in data and data["event_type"]:
        q = (
            db.query(EventType.id)
            .filter(
                EventType.key == data["event_type"],
                EventType.active.is_(True),
                EventType.origin == "INTERNAL",
            )
        )
        q = q.filter(EventType.brand == active_brand)
        exists = q.first()
        if not exists:
            raise HTTPException(status_code=400, detail="Unknown/inactive event_type or not INTERNAL. Create it in /admin/event-types first.")

    for k, v in data.items():
        if k == "brand":
            continue
        setattr(job, k, v)

    if "schedule" in data or "active" in data or "first_run_at" in data or "start_in_seconds" in data:
        if job.active and job.schedule is not None:
            if "first_run_at" in data and data.get("first_run_at") is not None:
                job.next_run_at = data["first_run_at"]
            elif "start_in_seconds" in data and data.get("start_in_seconds") is not None:
                job.next_run_at = datetime.utcnow() + timedelta(seconds=int(data["start_in_seconds"]))
            elif job.next_run_at is None:
                job.next_run_at = compute_next_run_at_from_schedule(base_utc=datetime.utcnow(), schedule=job.schedule)
        else:
            job.next_run_at = None

    db.commit()
    db.refresh(job)
    return job


@router.delete("/{job_id}")
def delete_internal_job(
    job_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    job = db.query(InternalJob).filter(InternalJob.id == job_id).first()
    if not job or job.brand != active_brand:
        raise HTTPException(status_code=404, detail="Internal job not found")

    db.delete(job)
    db.commit()
    return {"deleted": True}


@router.post("/{job_id}/preview")
def preview_internal_job(
    job_id: UUID,
    limit: int = 50,
    offset: int = 0,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    job = db.query(InternalJob).filter(InternalJob.id == job_id).first()
    if not job or job.brand != active_brand:
        raise HTTPException(status_code=404, detail="Internal job not found")

    if not job.active:
        raise HTTPException(status_code=400, detail="Internal job is inactive")

    today = date.today()

    q = db.query(Customer)
    q = q.filter(Customer.brand == active_brand)

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
def run_internal_job(
    job_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    job = db.query(InternalJob).filter(InternalJob.id == job_id).first()
    if not job or job.brand != active_brand:
        raise HTTPException(status_code=404, detail="Internal job not found")

    if not job.active:
        raise HTTPException(status_code=400, detail="Internal job is inactive")

    now = datetime.utcnow()
    try:
        stats = run_internal_job_once(db, job=job, now=now)
        job.last_status = "SUCCESS"
        job.last_error = None
    except Exception as e:
        job.last_status = "FAILED"
        job.last_error = str(e)
        raise
    finally:
        job.last_run_at = now
        job.next_run_at = compute_next_run_at_from_schedule(base_utc=now, schedule=job.schedule)
        db.commit()
        db.refresh(job)

    return {
        "jobId": str(job.id),
        "jobKey": job.job_key,
        "brand": job.brand,
        "eventType": job.event_type,
        "date": now.date().isoformat(),
        "targetCustomers": stats.processed,
        "created": stats.created,
        "idempotentExisting": stats.idempotent_existing,
        "failed": stats.failed,
    }
