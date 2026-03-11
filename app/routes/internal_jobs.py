from datetime import date, datetime, timedelta
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import extract
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.customer import Customer
from app.models.event_type import TransactionType
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


def _is_system_managed_job(job: InternalJob) -> bool:
    return job.job_key == "MAINT_EXPIRE_REWARDS"


def _selector_literal(value):
    from sqlalchemy import literal

    return literal(value)


def _as_mmdd(value):
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        if len(s) == 5 and s[2] == "-":
            try:
                mm = int(s[0:2])
                dd = int(s[3:5])
            except Exception:
                return None
            if mm < 1 or mm > 12 or dd < 1 or dd > 31:
                return None
            return mm * 100 + dd
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            try:
                mm = int(s[5:7])
                dd = int(s[8:10])
            except Exception:
                return None
            if mm < 1 or mm > 12 or dd < 1 or dd > 31:
                return None
            return mm * 100 + dd
    if hasattr(value, "month") and hasattr(value, "day"):
        try:
            return int(value.month) * 100 + int(value.day)
        except Exception:
            return None
    return None


def _resolve_selector_value(*, value, today: date, now_utc: datetime):
    if isinstance(value, dict) and "$system" in value:
        key = value.get("$system")
        if key == "now":
            return now_utc
        raise HTTPException(status_code=400, detail=f"Unknown system value preset: {key}")
    return value


def _resolve_selector_field(*, field: str, today: date, now_utc: datetime):
    if not isinstance(field, str) or not field:
        raise HTTPException(status_code=400, detail="Selector leaf requires non-empty 'field'")

    if field.startswith("customer."):
        key = field[len("customer.") :]
        if key == "gender":
            return Customer.gender
        if key == "status":
            return Customer.status
        if key == "loyalty_status":
            return Customer.loyalty_status
        if key == "lifetime_points":
            return Customer.lifetime_points
        if key == "created_at":
            return Customer.created_at
        if key == "last_activity_at":
            return Customer.last_activity_at
        if key == "birthdate":
            return (Customer.birth_month * 100) + Customer.birth_day

        raise HTTPException(status_code=400, detail=f"Unknown selector customer field: {field}")

    if field.startswith("system."):
        key = field[len("system.") :]
        if key == "now":
            return _selector_literal(now_utc)

        raise HTTPException(status_code=400, detail=f"Unknown selector system field: {field}")

    raise HTTPException(
        status_code=400,
        detail=f"Unsupported selector field namespace: {field}. Use customer.* or system.*",
    )


def _selector_compare(*, op: str, expr, value):
    op = (op or "").lower()

    if op in {"eq", "="}:
        return expr == value
    if op in {"neq", "!=", "ne"}:
        return expr != value

    if op == "exists":
        # exists true => is not null, exists false => is null
        if value is None:
            return expr.isnot(None)
        return expr.isnot(None) if bool(value) else expr.is_(None)

    if op == "in":
        if not isinstance(value, list) or not value:
            raise HTTPException(status_code=400, detail="Selector operator 'in' requires non-empty list value")
        return expr.in_(value)

    if op == "between":
        if not isinstance(value, list) or len(value) != 2:
            raise HTTPException(status_code=400, detail="Selector operator 'between' requires [lo, hi]")
        return expr.between(value[0], value[1])

    if op in {"gt", "gte", "lt", "lte"}:
        if op == "gt":
            return expr > value
        if op == "gte":
            return expr >= value
        if op == "lt":
            return expr < value
        if op == "lte":
            return expr <= value

    if op == "contains":
        if value is None:
            raise HTTPException(status_code=400, detail="Selector operator 'contains' requires a value")
        return expr.ilike(f"%{value}%")

    raise HTTPException(status_code=400, detail=f"Unsupported selector operator: {op}")


def _selector_ast_to_criterion(selector: dict, *, today: date, now_utc: datetime):
    if selector is None:
        return None
    if selector == {}:
        return None
    if not isinstance(selector, dict):
        raise HTTPException(status_code=400, detail="Invalid selector format: expected object")

    # Strict AST combinators
    if "and" in selector:
        from sqlalchemy import and_

        items = selector.get("and")
        if not isinstance(items, list):
            raise HTTPException(status_code=400, detail="Invalid selector 'and': expected list")
        parts = []
        for s in items:
            c = _selector_ast_to_criterion(s, today=today, now_utc=now_utc)
            if c is not None:
                parts.append(c)
        if not parts:
            return None
        return and_(*parts)

    if "or" in selector:
        from sqlalchemy import or_

        items = selector.get("or")
        if not isinstance(items, list):
            raise HTTPException(status_code=400, detail="Invalid selector 'or': expected list")
        parts = []
        for s in items:
            c = _selector_ast_to_criterion(s, today=today, now_utc=now_utc)
            if c is not None:
                parts.append(c)
        if not parts:
            return None
        return or_(*parts)

    if "not" in selector:
        from sqlalchemy import not_

        inner = _selector_ast_to_criterion(selector.get("not"), today=today, now_utc=now_utc)
        if inner is None:
            return None
        return not_(inner)

    # Leaf
    if "field" in selector:
        field = selector.get("field")
        op = selector.get("operator")
        if op is None:
            op = selector.get("op")
        if not op:
            raise HTTPException(status_code=400, detail="Selector leaf requires 'operator' (or alias 'op')")
        value = _resolve_selector_value(value=selector.get("value"), today=today, now_utc=now_utc)

        if field == "customer.birthdate":
            op_norm = (op or "").lower()
            if isinstance(value, list):
                parsed = [_as_mmdd(v) for v in value]
                if any(v is None for v in parsed):
                    raise HTTPException(status_code=400, detail="customer.birthdate values must be in format YYYY-MM-DD or MM-DD")
                value = parsed
            else:
                value = _as_mmdd(value)
                if op_norm != "exists" and value is None:
                    raise HTTPException(status_code=400, detail="customer.birthdate must be in format YYYY-MM-DD or MM-DD")

        expr = _resolve_selector_field(field=field, today=today, now_utc=now_utc)
        return _selector_compare(op=op, expr=expr, value=value)

    # Legacy guard (explicit)
    legacy_keys = {
        "all",
        "any",
        "birthdate_today",
        "created_anniversary_today",
        "inactive_days_gte",
        "status_in",
        "loyalty_status_in",
        "lifetime_points_gte",
    }
    found = [k for k in legacy_keys if k in selector]
    if found:
        raise HTTPException(status_code=400, detail=f"Legacy selector keys not supported: {found}")

    raise HTTPException(
        status_code=400,
        detail="Invalid selector format: expected {'and':[...]}, {'or':[...]}, {'not':...} or leaf {'field':..., 'operator':..., 'value':...}",
    )


@router.get("/ui-catalog")
def get_internal_jobs_ui_catalog():

    def _model_json_schema(model_cls):
        fn = getattr(model_cls, "model_json_schema", None)
        if callable(fn):
            return fn()
        return model_cls.schema()

    return {
        "create": {
            "jsonSchema": _model_json_schema(InternalJobCreate),
            "uiHints": {
                "job_key": {"widget": "hidden"},
                "brand": {"widget": "hidden"},
                "transaction_type": {
                    "widget": "remote_select",
                    "datasource": {
                        "endpoint": "/admin/ui-options/transaction-types",
                        "method": "GET",
                        "query": {"origin": "INTERNAL", "active": True},
                        "valueField": "key",
                        "labelField": "name",
                        "brandVia": "X-Brand",
                    },
                },
                "selector": {"widget": "internal_job_selector_builder", "catalog": {"endpoint": "/admin/internal-jobs/selector-catalog"}},
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


def _slug_key(value: str) -> str:
    s = (value or "").strip().lower()
    out = []
    prev_us = False
    for ch in s:
        ok = ("a" <= ch <= "z") or ("0" <= ch <= "9")
        if ok:
            out.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    key = "".join(out).strip("_")
    return key or "job"


@router.get("/selector-catalog")
def get_internal_job_selector_catalog_route():
    return get_internal_job_selector_catalog()


@router.get("/ui-bundle")
def get_internal_jobs_ui_bundle(
    brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    event_types = (
        db.query(TransactionType)
        .filter(TransactionType.brand == brand)
        .filter(TransactionType.origin == "INTERNAL")
        .order_by(TransactionType.key.asc())
        .all()
    )
    tiers = db.query(LoyaltyTier).filter(LoyaltyTier.brand == brand).order_by(LoyaltyTier.rank.asc()).all()
    return {
        "brand": brand,
        "uiCatalog": get_internal_jobs_ui_catalog(),
        "uiOptions": {
            "transactionTypes": {
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
        },
    }


def _apply_selector(q, selector: dict, today: date):
    if not selector:
        return q

    now_utc = datetime.utcnow()
    criterion = _selector_ast_to_criterion(selector, today=today, now_utc=now_utc)
    if criterion is None:
        return q
    return q.filter(criterion)


@router.get("", response_model=list[InternalJobOut])
def list_internal_jobs(
    active_brand: str = Depends(get_active_brand),
    active: bool | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(InternalJob).filter(InternalJob.brand == active_brand).filter(InternalJob.job_key != "MAINT_EXPIRE_REWARDS")
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
    if payload.job_key == "MAINT_EXPIRE_REWARDS":
        raise HTTPException(status_code=400, detail="This internal job is system-managed")
    q = (
        db.query(TransactionType.id)
        .filter(
            TransactionType.key == payload.transaction_type,
            TransactionType.active.is_(True),
            TransactionType.origin == "INTERNAL",
        )
    )
    q = q.filter(TransactionType.brand == active_brand)
    exists = q.first()
    if not exists:
        raise HTTPException(status_code=400, detail="Unknown/inactive transaction_type or not INTERNAL. Create it in /admin/transaction-types first.")

    schedule_dict = payload.schedule.model_dump() if payload.schedule is not None else None

    name_in = (payload.name or "").strip() or None
    if not name_in:
        raise HTTPException(status_code=400, detail="name is required")

    job_key_in = (payload.job_key or "").strip() or None
    job_key = job_key_in or _slug_key(payload.transaction_type)
    base = job_key
    i = 1
    while (
        db.query(InternalJob.id)
        .filter(InternalJob.brand == active_brand)
        .filter(InternalJob.job_key == job_key)
        .first()
    ):
        i += 1
        job_key = f"{base}_{i}"

    existing_name = (
        db.query(InternalJob.id)
        .filter(InternalJob.brand == active_brand)
        .filter(InternalJob.name == name_in)
        .first()
    )
    if existing_name:
        raise HTTPException(status_code=400, detail="Internal job name already exists")

    job = InternalJob(
        job_key=job_key,
        brand=active_brand,
        name=name_in,
        description=((payload.description or "").strip() or None),
        transaction_type=payload.transaction_type,
        selector=payload.selector or {},
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
    if _is_system_managed_job(job):
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
    if _is_system_managed_job(job):
        raise HTTPException(status_code=400, detail="This internal job is system-managed")

    data = payload.model_dump(exclude_unset=True)

    if "name" in data:
        name_in = (data.get("name") or "").strip() or None
        if not name_in:
            raise HTTPException(status_code=400, detail="name is required")
        existing_name = (
            db.query(InternalJob.id)
            .filter(InternalJob.brand == active_brand)
            .filter(InternalJob.name == name_in)
            .filter(InternalJob.id != job.id)
            .first()
        )
        if existing_name:
            raise HTTPException(status_code=400, detail="Internal job name already exists")
        data["name"] = name_in

    if "schedule" in data and data["schedule"] is not None:
        fn = getattr(data["schedule"], "model_dump", None)
        if callable(fn):
            data["schedule"] = fn()

    if "brand" in data and data["brand"] is not None and data["brand"] != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    if "transaction_type" in data and data["transaction_type"]:
        q = (
            db.query(TransactionType.id)
            .filter(
                TransactionType.key == data["transaction_type"],
                TransactionType.active.is_(True),
                TransactionType.origin == "INTERNAL",
            )
        )
        q = q.filter(TransactionType.brand == active_brand)
        exists = q.first()
        if not exists:
            raise HTTPException(status_code=400, detail="Unknown/inactive transaction_type or not INTERNAL. Create it in /admin/transaction-types first.")

    for k, v in data.items():
        if k == "brand":
            continue
        if k in {"name", "description"} and isinstance(v, str):
            v = v.strip() or None
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
    if _is_system_managed_job(job):
        raise HTTPException(status_code=400, detail="This internal job is system-managed")

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
    if _is_system_managed_job(job):
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
        "transactionType": job.transaction_type,
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
    if _is_system_managed_job(job):
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
        "transactionType": job.transaction_type,
        "date": now.date().isoformat(),
        "targetCustomers": stats.processed,
        "created": stats.created,
        "idempotentExisting": stats.idempotent_existing,
        "failed": stats.failed,
    }
