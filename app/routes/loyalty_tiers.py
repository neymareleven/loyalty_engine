from uuid import UUID
import os

from fastapi import APIRouter, Depends, HTTPException
from datetime import datetime
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.customer import Customer
from app.models.internal_job import InternalJob
from app.models.loyalty_tier import LoyaltyTier
from app.schemas.loyalty_tier import LoyaltyTierCreate, LoyaltyTierOut, LoyaltyTierUpdate
from app.services.loyalty_status_service import update_customer_status
from app.services.purchase_service import QUALIFICATION_PURCHASES, get_qualification_mode


router = APIRouter(prefix="/admin/loyalty-tiers", tags=["admin-loyalty-tiers"])


@router.get("/ui-catalog")
def get_loyalty_tiers_ui_catalog():

    def _model_json_schema(model_cls):
        fn = getattr(model_cls, "model_json_schema", None)
        if callable(fn):
            return fn()
        return model_cls.schema()

    return {
        "jsonSchema": _model_json_schema(LoyaltyTierCreate),
        "uiHints": {
            "brand": {"widget": "hidden"},
            "key": {"widget": "hidden"},
            "name": {"widget": "text"},
            "min_status_points": {
                "widget": "number",
                "min": 0,
                "label": "Seuil points (mode POINTS)",
            },
            "min_purchase_count": {
                "widget": "number",
                "min": 0,
                "label": "Seuil nombre d'achats (mode PURCHASES)",
            },
            "min_purchase_amount": {
                "widget": "number",
                "min": 0,
                "label": "Seuil montant total (mode PURCHASES)",
            },
            "rank": {"widget": "hidden"},
            "active": {"widget": "switch"},
        },
        "examples": [
            {
                "name": "Silver",
                "min_status_points": 100,
                "min_purchase_count": 3,
                "min_purchase_amount": 50000,
                "active": True,
            }
        ],
        "notes": {
            "baseTier": (
                "Un tier Base (min_status_points=0, min_purchase_count=0, min_purchase_amount=0) "
                "est créé automatiquement."
            ),
            "qualificationMode": (
                "GET/PATCH /admin/loyalty-settings → qualification_mode=POINTS|PURCHASES. "
                "En mode POINTS, min_status_points est utilisé. "
                "En mode PURCHASES, le client doit atteindre min_purchase_count ET min_purchase_amount."
            ),
            "firstPurchaseBypass": (
                "GET/PATCH /admin/loyalty-settings → first_purchase_bypass_enabled, "
                "first_purchase_bypass_min_amount, first_purchase_bypass_tier_key. "
                "Si activé, le tout premier achat (transaction sale) d'un montant >= min_amount "
                "accorde le palier cible s'il existe déjà, sans attendre les seuils du mode. "
                "Un palier plus élevé déjà mérité par les seuils n'est pas abaissé."
            ),
            "rank": "Calculé automatiquement côté serveur — ne pas envoyer rank=0 sauf pour le tier de base.",
            "hideInactiveModeFields": (
                "Masquer les champs du mode inactif. En POINTS : n’afficher que min_status_points. "
                "En PURCHASES : n’afficher que min_purchase_count et min_purchase_amount. "
                "Omettre les champs cachés (ou les envoyer à 0) : le serveur aligne les seuils "
                "du mode inactif pour garder rank/ordre cohérents au changement de mode."
            ),
            "baseTierByMode": (
                "Le palier de base du mode POINTS a min_status_points=0. "
                "Le palier de base du mode PURCHASES a min_purchase_count=0 et min_purchase_amount=0. "
                "POST /admin/loyalty-tiers/ensure-base-tier crée ou réactive ce palier 0/0/0."
            ),
        },
    }


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


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
    return key or "tier"


def _recompute_customers(db: Session, brand: str) -> dict:
    customers = db.query(Customer).filter(Customer.brand == brand).all()
    updated = 0
    for c in customers:
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
    db.commit()
    return {"brand": brand, "customers": len(customers), "updated": updated}


def _enqueue_recompute_customers_job(
    db: Session,
    brand: str,
    *,
    apply_qualification_mode: bool = False,
    backfill_purchases: bool = False,
    backfill_after_id=None,
) -> None:
    job = (
        db.query(InternalJob)
        .filter(InternalJob.job_key == "MAINT_RECOMPUTE_CUSTOMERS_LOYALTY_STATUS")
        .filter(InternalJob.brand == brand)
        .first()
    )
    if not job:
        job = InternalJob(
            job_key="MAINT_RECOMPUTE_CUSTOMERS_LOYALTY_STATUS",
            brand=brand,
            name="Maintenance: Recompute Customers Loyalty Status",
            description=None,
            transaction_type="MAINTENANCE",
            selector={"batch_size": 500},
            payload_template=None,
            active=True,
            schedule={"type": "cron", "cron": "*/1 * * * *", "timezone": "UTC"},
        )
        db.add(job)
        db.flush()

    from sqlalchemy.orm.attributes import flag_modified

    selector = dict(job.selector or {})
    selector.pop("after_id", None)
    selector.pop("backfill_after_id", None)
    selector.setdefault("batch_size", 500)
    if apply_qualification_mode:
        selector["apply_qualification_mode"] = True
    if backfill_purchases:
        selector["backfill_purchases"] = True
        if backfill_after_id:
            selector["backfill_after_id"] = str(backfill_after_id)
    else:
        selector.pop("backfill_purchases", None)
    job.selector = selector
    flag_modified(job, "selector")
    job.active = True
    job.next_run_at = datetime.utcnow()
    db.commit()


def _tier_mode_order_key(tier: LoyaltyTier, mode: str) -> tuple:
    points = int(tier.min_status_points or 0)
    count = int(getattr(tier, "min_purchase_count", 0) or 0)
    amount = int(getattr(tier, "min_purchase_amount", 0) or 0)
    created = getattr(tier, "created_at", None)
    if mode == QUALIFICATION_PURCHASES:
        return (count, amount, points, created or datetime.min)
    return (points, count, amount, created or datetime.min)


def _recompute_tier_ranks(db: Session, brand: str) -> None:
    mode = get_qualification_mode(db, brand=brand)
    tiers = db.query(LoyaltyTier).filter(LoyaltyTier.brand == brand).all()
    tiers.sort(key=lambda t: _tier_mode_order_key(t, mode))
    for i, t in enumerate(tiers):
        t.rank = i
    db.flush()


def _align_inactive_mode_thresholds(db: Session, brand: str) -> None:
    """Keep hidden-mode thresholds unique and strictly increasing in the active-mode order.

    The palier order follows the active qualification mode. The other mode's minima are
    auto-adjusted so switching mode does not invert ranks.
    """
    mode = get_qualification_mode(db, brand=brand)
    tiers = db.query(LoyaltyTier).filter(LoyaltyTier.brand == brand).all()
    if not tiers:
        return
    ordered = sorted(tiers, key=lambda t: _tier_mode_order_key(t, mode))
    if mode == QUALIFICATION_PURCHASES:
        prev_points = -1
        for i, t in enumerate(ordered):
            points = int(t.min_status_points or 0)
            if i == 0:
                if points != 0:
                    t.min_status_points = 0
                prev_points = 0
            elif points <= prev_points:
                t.min_status_points = prev_points + 1
                prev_points = int(t.min_status_points)
            else:
                prev_points = points
    else:
        prev_count, prev_amount = -1, -1
        for i, t in enumerate(ordered):
            count = int(getattr(t, "min_purchase_count", 0) or 0)
            amount = int(getattr(t, "min_purchase_amount", 0) or 0)
            if i == 0:
                t.min_purchase_count = 0
                t.min_purchase_amount = 0
                prev_count, prev_amount = 0, 0
                continue
            if count < prev_count:
                count = prev_count
                t.min_purchase_count = count
            if count == prev_count and amount <= prev_amount:
                amount = prev_amount + 1
                t.min_purchase_amount = amount
            prev_count, prev_amount = count, amount
    db.flush()
    _recompute_tier_ranks(db, brand)


def unique_min_status_points(used: set[int], desired: int) -> int:
    """Next value that satisfies uq_loyalty_tiers_brand_min_status_points."""
    desired = int(desired or 0)
    if desired not in used:
        return desired
    return (max(used) + 1) if used else 0


def _used_min_status_points(db: Session, brand: str, *, exclude_id: UUID | None = None) -> set[int]:
    q = db.query(LoyaltyTier.min_status_points).filter(LoyaltyTier.brand == brand)
    if exclude_id is not None:
        q = q.filter(LoyaltyTier.id != exclude_id)
    return {int(v[0] or 0) for v in q.all()}


def _initial_rank_for_new_tier(*, db: Session, brand: str, min_status_points: int) -> int:
    """Pick a rank valid for DB flush before _recompute_tier_ranks.

    Constraint ck_loyalty_tiers_rank0_min0: rank=0 requires min_status_points=0.
    """
    min_i = int(min_status_points)
    if min_i == 0:
        return 0
    existing_count = (
        db.query(func.count(LoyaltyTier.id)).filter(LoyaltyTier.brand == brand).scalar() or 0
    )
    return max(int(existing_count), 1)


def _as_non_negative_int(value, *, field: str) -> int:
    if value is None:
        raise HTTPException(status_code=400, detail=f"{field} is required")
    try:
        parsed = int(value)
    except Exception:
        raise HTTPException(status_code=400, detail=f"{field} must be a whole number")
    if parsed < 0:
        raise HTTPException(status_code=400, detail=f"{field} must be 0 or more")
    return parsed


def _validate_tier_payload(
    *,
    db: Session,
    brand: str,
    tier_id: UUID | None,
    key: str,
    name: str,
    rank: int,
    min_status_points: int,
    min_purchase_count: int = 0,
    min_purchase_amount: int = 0,
    active: bool,
):
    if not (name and str(name).strip()):
        raise HTTPException(status_code=400, detail="Level name is required")

    min_i = _as_non_negative_int(min_status_points, field="min_status_points")
    min_count = _as_non_negative_int(min_purchase_count, field="min_purchase_count")
    min_amount = _as_non_negative_int(min_purchase_amount, field="min_purchase_amount")

    dup_name_q = db.query(LoyaltyTier.id).filter(LoyaltyTier.brand == brand).filter(LoyaltyTier.name == name)
    if tier_id is not None:
        dup_name_q = dup_name_q.filter(LoyaltyTier.id != tier_id)
    if dup_name_q.first():
        raise HTTPException(status_code=400, detail="A level with this name already exists for this brand")

    mode = get_qualification_mode(db, brand=brand)

    tiers = db.query(LoyaltyTier).filter(LoyaltyTier.brand == brand).all()
    simulated = []
    for t in tiers:
        if tier_id is not None and t.id == tier_id:
            simulated.append(
                {
                    "min": min_i,
                    "count": min_count,
                    "amount": min_amount,
                    "key": key,
                    "active": bool(active),
                }
            )
        else:
            simulated.append(
                {
                    "min": int(t.min_status_points),
                    "count": int(getattr(t, "min_purchase_count", 0) or 0),
                    "amount": int(getattr(t, "min_purchase_amount", 0) or 0),
                    "key": t.key,
                    "active": bool(active),
                }
            )
    if tier_id is None:
        simulated.append(
            {
                "min": min_i,
                "count": min_count,
                "amount": min_amount,
                "key": key,
                "active": bool(active),
            }
        )

    if mode == QUALIFICATION_PURCHASES:
        simulated.sort(key=lambda x: (x["count"], x["amount"], x["min"]))
        for prev, cur in zip(simulated, simulated[1:]):
            if (cur["count"], cur["amount"]) <= (prev["count"], prev["amount"]):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Invalid levels: min_purchase_count then min_purchase_amount "
                        "must be unique and strictly increasing"
                    ),
                )
        has_base = any(
            t["active"] and int(t["count"]) == 0 and int(t["amount"]) == 0 for t in simulated
        )
        if not has_base:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Invalid levels: you must have at least one active base level "
                    "with min_purchase_count=0 and min_purchase_amount=0"
                ),
            )
    else:
        simulated.sort(key=lambda x: (x["min"], x["count"], x["amount"]))
        for prev, cur in zip(simulated, simulated[1:]):
            if cur["min"] <= prev["min"]:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Invalid levels: minimum status points must be unique and strictly increasing"
                    ),
                )
        has_base = any(t["active"] and int(t["min"]) == 0 for t in simulated)
        if not has_base:
            raise HTTPException(
                status_code=400,
                detail="Invalid levels: you must have at least one active base level with min_status_points=0",
            )


@router.get("", response_model=list[LoyaltyTierOut])
def list_loyalty_tiers(
    active_brand: str = Depends(get_active_brand),
    brand: str | None = None,
    active: bool | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(LoyaltyTier)
    if brand and brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    q = q.filter(LoyaltyTier.brand == active_brand)
    if active is not None:
        q = q.filter(LoyaltyTier.active.is_(active))
    return q.order_by(LoyaltyTier.brand.asc(), LoyaltyTier.rank.asc(), LoyaltyTier.created_at.asc()).all()


@router.post("", response_model=LoyaltyTierOut)
def create_loyalty_tier(
    payload: LoyaltyTierCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if payload.brand is not None and payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")
    key_in = (payload.key or "").strip() or None
    key = key_in or _slug_key(payload.name)
    base = key
    i = 1
    while (
        db.query(LoyaltyTier.id)
        .filter(LoyaltyTier.brand == active_brand)
        .filter(LoyaltyTier.key == key)
        .first()
    ):
        i += 1
        key = f"{base}_{i}"

    min_purchase_count = int(getattr(payload, "min_purchase_count", 0) or 0)
    min_purchase_amount = int(getattr(payload, "min_purchase_amount", 0) or 0)
    min_status_points = int(getattr(payload, "min_status_points", 0) or 0)
    if get_qualification_mode(db, brand=active_brand) == QUALIFICATION_PURCHASES:
        min_status_points = unique_min_status_points(
            _used_min_status_points(db, active_brand),
            min_status_points,
        )
    _validate_tier_payload(
        db=db,
        brand=active_brand,
        tier_id=None,
        key=key,
        name=payload.name,
        rank=0,
        min_status_points=min_status_points,
        min_purchase_count=min_purchase_count,
        min_purchase_amount=min_purchase_amount,
        active=payload.active,
    )

    obj = LoyaltyTier(
        brand=active_brand,
        key=key,
        name=payload.name,
        min_status_points=min_status_points,
        min_purchase_count=min_purchase_count,
        min_purchase_amount=min_purchase_amount,
        rank=_initial_rank_for_new_tier(
            db=db,
            brand=active_brand,
            min_status_points=min_status_points,
        ),
        active=payload.active,
    )
    db.add(obj)
    db.flush()
    _align_inactive_mode_thresholds(db, active_brand)
    db.commit()
    db.refresh(obj)

    _enqueue_recompute_customers_job(db, active_brand)

    return obj


@router.get("/{tier_id}", response_model=LoyaltyTierOut)
def get_loyalty_tier(
    tier_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(LoyaltyTier).filter(LoyaltyTier.id == tier_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Tier not found")
    return obj


@router.patch("/{tier_id}", response_model=LoyaltyTierOut)
def update_loyalty_tier(
    tier_id: UUID,
    payload: LoyaltyTierUpdate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(LoyaltyTier).filter(LoyaltyTier.id == tier_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Tier not found")

    data = payload.model_dump(exclude_unset=True)
    if "brand" in data and data["brand"] is not None and data["brand"] != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    if "key" in data and data["key"] != obj.key:
        existing = (
            db.query(LoyaltyTier.id)
            .filter(LoyaltyTier.brand == obj.brand)
            .filter(LoyaltyTier.key == data["key"])
            .first()
        )
        if existing:
            raise HTTPException(status_code=400, detail="Tier key already exists for brand")

    final_key = data.get("key", obj.key)
    final_name = data.get("name", obj.name)
    final_min = data.get("min_status_points", obj.min_status_points)
    final_count = data.get("min_purchase_count", getattr(obj, "min_purchase_count", 0) or 0)
    final_amount = data.get("min_purchase_amount", getattr(obj, "min_purchase_amount", 0) or 0)
    final_active = data.get("active", obj.active)
    if get_qualification_mode(db, brand=active_brand) == QUALIFICATION_PURCHASES:
        final_min = unique_min_status_points(
            _used_min_status_points(db, active_brand, exclude_id=tier_id),
            int(final_min or 0),
        )
        data["min_status_points"] = final_min
    _validate_tier_payload(
        db=db,
        brand=active_brand,
        tier_id=tier_id,
        key=final_key,
        name=final_name,
        rank=0,
        min_status_points=final_min,
        min_purchase_count=final_count,
        min_purchase_amount=final_amount,
        active=final_active,
    )

    for k, v in data.items():
        if k == "brand":
            continue
        if k == "rank":
            continue
        setattr(obj, k, v)

    db.flush()
    _align_inactive_mode_thresholds(db, active_brand)
    db.commit()
    db.refresh(obj)

    _enqueue_recompute_customers_job(db, active_brand)

    return obj


@router.delete("/{tier_id}")
def delete_loyalty_tier(
    tier_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(LoyaltyTier).filter(LoyaltyTier.id == tier_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Tier not found")

    is_base = (
        int(obj.min_status_points) == 0
        and int(getattr(obj, "min_purchase_count", 0) or 0) == 0
        and int(getattr(obj, "min_purchase_amount", 0) or 0) == 0
    )
    if bool(obj.active) and is_base:
        remaining_active_zero = (
            db.query(func.count(LoyaltyTier.id))
            .filter(LoyaltyTier.brand == active_brand)
            .filter(LoyaltyTier.active.is_(True))
            .filter(LoyaltyTier.min_status_points == 0)
            .filter(LoyaltyTier.min_purchase_count == 0)
            .filter(LoyaltyTier.min_purchase_amount == 0)
            .filter(LoyaltyTier.id != obj.id)
            .scalar()
            or 0
        )
        if int(remaining_active_zero) <= 0:
            raise HTTPException(
                status_code=400,
                detail=(
                    "You cannot delete the last active base level "
                    "(min_status_points=0, min_purchase_count=0, min_purchase_amount=0)"
                ),
            )

    db.delete(obj)
    db.flush()
    _align_inactive_mode_thresholds(db, active_brand)
    db.commit()

    _enqueue_recompute_customers_job(db, active_brand)

    return {"deleted": True}


@router.post("/recompute-customers")
def recompute_customers_loyalty_status(
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    _enqueue_recompute_customers_job(db, active_brand)
    return {"enqueued": True, "brand": active_brand}


@router.post("/ensure-base-tier", response_model=LoyaltyTierOut)
def ensure_base_loyalty_tier(
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    existing = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == active_brand)
        .filter(LoyaltyTier.min_status_points == 0)
        .filter(LoyaltyTier.min_purchase_count == 0)
        .filter(LoyaltyTier.min_purchase_amount == 0)
        .first()
    )
    if not existing:
        existing = (
            db.query(LoyaltyTier)
            .filter(LoyaltyTier.brand == active_brand)
            .filter(LoyaltyTier.min_status_points == 0)
            .order_by(LoyaltyTier.rank.asc())
            .first()
        )
    if existing:
        if not bool(existing.active):
            _validate_tier_payload(
                db=db,
                brand=active_brand,
                tier_id=existing.id,
                key=existing.key,
                name=existing.name,
                rank=0,
                min_status_points=0,
                active=True,
            )
            existing.active = True
            db.flush()
            _align_inactive_mode_thresholds(db, active_brand)
            db.commit()
            db.refresh(existing)

            _enqueue_recompute_customers_job(db, active_brand)

        return existing

    name = "Base"
    key = _slug_key(name)
    base = key
    i = 1
    while (
        db.query(LoyaltyTier.id)
        .filter(LoyaltyTier.brand == active_brand)
        .filter(LoyaltyTier.key == key)
        .first()
    ):
        i += 1
        key = f"{base}_{i}"

    _validate_tier_payload(
        db=db,
        brand=active_brand,
        tier_id=None,
        key=key,
        name=name,
        rank=0,
        min_status_points=0,
        active=True,
    )

    obj = LoyaltyTier(
        brand=active_brand,
        key=key,
        name=name,
        min_status_points=0,
        min_purchase_count=0,
        min_purchase_amount=0,
        rank=0,
        active=True,
    )
    db.add(obj)
    db.flush()
    _align_inactive_mode_thresholds(db, active_brand)
    db.commit()
    db.refresh(obj)

    _enqueue_recompute_customers_job(db, active_brand)

    return obj
