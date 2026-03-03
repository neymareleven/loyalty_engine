from uuid import UUID
import os

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.customer import Customer
from app.models.loyalty_tier import LoyaltyTier
from app.schemas.loyalty_tier import LoyaltyTierCreate, LoyaltyTierOut, LoyaltyTierUpdate
from app.services.loyalty_status_service import compute_loyalty_status_from_tiers


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
            "min_status_points": {"widget": "number", "min": 0},
            "rank": {"widget": "hidden"},
            "active": {"widget": "switch"},
        },
        "examples": [
            {
                "name": "Silver",
                "min_status_points": 0,
                "rank": 0,
                "active": True,
            }
        ],
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
        new_status = compute_loyalty_status_from_tiers(db, brand, c.status_points)
        new_status = new_status if new_status else "UNCONFIGURED"
        if c.loyalty_status != new_status:
            c.loyalty_status = new_status
            updated += 1
    db.commit()
    return {"brand": brand, "customers": len(customers), "updated": updated}


def _recompute_tier_ranks(db: Session, brand: str) -> None:
    tiers = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == brand)
        .order_by(LoyaltyTier.min_status_points.asc(), LoyaltyTier.created_at.asc())
        .all()
    )
    for i, t in enumerate(tiers):
        t.rank = i
    db.flush()


def _validate_tier_payload(
    *,
    db: Session,
    brand: str,
    tier_id: UUID | None,
    key: str,
    name: str,
    rank: int,
    min_status_points: int,
):
    if not (name and str(name).strip()):
        raise HTTPException(status_code=400, detail="Level name is required")
    if min_status_points is None:
        raise HTTPException(status_code=400, detail="Minimum status points is required")

    try:
        min_i = int(min_status_points)
    except Exception:
        raise HTTPException(status_code=400, detail="Minimum status points must be a whole number")

    if min_i < 0:
        raise HTTPException(status_code=400, detail="Minimum status points must be 0 or more")

    dup_name_q = db.query(LoyaltyTier.id).filter(LoyaltyTier.brand == brand).filter(LoyaltyTier.name == name)
    if tier_id is not None:
        dup_name_q = dup_name_q.filter(LoyaltyTier.id != tier_id)
    if dup_name_q.first():
        raise HTTPException(status_code=400, detail="A level with this name already exists for this brand")

    dup_min_q = (
        db.query(LoyaltyTier.id)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.min_status_points == min_i)
    )
    if tier_id is not None:
        dup_min_q = dup_min_q.filter(LoyaltyTier.id != tier_id)
    if dup_min_q.first():
        raise HTTPException(
            status_code=400,
            detail="You've already used this minimum status points value for another level in this brand",
        )

    tiers = db.query(LoyaltyTier).filter(LoyaltyTier.brand == brand).all()
    simulated = []
    for t in tiers:
        if tier_id is not None and t.id == tier_id:
            simulated.append({"min": min_i, "key": key})
        else:
            simulated.append({"min": int(t.min_status_points), "key": t.key})
    if tier_id is None:
        simulated.append({"min": min_i, "key": key})

    simulated.sort(key=lambda x: x["min"])
    for prev, cur in zip(simulated, simulated[1:]):
        if cur["min"] <= prev["min"]:
            raise HTTPException(
                status_code=400,
                detail="Invalid levels configuration: minimum status points must be unique and strictly increasing",
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
    return q.order_by(LoyaltyTier.brand.asc(), LoyaltyTier.min_status_points.asc()).all()


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

    _validate_tier_payload(
        db=db,
        brand=active_brand,
        tier_id=None,
        key=key,
        name=payload.name,
        rank=0,
        min_status_points=payload.min_status_points,
    )

    obj = LoyaltyTier(
        brand=active_brand,
        key=key,
        name=payload.name,
        min_status_points=payload.min_status_points,
        rank=0,
        active=payload.active,
    )
    db.add(obj)
    db.flush()
    _recompute_tier_ranks(db, active_brand)
    db.commit()
    db.refresh(obj)

    if _env_bool("AUTO_RECOMPUTE_CUSTOMERS_ON_TIER_CHANGE", default=False):
        _recompute_customers(db, active_brand)

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
    _validate_tier_payload(
        db=db,
        brand=active_brand,
        tier_id=tier_id,
        key=final_key,
        name=final_name,
        rank=0,
        min_status_points=final_min,
    )

    for k, v in data.items():
        if k == "brand":
            continue
        if k == "rank":
            continue
        setattr(obj, k, v)

    db.flush()
    _recompute_tier_ranks(db, active_brand)
    db.commit()
    db.refresh(obj)

    if _env_bool("AUTO_RECOMPUTE_CUSTOMERS_ON_TIER_CHANGE", default=False):
        _recompute_customers(db, active_brand)

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

    db.delete(obj)
    db.commit()

    if _env_bool("AUTO_RECOMPUTE_CUSTOMERS_ON_TIER_CHANGE", default=False):
        _recompute_customers(db, active_brand)

    return {"deleted": True}


@router.post("/recompute-customers")
def recompute_customers_loyalty_status(
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    return _recompute_customers(db, active_brand)
