from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.customer import Customer
from app.models.loyalty_tier import LoyaltyTier
from app.schemas.loyalty_tier import LoyaltyTierCreate, LoyaltyTierOut, LoyaltyTierUpdate
from app.services.loyalty_status_service import compute_loyalty_status_from_tiers


router = APIRouter(prefix="/admin/loyalty-tiers", tags=["admin-loyalty-tiers"])


def _validate_tier_payload(
    *,
    db: Session,
    brand: str,
    tier_id: UUID | None,
    key: str,
    rank: int,
    min_status_points: int,
):
    if rank is None:
        raise HTTPException(status_code=400, detail="rank is required")
    if min_status_points is None:
        raise HTTPException(status_code=400, detail="min_status_points is required")

    try:
        rank_i = int(rank)
        min_i = int(min_status_points)
    except Exception:
        raise HTTPException(status_code=400, detail="rank and min_status_points must be integers")

    if rank_i < 0:
        raise HTTPException(status_code=400, detail="rank must be >= 0")
    if min_i < 0:
        raise HTTPException(status_code=400, detail="min_status_points must be >= 0")
    if rank_i == 0 and min_i != 0:
        raise HTTPException(status_code=400, detail="rank=0 tier must have min_status_points=0")

    dup_rank_q = db.query(LoyaltyTier.id).filter(LoyaltyTier.brand == brand).filter(LoyaltyTier.rank == rank_i)
    if tier_id is not None:
        dup_rank_q = dup_rank_q.filter(LoyaltyTier.id != tier_id)
    if dup_rank_q.first():
        raise HTTPException(status_code=400, detail="Tier rank already exists for brand")

    dup_min_q = (
        db.query(LoyaltyTier.id)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.min_status_points == min_i)
    )
    if tier_id is not None:
        dup_min_q = dup_min_q.filter(LoyaltyTier.id != tier_id)
    if dup_min_q.first():
        raise HTTPException(status_code=400, detail="Tier min_status_points already exists for brand")

    tiers = db.query(LoyaltyTier).filter(LoyaltyTier.brand == brand).all()
    simulated = []
    for t in tiers:
        if tier_id is not None and t.id == tier_id:
            simulated.append({"rank": rank_i, "min": min_i, "key": key})
        else:
            simulated.append({"rank": int(t.rank), "min": int(t.min_status_points), "key": t.key})
    if tier_id is None:
        simulated.append({"rank": rank_i, "min": min_i, "key": key})

    simulated.sort(key=lambda x: x["rank"])
    for prev, cur in zip(simulated, simulated[1:]):
        if cur["min"] <= prev["min"]:
            raise HTTPException(
                status_code=400,
                detail="Invalid tiers configuration: min_status_points must strictly increase as rank increases",
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
    return q.order_by(LoyaltyTier.brand.asc(), LoyaltyTier.rank.asc()).all()


@router.post("", response_model=LoyaltyTierOut)
def create_loyalty_tier(
    payload: LoyaltyTierCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if payload.brand is not None and payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")
    existing = (
        db.query(LoyaltyTier.id)
        .filter(LoyaltyTier.brand == active_brand)
        .filter(LoyaltyTier.key == payload.key)
        .first()
    )
    if existing:
        raise HTTPException(status_code=400, detail="Tier key already exists for brand")

    _validate_tier_payload(
        db=db,
        brand=active_brand,
        tier_id=None,
        key=payload.key,
        rank=payload.rank,
        min_status_points=payload.min_status_points,
    )

    obj = LoyaltyTier(
        brand=active_brand,
        key=payload.key,
        name=payload.name,
        min_status_points=payload.min_status_points,
        rank=payload.rank,
        active=payload.active,
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
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
    final_rank = data.get("rank", obj.rank)
    final_min = data.get("min_status_points", obj.min_status_points)
    _validate_tier_payload(
        db=db,
        brand=active_brand,
        tier_id=tier_id,
        key=final_key,
        rank=final_rank,
        min_status_points=final_min,
    )

    for k, v in data.items():
        if k == "brand":
            continue
        setattr(obj, k, v)

    db.commit()
    db.refresh(obj)
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
    return {"deleted": True}


@router.post("/recompute-customers")
def recompute_customers_loyalty_status(
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    customers = db.query(Customer).filter(Customer.brand == active_brand).all()
    updated = 0
    for c in customers:
        new_status = compute_loyalty_status_from_tiers(db, active_brand, c.status_points)
        new_status = new_status if new_status else "UNCONFIGURED"
        if c.loyalty_status != new_status:
            c.loyalty_status = new_status
            updated += 1
    db.commit()
    return {"brand": active_brand, "customers": len(customers), "updated": updated}
