from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.models.customer import Customer
from app.models.customer_reward import CustomerReward
from app.models.point_movement import PointMovement
from app.schemas.customer import CustomerOut, CustomerUpsert
from app.schemas.customer_reward import CustomerRewardOut
from app.schemas.point_movement import PointMovementOut
from app.services.contact_service import get_or_create_customer
from app.services.reward_service import use_reward
from app.services.wallet_service import get_points_balance
from app.models.loyalty_tier import LoyaltyTier
from app.models.transaction import Transaction
from app.models.customer_tag import CustomerTag


router = APIRouter(prefix="/customers", tags=["customers"])


@router.get("/{brand}/{profile_id}", response_model=CustomerOut)
def get_customer(brand: str, profile_id: str, db: Session = Depends(get_db)):
    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .first()
    )
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer


@router.post("/upsert", response_model=CustomerOut)
def upsert_customer(payload: CustomerUpsert, db: Session = Depends(get_db)):
    customer = get_or_create_customer(
        db,
        payload.brand,
        payload.profileId,
        {"gender": payload.gender, "birthdate": payload.birthdate},
    )
    db.commit()
    db.refresh(customer)
    return customer


@router.get("/{brand}/{profile_id}/wallet")
def get_customer_wallet(brand: str, profile_id: str, db: Session = Depends(get_db)):
    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .first()
    )
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    balance = get_points_balance(db, customer.id)
    return {
        "brand": brand,
        "profileId": profile_id,
        "loyaltyStatus": customer.loyalty_status,
        "lifetimePoints": customer.lifetime_points,
        "pointsBalance": balance,
    }


@router.get("/{brand}/{profile_id}/point-movements", response_model=list[PointMovementOut])
def list_point_movements(
    brand: str,
    profile_id: str,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .first()
    )
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    limit = max(1, min(limit, 500))
    offset = max(0, offset)

    return (
        db.query(PointMovement)
        .filter(PointMovement.customer_id == customer.id)
        .order_by(PointMovement.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )


@router.get("/{brand}/{profile_id}/rewards", response_model=list[CustomerRewardOut])
def list_customer_rewards(
    brand: str,
    profile_id: str,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .first()
    )
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    q = db.query(CustomerReward).filter(CustomerReward.customer_id == customer.id)
    if status:
        q = q.filter(CustomerReward.status == status)

    limit = max(1, min(limit, 500))
    offset = max(0, offset)

    return (
        q.order_by(CustomerReward.issued_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )


@router.post("/{brand}/{profile_id}/rewards/{customer_reward_id}/use", response_model=CustomerRewardOut)
def use_customer_reward(
    brand: str,
    profile_id: str,
    customer_reward_id: str,
    db: Session = Depends(get_db),
):
    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .first()
    )
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    cr = (
        db.query(CustomerReward)
        .filter(CustomerReward.id == customer_reward_id)
        .filter(CustomerReward.customer_id == customer.id)
        .first()
    )
    if not cr:
        raise HTTPException(status_code=404, detail="Customer reward not found")

    use_reward(db, cr)
    db.commit()
    db.refresh(cr)
    return cr


@router.get("/{brand}/{profile_id}/loyalty")
def get_customer_loyalty(brand: str, profile_id: str, db: Session = Depends(get_db)):
    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .first()
    )
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    tiers = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .order_by(LoyaltyTier.rank.asc(), LoyaltyTier.min_status_points.asc())
        .all()
    )

    current_key = customer.loyalty_status
    current_tier = next((t for t in tiers if t.key == current_key), None)
    current_rank = int(current_tier.rank) if current_tier else None

    next_tier = None
    if current_rank is not None:
        next_tier = next((t for t in tiers if int(t.rank) == current_rank + 1), None)
    elif tiers:
        # If current tier isn't found, best-effort: choose the first tier above current status_points.
        sp = int(customer.status_points or 0)
        for t in tiers:
            if int(t.min_status_points) > sp:
                next_tier = t
                break

    sp = int(customer.status_points or 0)
    next_min = int(next_tier.min_status_points) if next_tier else None
    points_to_next = (max(0, next_min - sp) if next_min is not None else None)

    return {
        "brand": brand,
        "profileId": profile_id,
        "loyaltyStatus": customer.loyalty_status,
        "statusPoints": sp,
        "lifetimePoints": int(customer.lifetime_points or 0),
        "lastActivityAt": customer.last_activity_at,
        "currentTier": (
            {
                "key": current_tier.key,
                "name": current_tier.name,
                "rank": int(current_tier.rank),
                "minStatusPoints": int(current_tier.min_status_points),
            }
            if current_tier
            else None
        ),
        "nextTier": (
            {
                "key": next_tier.key,
                "name": next_tier.name,
                "rank": int(next_tier.rank),
                "minStatusPoints": int(next_tier.min_status_points),
            }
            if next_tier
            else None
        ),
        "pointsToNextTier": points_to_next,
        "tiers": [
            {
                "key": t.key,
                "name": t.name,
                "rank": int(t.rank),
                "minStatusPoints": int(t.min_status_points),
            }
            for t in tiers
        ],
    }


@router.get("/{brand}/{profile_id}/loyalty/history")
def get_customer_loyalty_history(
    brand: str,
    profile_id: str,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    customer = (
        db.query(Customer.id)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .first()
    )
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    limit = max(1, min(limit, 500))
    offset = max(0, offset)

    tier_event_types = ["TIER_UPGRADED", "TIER_DOWNGRADED", "STATUS_RESET"]

    q = (
        db.query(Transaction)
        .filter(Transaction.brand == brand)
        .filter(Transaction.profile_id == profile_id)
        .filter(Transaction.event_type.in_(tier_event_types))
    )

    total = q.count()
    items = (
        q.order_by(Transaction.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )

    return {
        "brand": brand,
        "profileId": profile_id,
        "count": total,
        "items": [
            {
                "id": str(tx.id),
                "eventType": tx.event_type,
                "eventId": tx.event_id,
                "status": tx.status,
                "source": tx.source,
                "createdAt": tx.created_at,
                "payload": tx.payload,
            }
            for tx in items
        ],
    }


@router.get("/{brand}/{profile_id}/tags")
def list_customer_tags(
    brand: str,
    profile_id: str,
    db: Session = Depends(get_db),
):
    customer = (
        db.query(Customer.id)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .first()
    )
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    tags = (
        db.query(CustomerTag.tag)
        .filter(CustomerTag.customer_id == customer.id)
        .order_by(CustomerTag.tag.asc())
        .all()
    )
    return {
        "brand": brand,
        "profileId": profile_id,
        "tags": [t[0] for t in tags],
    }
