from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, or_
from sqlalchemy.orm import Session
from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.customer import Customer
from app.models.customer_coupon import CustomerCoupon
from app.models.customer_reward import CustomerReward
from app.models.loyalty_tier import LoyaltyTier
from app.models.transaction import Transaction
from app.schemas.customer import (
    CustomerLoyaltyStatusOut,
    CustomerLoyaltyStatusUpdate,
    CustomerOut,
    CustomerUpsert,
)
from app.schemas.customer_coupon import CustomerCouponOut, CustomerCouponStatusUpdate
from app.schemas.customer_reward import CustomerRewardOut
from app.schemas.point_movement import PointMovementOut
from app.services.contact_service import get_or_create_customer
from app.services.customer_coupon_service import set_customer_coupon_status
from app.services.transaction_protection import transaction_deletion_meta
from app.services.customer_loyalty_service import set_customer_loyalty_tier
from app.services.customer_serialization import serialize_customer_out
from app.services.loyalty_status_service import update_customer_status
from app.services.wallet_service import get_status_points_balance


router = APIRouter(prefix="/customers", tags=["customers"])


@router.get("")
def list_customers(
    q: str | None = None,
    status: str | None = None,
    loyalty_status: str | None = None,
    limit: int = 100,
    offset: int = 0,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    limit = max(1, min(limit, 500))
    offset = max(0, offset)

    query = db.query(Customer).filter(Customer.brand == active_brand)

    if q:
        query = query.filter(Customer.profile_id.ilike(f"%{q}%"))

    if status:
        query = query.filter(Customer.status == status)

    if loyalty_status:
        query = query.filter(Customer.loyalty_status == loyalty_status)

    total = query.with_entities(func.count()).scalar() or 0
    items = (
        query.order_by(Customer.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )

    out_items = [serialize_customer_out(db, customer=c, brand=active_brand) for c in items]

    return {
        "brand": active_brand,
        "count": int(total),
        "items": out_items,
        "limit": limit,
        "offset": offset,
    }


@router.get("/{brand}/{profile_id}", response_model=CustomerOut)
def get_customer(
    brand: str,
    profile_id: str,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .first()
    )
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    return serialize_customer_out(db, customer=customer, brand=brand)


@router.post("/upsert", response_model=CustomerOut)
def upsert_customer(
    payload: CustomerUpsert,
    db: Session = Depends(get_db),
):
    props = payload.properties or {}
    brand = (payload.brand or props.get("brand") or "").strip() or None
    if not brand:
        raise HTTPException(status_code=400, detail="brand is required")
    profile_id = (payload.profileId or "").strip() or None
    if not profile_id:
        raise HTTPException(status_code=400, detail="profileId is required")

    if "gender" in props and props.get("gender") not in (None, "") and not isinstance(props.get("gender"), str):
        raise HTTPException(status_code=400, detail="properties.gender must be a string")
    gender = payload.gender or (props.get("gender") if isinstance(props.get("gender"), str) else None)
    birthdate = payload.birthdate

    if isinstance(birthdate, str):
        s = birthdate.strip()
        if s and not ((len(s) == 10 and s[4] == "-" and s[7] == "-") or (len(s) == 5 and s[2] == "-")):
            raise HTTPException(status_code=400, detail="birthdate must be in format YYYY-MM-DD or MM-DD")

    if not birthdate:
        bd = props.get("birthDate")
        if "birthDate" in props and bd not in (None, "") and not isinstance(bd, (int, float)):
            raise HTTPException(status_code=400, detail="properties.birthDate must be an epoch millisecond number")
        if isinstance(bd, (int, float)):
            birthdate = datetime.utcfromtimestamp(float(bd) / 1000.0).date()

    try:
        existed = bool(
            db.query(Customer.id)
            .filter(Customer.brand == brand)
            .filter(Customer.profile_id == profile_id)
            .first()
        )
        customer = get_or_create_customer(
            db,
            brand,
            profile_id,
            {"gender": gender, "birthdate": birthdate},
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not existed:
        from app.services.transaction_service import create_internal_transaction

        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        tx_id = f"customer_{brand}_{profile_id}_CUSTOMER_REGISTRATION_{ts}"
        tx_payload = {
            "reason": "CUSTOMER_CREATED",
            "brand": brand,
            "profileId": profile_id,
            "_ruleDepth": 0,
        }
        create_internal_transaction(
            db,
            brand=brand,
            profile_id=profile_id,
            transaction_type="CUSTOMER_REGISTRATION",
            transaction_id=tx_id,
            payload=tx_payload,
            depth=0,
            commit=False,
        )

    if customer.loyalty_status in (None, "UNCONFIGURED"):
        update_customer_status(
            db,
            customer,
            reason="AUTO_TIER_REFRESH",
            source_transaction_id=None,
            depth=0,
            refresh_window=True,
            emit_events=False,
        )
    db.commit()
    db.refresh(customer)

    return serialize_customer_out(db, customer=customer, brand=brand, include_points_balance=False)


@router.get("/{brand}/{profile_id}/point-movements", response_model=list[PointMovementOut])
def list_point_movements(
    brand: str,
    profile_id: str,
    active_brand: str = Depends(get_active_brand),
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    if brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .first()
    )
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    from app.models.point_movement import PointMovement

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
    active_brand: str = Depends(get_active_brand),
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    if brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
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


@router.get("/{brand}/{profile_id}/coupons", response_model=list[CustomerCouponOut])
def list_customer_coupons(
    brand: str,
    profile_id: str,
    active_brand: str = Depends(get_active_brand),
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    if brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .first()
    )
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    q = db.query(CustomerCoupon).filter(CustomerCoupon.customer_id == customer.id)
    if status:
        q = q.filter(CustomerCoupon.status == status)

    limit = max(1, min(limit, 500))
    offset = max(0, offset)

    return (
        q.order_by(CustomerCoupon.issued_at.desc(), CustomerCoupon.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )


@router.get("/{brand}/{profile_id}/coupons-with-rewards")
def list_customer_coupons_with_rewards(
    brand: str,
    profile_id: str,
    active_brand: str = Depends(get_active_brand),
    status: str | None = None,
    coupon_limit: int = 100,
    coupon_offset: int = 0,
    reward_status: str | None = None,
    db: Session = Depends(get_db),
):
    if brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .first()
    )
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    q = db.query(CustomerCoupon).filter(CustomerCoupon.customer_id == customer.id)
    if status:
        q = q.filter(CustomerCoupon.status == status)
    coupon_limit = max(1, min(coupon_limit, 500))
    coupon_offset = max(0, coupon_offset)
    coupons = (
        q.order_by(CustomerCoupon.issued_at.desc(), CustomerCoupon.created_at.desc())
        .offset(coupon_offset)
        .limit(coupon_limit)
        .all()
    )

    coupon_ids = [c.id for c in coupons if c and getattr(c, "id", None) is not None]
    rewards_by_coupon_id: dict[str, list[CustomerRewardOut]] = {}
    if coupon_ids:
        rq = db.query(CustomerReward).filter(CustomerReward.customer_id == customer.id).filter(
            CustomerReward.customer_coupon_id.in_(coupon_ids)
        )
        if reward_status:
            rq = rq.filter(CustomerReward.status == reward_status)
        for r in rq.order_by(CustomerReward.issued_at.desc()).all():
            key = str(r.customer_coupon_id) if r.customer_coupon_id is not None else None
            if not key:
                continue
            rewards_by_coupon_id.setdefault(key, []).append(CustomerRewardOut.model_validate(r))

    return {
        "brand": brand,
        "profileId": profile_id,
        "items": [
            {
                "coupon": CustomerCouponOut.model_validate(c),
                "rewards": rewards_by_coupon_id.get(str(c.id), []),
            }
            for c in coupons
        ],
        "limit": coupon_limit,
        "offset": coupon_offset,
    }


@router.patch("/{brand}/{profile_id}/coupons/{customer_coupon_id}/status", response_model=CustomerCouponOut)
def patch_customer_coupon_status(
    brand: str,
    profile_id: str,
    customer_coupon_id: str,
    payload: CustomerCouponStatusUpdate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")

    coupon = set_customer_coupon_status(
        db,
        brand=brand,
        profile_id=profile_id,
        customer_coupon_id=customer_coupon_id,
        status=payload.status,
    )
    db.commit()
    db.refresh(coupon)
    return coupon


@router.patch("/{brand}/{profile_id}/loyalty/status", response_model=CustomerLoyaltyStatusOut)
def patch_customer_loyalty_status(
    brand: str,
    profile_id: str,
    payload: CustomerLoyaltyStatusUpdate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")

    result = set_customer_loyalty_tier(
        db,
        brand=brand,
        profile_id=profile_id,
        tier_key=payload.tierKey,
        reason=payload.reason,
    )
    db.commit()
    db.refresh(result.customer)

    data = serialize_customer_out(db, customer=result.customer, brand=brand)
    data["loyaltyOverride"] = {
        "fromTier": result.from_tier_key,
        "toTier": result.to_tier_key,
        "fromPointsBalance": result.from_points_balance,
        "toPointsBalance": result.to_points_balance,
        "pointsDelta": result.points_delta,
        "auditTransactionId": str(result.transaction.id),
    }
    return data


@router.get("/{brand}/{profile_id}/loyalty")
def get_customer_loyalty(
    brand: str,
    profile_id: str,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
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
        .order_by(LoyaltyTier.min_status_points.asc(), LoyaltyTier.created_at.asc())
        .all()
    )

    current_key = customer.loyalty_status
    current_tier = next((t for t in tiers if t.key == current_key), None)
    current_min = int(current_tier.min_status_points) if current_tier else None

    next_tier = None
    if current_min is not None:
        next_tier = next((t for t in tiers if int(t.min_status_points) > current_min), None)
    elif tiers:
        sp = int(customer.status_points or 0)
        for t in tiers:
            if int(t.min_status_points) > sp:
                next_tier = t
                break

    sp = int(customer.status_points or 0)
    next_min = int(next_tier.min_status_points) if next_tier else None
    points_to_next = (max(0, next_min - sp) if next_min is not None else None)

    last_change = None
    if current_tier:
        last_tier_event = (
            db.query(Transaction.transaction_type)
            .filter(Transaction.brand == brand)
            .filter(Transaction.profile_id == profile_id)
            .filter(
                Transaction.transaction_type.in_(
                    [
                        "CUSTOMER_REGISTRATION",
                        "TIER_UPGRADED",
                        "TIER_DOWNGRADED",
                        "TIER_RENEWED",
                        "STATUS_RESET",
                        "ADMIN_SET_TIER",
                    ]
                )
            )
            .filter(
                or_(
                    Transaction.payload["toTier"].as_string() == current_tier.key,
                    Transaction.payload["toStatus"].as_string() == current_tier.key,
                )
            )
            .order_by(Transaction.created_at.desc())
            .first()
        )
        if last_tier_event:
            ttype = last_tier_event[0]
            if ttype == "TIER_UPGRADED":
                last_change = "upgrade"
            elif ttype == "TIER_DOWNGRADED":
                last_change = "downgrade"
            elif ttype == "TIER_RENEWED":
                last_change = "no_change"
            elif ttype == "ADMIN_SET_TIER":
                last_change = "admin_override"

    return {
        "brand": brand,
        "profileId": profile_id,
        "loyaltyStatus": customer.loyalty_status,
        "statusPoints": sp,
        "pointsBalance": get_status_points_balance(db, customer.id),
        "lastActivityAt": customer.last_activity_at,
        "lastChange": last_change,
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
    active_brand: str = Depends(get_active_brand),
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    if brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    customer = (
        db.query(Customer.id)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .first()
    )
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    limit = max(1, min(limit, 500))
    offset = max(0, offset)

    tier_event_types = [
        "CUSTOMER_REGISTRATION",
        "TIER_UPGRADED",
        "TIER_DOWNGRADED",
        "TIER_RENEWED",
        "STATUS_RESET",
        "ADMIN_SET_TIER",
    ]

    q = (
        db.query(Transaction)
        .filter(Transaction.brand == brand)
        .filter(Transaction.profile_id == profile_id)
        .filter(Transaction.transaction_type.in_(tier_event_types))
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
                "transactionType": tx.transaction_type,
                "transactionId": tx.transaction_id,
                "status": tx.status,
                "source": tx.source,
                "createdAt": tx.created_at,
                "payload": tx.payload,
                "canDelete": transaction_deletion_meta(tx)["can_delete"],
                "isSystemManaged": transaction_deletion_meta(tx)["is_system_managed"],
            }
            for tx in items
        ],
    }
