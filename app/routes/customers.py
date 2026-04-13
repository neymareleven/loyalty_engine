from datetime import date, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session
from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.customer import Customer
from app.models.customer_coupon import CustomerCoupon
from app.models.customer_reward import CustomerReward
from app.models.point_movement import PointMovement
from app.models.transaction import Transaction
from app.models.loyalty_tier import LoyaltyTier
from app.schemas.customer import CustomerOut, CustomerUpsert
from app.schemas.customer import CustomerSetTierMinOnly
from app.schemas.customer_coupon import CustomerCouponOut
from app.schemas.customer_reward import CustomerRewardOut
from app.schemas.point_movement import PointMovementOut
from app.services.contact_service import get_or_create_customer
from app.services.loyalty_status_service import update_customer_status
from app.services.wallet_service import get_status_points_balance
from app.services.loyalty_settings_service import get_loyalty_settings


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

    tiers = (
        db.query(LoyaltyTier.key, LoyaltyTier.name)
        .filter(LoyaltyTier.brand == active_brand)
        .all()
    )
    tier_name_by_key = {row[0]: row[1] for row in tiers}

    out_items = []
    for c in items:
        data = CustomerOut.model_validate(c).model_dump()
        if getattr(c, "birth_year", None) and getattr(c, "birth_month", None) and getattr(c, "birth_day", None):
            data["birthdate"] = f"{int(c.birth_year):04d}-{int(c.birth_month):02d}-{int(c.birth_day):02d}"
        elif getattr(c, "birth_month", None) and getattr(c, "birth_day", None):
            data["birthdate"] = f"{int(c.birth_month):02d}-{int(c.birth_day):02d}"
        else:
            data["birthdate"] = None
        data["loyalty_status_name"] = tier_name_by_key.get(c.loyalty_status)
        data["points_balance"] = get_status_points_balance(db, c.id)
        out_items.append(data)

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

    tier_name = (
        db.query(LoyaltyTier.name)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.key == customer.loyalty_status)
        .scalar()
        if customer.loyalty_status
        else None
    )
    data = CustomerOut.model_validate(customer).model_dump()
    if getattr(customer, "birth_year", None) and getattr(customer, "birth_month", None) and getattr(customer, "birth_day", None):
        data["birthdate"] = f"{int(customer.birth_year):04d}-{int(customer.birth_month):02d}-{int(customer.birth_day):02d}"
    elif getattr(customer, "birth_month", None) and getattr(customer, "birth_day", None):
        data["birthdate"] = f"{int(customer.birth_month):02d}-{int(customer.birth_day):02d}"
    else:
        data["birthdate"] = None
    data["loyalty_status_name"] = tier_name
    data["points_balance"] = get_status_points_balance(db, customer.id)
    return data


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

    tier_name = (
        db.query(LoyaltyTier.name)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.key == customer.loyalty_status)
        .scalar()
        if customer.loyalty_status
        else None
    )
    data = CustomerOut.model_validate(customer).model_dump()
    if getattr(customer, "birth_year", None) and getattr(customer, "birth_month", None) and getattr(customer, "birth_day", None):
        data["birthdate"] = f"{int(customer.birth_year):04d}-{int(customer.birth_month):02d}-{int(customer.birth_day):02d}"
    elif getattr(customer, "birth_month", None) and getattr(customer, "birth_day", None):
        data["birthdate"] = f"{int(customer.birth_month):02d}-{int(customer.birth_day):02d}"
    else:
        data["birthdate"] = None
    data["loyalty_status_name"] = tier_name
    return data


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

    coupon_limit = max(1, min(coupon_limit, 500))
    coupon_offset = max(0, coupon_offset)

    cq = db.query(CustomerCoupon).filter(CustomerCoupon.customer_id == customer.id)
    if status:
        cq = cq.filter(CustomerCoupon.status == status)

    coupons = (
        cq.order_by(CustomerCoupon.issued_at.desc(), CustomerCoupon.created_at.desc())
        .offset(coupon_offset)
        .limit(coupon_limit)
        .all()
    )

    coupon_ids = [c.id for c in coupons if c and getattr(c, "id", None) is not None]
    rewards_by_coupon_id: dict[str, list[CustomerRewardOut]] = {}
    if coupon_ids:
        rq = db.query(CustomerReward).filter(CustomerReward.customer_id == customer.id).filter(CustomerReward.customer_coupon_id.in_(coupon_ids))
        if reward_status:
            rq = rq.filter(CustomerReward.status == reward_status)
        rewards = rq.order_by(CustomerReward.issued_at.desc()).all()
        for r in rewards:
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


@router.post("/{brand}/{profile_id}/coupons/{customer_coupon_id}/use", response_model=CustomerCouponOut)
def use_customer_coupon(
    brand: str,
    profile_id: str,
    customer_coupon_id: str,
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

    now = datetime.utcnow()

    coupon = (
        db.query(CustomerCoupon)
        .filter(CustomerCoupon.id == customer_coupon_id)
        .filter(CustomerCoupon.customer_id == customer.id)
        .with_for_update()
        .first()
    )
    if not coupon:
        raise HTTPException(status_code=404, detail="Customer coupon not found")

    if coupon.expires_at is not None and coupon.expires_at < now:
        coupon.status = "EXPIRED"
        db.commit()
        raise HTTPException(status_code=400, detail="Coupon not usable")

    if coupon.status != "ISSUED":
        raise HTTPException(status_code=400, detail="Coupon not usable")

    tx = Transaction(
        transaction_id=f"admin_use_coupon_{brand}_{profile_id}_{customer_coupon_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}",
        brand=brand,
        profile_id=profile_id,
        transaction_type="ADMIN_USE_COUPON",
        source="ADMIN_UI",
        payload={
            "couponId": str(coupon.id),
            "fromStatus": coupon.status,
            "toStatus": "USED",
        },
        status="PROCESSED",
        processed_at=datetime.utcnow(),
    )
    db.add(tx)
    db.flush()

    coupon.status = "USED"
    coupon.used_at = now
    coupon.source_transaction_id = tx.id

    rewards = (
        db.query(CustomerReward)
        .filter(CustomerReward.customer_id == customer.id)
        .filter(CustomerReward.customer_coupon_id == coupon.id)
        .filter(CustomerReward.status == "ISSUED")
        .all()
    )
    for cr in rewards:
        if cr.expires_at is not None and cr.expires_at < now:
            cr.status = "EXPIRED"
        else:
            cr.status = "USED"
            cr.used_at = now
        cr.source_transaction_id = tx.id

    db.commit()
    db.refresh(coupon)
    return coupon


@router.post("/{brand}/{profile_id}/coupons/{customer_coupon_id}/reopen", response_model=CustomerCouponOut)
def reopen_customer_coupon(
    brand: str,
    profile_id: str,
    customer_coupon_id: str,
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

    now = datetime.utcnow()

    coupon = (
        db.query(CustomerCoupon)
        .filter(CustomerCoupon.id == customer_coupon_id)
        .filter(CustomerCoupon.customer_id == customer.id)
        .with_for_update()
        .first()
    )
    if not coupon:
        raise HTTPException(status_code=404, detail="Customer coupon not found")

    if coupon.expires_at is not None and coupon.expires_at < now:
        coupon.status = "EXPIRED"
        db.commit()
        raise HTTPException(status_code=400, detail="Coupon not usable")

    if coupon.status != "USED":
        raise HTTPException(status_code=400, detail="Coupon not usable")

    tx = Transaction(
        transaction_id=f"admin_reopen_coupon_{brand}_{profile_id}_{customer_coupon_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}",
        brand=brand,
        profile_id=profile_id,
        transaction_type="ADMIN_REOPEN_COUPON",
        source="ADMIN_UI",
        payload={
            "couponId": str(coupon.id),
            "fromStatus": coupon.status,
            "toStatus": "ISSUED",
        },
        status="PROCESSED",
        processed_at=datetime.utcnow(),
    )
    db.add(tx)
    db.flush()

    coupon.status = "ISSUED"
    coupon.used_at = None
    coupon.source_transaction_id = tx.id

    rewards = (
        db.query(CustomerReward)
        .filter(CustomerReward.customer_id == customer.id)
        .filter(CustomerReward.customer_coupon_id == coupon.id)
        .filter(CustomerReward.status == "USED")
        .all()
    )
    for cr in rewards:
        if cr.expires_at is not None and cr.expires_at < now:
            cr.status = "EXPIRED"
        else:
            cr.status = "ISSUED"
            cr.used_at = None
        cr.source_transaction_id = tx.id

    db.commit()
    db.refresh(coupon)
    return coupon


@router.post("/{brand}/{profile_id}/loyalty/set-tier", response_model=CustomerOut)
def set_customer_tier_min_only(
    brand: str,
    profile_id: str,
    payload: CustomerSetTierMinOnly,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")

    tier_key = (payload.tierKey or "").strip()
    if not tier_key:
        raise HTTPException(status_code=400, detail="tierKey is required")

    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .with_for_update()
        .first()
    )
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    tier = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .filter(LoyaltyTier.key == tier_key)
        .first()
    )
    if not tier:
        raise HTTPException(status_code=400, detail="Target loyalty tier not found")

    target_points = int(tier.min_status_points or 0)

    current_balance = int(get_status_points_balance(db, customer.id) or 0)
    delta = int(target_points) - int(current_balance)

    # Create an audit transaction WITHOUT running rules.
    tx = Transaction(
        transaction_id=f"admin_set_tier_{brand}_{profile_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}",
        brand=brand,
        profile_id=profile_id,
        transaction_type="ADMIN_SET_TIER",
        source="ADMIN_UI",
        payload={
            "tierKey": tier_key,
            "fromStatus": customer.loyalty_status,
            "toStatus": tier_key,
            "fromPointsBalance": int(current_balance),
            "toPointsBalance": int(target_points),
            "delta": int(delta),
        },
        status="PROCESSED",
        processed_at=datetime.utcnow(),
    )
    db.add(tx)
    db.flush()

    if delta != 0:
        pm_type = "EARN" if delta > 0 else "DEDUCT"
        expires_at = None
        if delta > 0:
            settings = get_loyalty_settings(db, brand=customer.brand)
            points_days = getattr(settings, "points_validity_days", None) if settings else None
            expires_at = (date.today() + timedelta(days=int(points_days))) if points_days is not None else None
        db.add(
            PointMovement(
                customer_id=customer.id,
                points=int(delta),
                type=pm_type,
                source_transaction_id=tx.id,
                expires_at=expires_at,
            )
        )
        db.flush()

    # Keep cache consistent with ledger.
    customer.status_points = int(get_status_points_balance(db, customer.id) or 0)
    customer.status_points_reset_at = datetime.utcnow()

    update_customer_status(
        db,
        customer,
        reason="ADMIN_OVERRIDE",
        source_transaction_id=tx.id,
        depth=0,
        refresh_window=True,
        emit_events=False,
        allow_downgrade_before_expiry=True,
    )

    db.commit()
    db.refresh(customer)

    tier_name = (
        db.query(LoyaltyTier.name)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.key == customer.loyalty_status)
        .scalar()
        if customer.loyalty_status
        else None
    )
    data = CustomerOut.model_validate(customer).model_dump()
    if getattr(customer, "birth_year", None) and getattr(customer, "birth_month", None) and getattr(customer, "birth_day", None):
        data["birthdate"] = f"{int(customer.birth_year):04d}-{int(customer.birth_month):02d}-{int(customer.birth_day):02d}"
    elif getattr(customer, "birth_month", None) and getattr(customer, "birth_day", None):
        data["birthdate"] = f"{int(customer.birth_month):02d}-{int(customer.birth_day):02d}"
    else:
        data["birthdate"] = None
    data["loyalty_status_name"] = tier_name
    data["points_balance"] = get_status_points_balance(db, customer.id)
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
        # If current tier isn't found, best-effort: choose the first tier above current status_points.
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
            .filter(Transaction.transaction_type.in_(["TIER_UPGRADED", "TIER_DOWNGRADED", "TIER_RENEWED"]))
            .filter(Transaction.payload["toTier"].astext == current_tier.key)
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

    return {
        "brand": brand,
        "profileId": profile_id,
        "loyaltyStatus": customer.loyalty_status,
        "statusPoints": sp,
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

    tier_event_types = ["TIER_UPGRADED", "TIER_DOWNGRADED", "TIER_RENEWED", "STATUS_RESET"]

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
            }
            for tx in items
        ],
    }


