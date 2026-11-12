from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.loyalty_tier import LoyaltyTier
from app.models.point_movement import PointMovement
from app.models.transaction import Transaction
from app.services.contact_service import get_customer
from app.services.loyalty_settings_service import get_loyalty_settings
from app.services.loyalty_status_service import update_customer_status
from app.services.purchase_service import (
    apply_purchase_delta,
    get_purchase_balances,
    get_qualification_mode,
)
from app.services.wallet_service import get_status_points_balance


@dataclass
class LoyaltyTierOverrideResult:
    customer: Customer
    transaction: Transaction
    from_tier_key: str | None
    to_tier_key: str
    from_points_balance: int
    to_points_balance: int
    points_delta: int
    from_purchase_count: int = 0
    to_purchase_count: int = 0
    from_purchase_amount: int = 0
    to_purchase_amount: int = 0


def _utcnow() -> datetime:
    return datetime.utcnow()


def set_customer_loyalty_tier(
    db: Session,
    *,
    brand: str,
    profile_id: str,
    tier_key: str,
    reason: str | None = None,
) -> LoyaltyTierOverrideResult:
    """Admin override: align counters to the target tier minimum and set loyalty_status."""
    tier_key = (tier_key or "").strip()
    if not tier_key:
        raise HTTPException(status_code=400, detail="tierKey is required")

    customer = get_customer(db, brand, profile_id)
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    customer = (
        db.query(Customer)
        .filter(Customer.id == customer.id)
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

    mode = get_qualification_mode(db, brand=brand)
    from_tier_key = customer.loyalty_status
    target_points = int(tier.min_status_points or 0)
    current_balance = int(get_status_points_balance(db, customer.id) or 0)
    delta = int(target_points) - int(current_balance)

    from_count, from_amount = get_purchase_balances(db, customer.id)
    target_count = int(getattr(tier, "min_purchase_count", 0) or 0)
    target_amount = int(getattr(tier, "min_purchase_amount", 0) or 0)
    count_delta = int(target_count) - int(from_count)
    amount_delta = int(target_amount) - int(from_amount)

    now = _utcnow()
    tx = Transaction(
        transaction_id=f"admin_set_tier_{brand}_{profile_id}_{now.strftime('%Y%m%d%H%M%S%f')}",
        brand=brand,
        profile_id=profile_id,
        transaction_type="ADMIN_SET_TIER",
        source="ADMIN_UI",
        payload={
            "tierKey": tier_key,
            "fromStatus": from_tier_key,
            "toStatus": tier_key,
            "qualificationMode": mode,
            "fromPointsBalance": int(current_balance),
            "toPointsBalance": int(target_points),
            "delta": int(delta),
            "fromPurchaseCount": int(from_count),
            "toPurchaseCount": int(target_count),
            "fromPurchaseAmount": int(from_amount),
            "toPurchaseAmount": int(target_amount),
            "reason": reason or "ADMIN_OVERRIDE",
        },
        status="PROCESSED",
        processed_at=now,
    )
    db.add(tx)
    db.flush()

    # Top up every counter to the assigned palier's minima so the fiche matches the status.
    # Never deduct real sales/points history.
    points_topup = max(0, int(delta))
    if points_topup:
        settings = get_loyalty_settings(db, brand=customer.brand)
        points_days = getattr(settings, "points_validity_days", None) if settings else None
        expires_at = (date.today() + timedelta(days=int(points_days))) if points_days is not None else None
        db.add(
            PointMovement(
                customer_id=customer.id,
                points=int(points_topup),
                type="EARN",
                source_transaction_id=tx.id,
                expires_at=expires_at,
            )
        )
        db.flush()
        customer.status_points = int(get_status_points_balance(db, customer.id) or 0)

    count_topup = max(0, int(count_delta))
    amount_topup = max(0, int(amount_delta))
    if count_topup or amount_topup:
        apply_purchase_delta(
            db,
            customer=customer,
            quantity_delta=count_topup,
            amount_delta=amount_topup,
            source_transaction_id=tx.id,
            movement_type="EARN",
        )

    from app.models.customer_loyalty_guarantee import GUARANTEE_ADMIN
    from app.services.loyalty_guarantee_service import grant_loyalty_guarantee

    settings = get_loyalty_settings(db, brand=customer.brand)
    status_days = getattr(settings, "loyalty_status_validity_days", None) if settings else None
    expires_at = (now + timedelta(days=int(status_days))) if status_days is not None else None
    grant_loyalty_guarantee(
        db,
        customer=customer,
        source=GUARANTEE_ADMIN,
        floor_tier_key=tier_key,
        expires_at=expires_at,
    )

    update_customer_status(
        db,
        customer,
        reason="ADMIN_SET_TIER",
        source_transaction_id=tx.id,
        depth=0,
        refresh_window=True,
        emit_events=True,
        allow_downgrade_before_expiry=True,
        min_tier_key=tier_key,
    )

    db.flush()

    from app.services.unomi_profile_service import sync_customer_profile_to_unomi

    sync_customer_profile_to_unomi(
        db,
        customer=customer,
        reason="admin_tier_override",
        transport_override="profiles",
    )

    to_count, to_amount = get_purchase_balances(db, customer.id)
    return LoyaltyTierOverrideResult(
        customer=customer,
        transaction=tx,
        from_tier_key=from_tier_key,
        to_tier_key=tier_key,
        from_points_balance=int(current_balance),
        to_points_balance=int(get_status_points_balance(db, customer.id) or 0),
        points_delta=int(delta),
        from_purchase_count=int(from_count),
        to_purchase_count=int(to_count),
        from_purchase_amount=int(from_amount),
        to_purchase_amount=int(to_amount),
    )
