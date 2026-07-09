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
    """Admin override: align status points to the target tier minimum and set loyalty_status."""
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

    from_tier_key = customer.loyalty_status
    target_points = int(tier.min_status_points or 0)
    current_balance = int(get_status_points_balance(db, customer.id) or 0)
    delta = int(target_points) - int(current_balance)

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
            "fromPointsBalance": int(current_balance),
            "toPointsBalance": int(target_points),
            "delta": int(delta),
            "reason": reason or "ADMIN_OVERRIDE",
        },
        status="PROCESSED",
        processed_at=now,
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

    customer.status_points = int(get_status_points_balance(db, customer.id) or 0)

    update_customer_status(
        db,
        customer,
        reason=reason or "ADMIN_OVERRIDE",
        source_transaction_id=tx.id,
        depth=0,
        refresh_window=True,
        emit_events=False,
        allow_downgrade_before_expiry=True,
    )

    db.flush()

    from app.services.unomi_profile_service import sync_customer_profile_to_unomi

    sync_customer_profile_to_unomi(
        db,
        customer=customer,
        reason="admin_tier_override",
        transport_override="profiles",
    )

    return LoyaltyTierOverrideResult(
        customer=customer,
        transaction=tx,
        from_tier_key=from_tier_key,
        to_tier_key=tier_key,
        from_points_balance=int(current_balance),
        to_points_balance=int(target_points),
        points_delta=int(delta),
    )
