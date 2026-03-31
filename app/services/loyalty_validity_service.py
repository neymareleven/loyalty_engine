from __future__ import annotations

from datetime import date, datetime, timedelta

from sqlalchemy import and_
from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.loyalty_tier import LoyaltyTier
from app.models.point_movement import PointMovement
from app.services.loyalty_settings_service import get_loyalty_settings
from app.services.loyalty_status_service import update_customer_status
from app.services.wallet_service import get_points_balance


def initialize_validity_windows_for_existing_customers(db: Session, *, brand: str) -> dict[str, int]:
    now = datetime.utcnow()

    settings = get_loyalty_settings(db, brand=brand)
    points_days = getattr(settings, "points_validity_days", None) if settings else None
    status_days = getattr(settings, "loyalty_status_validity_days", None) if settings else None

    updated_points = 0
    updated_status = 0

    if points_days is not None:
        updated_points = (
            db.query(Customer)
            .filter(Customer.brand == brand)
            .filter(Customer.points_expires_at.is_(None))
            .filter(Customer.status_points > 0)
            .update({Customer.points_expires_at: now + timedelta(days=int(points_days))})
        )

    if status_days is not None:
        base_row = (
            db.query(LoyaltyTier.key)
            .filter(LoyaltyTier.brand == brand)
            .filter(LoyaltyTier.active.is_(True))
            .order_by(LoyaltyTier.min_status_points.asc(), LoyaltyTier.created_at.asc())
            .first()
        )
        base_key = base_row[0] if base_row else None

        # Non-base tiers: initialize assigned+expires.
        q = (
            db.query(Customer)
            .filter(Customer.brand == brand)
            .filter(Customer.loyalty_status.isnot(None))
            .filter(Customer.loyalty_status != "UNCONFIGURED")
            .filter(
                and_(
                    Customer.loyalty_status_assigned_at.is_(None),
                    Customer.loyalty_status_expires_at.is_(None),
                )
            )
        )
        if base_key:
            q = q.filter(Customer.loyalty_status != base_key)

        updated_status = q.update(
            {
                Customer.loyalty_status_assigned_at: now,
                Customer.loyalty_status_expires_at: now + timedelta(days=int(status_days)),
            }
        )

        # Base tier: initialize assigned only (no expiration).
        if base_key:
            db.query(Customer).filter(Customer.brand == brand).filter(Customer.loyalty_status == base_key).filter(
                and_(
                    Customer.loyalty_status_assigned_at.is_(None),
                    Customer.loyalty_status_expires_at.is_(None),
                )
            ).update(
                {
                    Customer.loyalty_status_assigned_at: now,
                    Customer.loyalty_status_expires_at: None,
                }
            )

    db.flush()
    return {"points": int(updated_points or 0), "loyalty_status": int(updated_status or 0)}


def expire_points(db: Session, *, brand: str) -> int:
    now = datetime.utcnow()

    settings = get_loyalty_settings(db, brand=brand)
    points_days = getattr(settings, "points_validity_days", None) if settings else None
    if points_days is None:
        return 0

    today = date.today()
    expired_customer_ids = (
        db.query(PointMovement.customer_id)
        .join(Customer, Customer.id == PointMovement.customer_id)
        .filter(Customer.brand == brand)
        .filter(PointMovement.expires_at.isnot(None))
        .filter(PointMovement.expires_at < today)
        .group_by(PointMovement.customer_id)
        .all()
    )
    customer_ids = [row[0] for row in expired_customer_ids if row and row[0] is not None]
    if not customer_ids:
        return 0

    customers = (
        db.query(Customer)
        .filter(Customer.id.in_(customer_ids))
        .with_for_update()
        .all()
    )

    updated = 0
    for c in customers:
        before = int(c.status_points or 0)
        balance = max(0, int(get_points_balance(db, c.id) or 0))
        if balance != before:
            c.status_points = balance
            c.status_points_reset_at = now

        update_customer_status(
            db,
            c,
            reason="POINTS_EXPIRED",
            source_transaction_id=None,
            depth=0,
            refresh_window=False,
            emit_events=True,
        )
        updated += 1

    db.flush()
    return updated


def expire_loyalty_status(db: Session, *, brand: str) -> int:
    now = datetime.utcnow()

    settings = get_loyalty_settings(db, brand=brand)
    status_days = getattr(settings, "loyalty_status_validity_days", None) if settings else None
    if status_days is None:
        return 0

    base_row = (
        db.query(LoyaltyTier.key)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .order_by(LoyaltyTier.min_status_points.asc(), LoyaltyTier.created_at.asc())
        .first()
    )
    base_key = base_row[0] if base_row else None

    q = (
        db.query(Customer)
        .filter(Customer.brand == brand)
        .filter(Customer.loyalty_status_expires_at.isnot(None))
        .filter(Customer.loyalty_status_expires_at < now)
    )
    if base_key:
        q = q.filter(Customer.loyalty_status != base_key)

    expired_customers = q.all()

    updated = 0
    for c in expired_customers:
        update_customer_status(
            db,
            c,
            reason="LOYALTY_STATUS_EXPIRED",
            source_transaction_id=None,
            depth=0,
            refresh_window=True,
            emit_events=True,
        )
        updated += 1

    db.flush()
    return updated
