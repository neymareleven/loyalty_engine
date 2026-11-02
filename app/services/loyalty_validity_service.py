from __future__ import annotations

from datetime import date, datetime, timedelta

from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.loyalty_tier import LoyaltyTier
from app.models.point_movement import PointMovement
from app.models.purchase_movement import PurchaseMovement
from app.services.loyalty_settings_service import get_loyalty_settings
from app.services.loyalty_status_service import update_customer_status
from app.services.purchase_service import get_purchase_balances, sync_customer_purchase_counters
from app.services.wallet_service import get_status_points_balance, sync_customer_points_expires_at


def initialize_validity_windows_for_existing_customers(db: Session, *, brand: str) -> dict[str, int]:
    now = datetime.utcnow()

    settings = get_loyalty_settings(db, brand=brand)
    points_days = getattr(settings, "points_validity_days", None) if settings else None
    status_days = getattr(settings, "loyalty_status_validity_days", None) if settings else None
    purchase_days = getattr(settings, "purchase_validity_days", None) if settings else None

    updated_points = 0
    updated_status = 0
    updated_purchases = 0

    if points_days is not None:
        updated_points = (
            db.query(Customer)
            .filter(Customer.brand == brand)
            .filter(Customer.points_expires_at.is_(None))
            .filter(Customer.status_points > 0)
            .update({Customer.points_expires_at: now + timedelta(days=int(points_days))})
        )

    if purchase_days is not None:
        updated_purchases = (
            db.query(Customer)
            .filter(Customer.brand == brand)
            .filter(Customer.purchases_expires_at.is_(None))
            .filter((Customer.purchase_count > 0) | (Customer.purchase_amount_total > 0))
            .update({Customer.purchases_expires_at: now + timedelta(days=int(purchase_days))})
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

            # Safety backfill: legacy base-tier rows might have expires_at set.
            # Base tier must never expire.
            db.query(Customer).filter(Customer.brand == brand).filter(Customer.loyalty_status == base_key).filter(
                Customer.loyalty_status_expires_at.isnot(None)
            ).update(
                {
                    Customer.loyalty_status_expires_at: None,
                    Customer.loyalty_status_assigned_at: func.coalesce(Customer.loyalty_status_assigned_at, now),
                }
            )

    db.flush()
    return {
        "points": int(updated_points or 0),
        "loyalty_status": int(updated_status or 0),
        "purchases": int(updated_purchases or 0),
    }


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
        balance = max(0, int(get_status_points_balance(db, c.id) or 0))
        expired_points = max(0, before - balance)
        if expired_points > 0:
            db.add(
                PointMovement(
                    customer_id=c.id,
                    points=-expired_points,
                    type="EXPIRE",
                    source_transaction_id=None,
                    expires_at=None,
                )
            )
        if balance != before:
            c.status_points = balance
            c.status_points_reset_at = now

        sync_customer_points_expires_at(db, c)

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


def expire_purchases(db: Session, *, brand: str) -> int:
    """Expire purchase-count / purchase-amount lots past expires_at (same pattern as points)."""
    settings = get_loyalty_settings(db, brand=brand)
    purchase_days = getattr(settings, "purchase_validity_days", None) if settings else None
    if purchase_days is None:
        return 0

    today = date.today()
    expired_customer_ids = (
        db.query(PurchaseMovement.customer_id)
        .join(Customer, Customer.id == PurchaseMovement.customer_id)
        .filter(Customer.brand == brand)
        .filter(PurchaseMovement.expires_at.isnot(None))
        .filter(PurchaseMovement.expires_at < today)
        .group_by(PurchaseMovement.customer_id)
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
        before_count = int(c.purchase_count or 0)
        before_amount = int(c.purchase_amount_total or 0)
        balance_count, balance_amount = get_purchase_balances(db, c.id)
        expired_count = max(0, before_count - balance_count)
        expired_amount = max(0, before_amount - balance_amount)
        if expired_count > 0 or expired_amount > 0:
            db.add(
                PurchaseMovement(
                    customer_id=c.id,
                    quantity=-expired_count,
                    amount=-expired_amount,
                    type="EXPIRE",
                    source_transaction_id=None,
                    expires_at=None,
                )
            )
        if balance_count != before_count or balance_amount != before_amount:
            sync_customer_purchase_counters(db, c)

        update_customer_status(
            db,
            c,
            reason="PURCHASES_EXPIRED",
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
