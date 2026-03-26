from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.services.loyalty_settings_service import get_loyalty_settings
from app.services.loyalty_status_service import update_customer_status


def expire_points(db: Session, *, brand: str) -> int:
    now = datetime.utcnow()

    settings = get_loyalty_settings(db, brand=brand)
    points_days = getattr(settings, "points_validity_days", None) if settings else None
    if points_days is None:
        return 0

    expired_customers = (
        db.query(Customer)
        .filter(Customer.brand == brand)
        .filter(Customer.points_expires_at.isnot(None))
        .filter(Customer.points_expires_at < now)
        .all()
    )

    updated = 0
    for c in expired_customers:
        if int(c.status_points or 0) != 0:
            c.status_points = 0
            c.status_points_reset_at = now
        c.points_expires_at = None
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

    expired_customers = (
        db.query(Customer)
        .filter(Customer.brand == brand)
        .filter(Customer.loyalty_status_expires_at.isnot(None))
        .filter(Customer.loyalty_status_expires_at < now)
        .all()
    )

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
