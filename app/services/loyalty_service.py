from datetime import date, datetime, timedelta
from sqlalchemy.orm import Session
from fastapi import HTTPException

from app.models.point_movement import PointMovement
from app.models.customer import Customer
from app.services.loyalty_status_service import update_customer_status
from app.services.loyalty_settings_service import get_loyalty_settings
from app.services.wallet_service import get_points_balance


# ============================================================
# EARN POINTS
# ============================================================

def earn_points(
    db: Session,
    customer,
    *,
    points: int,
    source_transaction_id,
    depth: int = 0,
):
    try:
        points = int(points)
    except Exception:
        return None

    if points <= 0:
        return None

    settings = get_loyalty_settings(db, brand=customer.brand)
    points_days = getattr(settings, "points_validity_days", None) if settings else None
    expires_at = (date.today() + timedelta(days=int(points_days))) if points_days is not None else None

    # 🔹 sécuriser le customer attaché à la session
    customer = db.query(Customer).filter(Customer.id == customer.id).with_for_update().one()

    movement = PointMovement(
        customer_id=customer.id,
        points=points,
        type="EARN",
        source_transaction_id=source_transaction_id,
        expires_at=expires_at,
    )

    db.add(movement)

    # 🔹 mise à jour lifetime fiable
    customer.lifetime_points = (customer.lifetime_points or 0) + points

    customer.status_points = (customer.status_points or 0) + points

    # 🔹 recalcul statut
    try:
        depth = int(depth or 0)
    except Exception:
        depth = 0

    update_customer_status(
        db,
        customer,
        reason="EARN_POINTS",
        source_transaction_id=source_transaction_id,
        depth=depth,
    )

    db.flush()

    return movement


# ============================================================
# BURN POINTS
# ============================================================

def burn_points(
    db: Session,
    customer,
    *,
    points: int,
    source_transaction_id,
    depth: int = 0,
):
    try:
        points = int(points)
    except Exception:
        return None

    if points <= 0:
        return None

    # Ensure we operate on the row attached to the current session.
    customer = db.query(Customer).filter(Customer.id == customer.id).with_for_update().one()

    balance = get_points_balance(db, customer.id)
    if balance < points:
        raise HTTPException(status_code=400, detail="Not enough points")

    movement = PointMovement(
        customer_id=customer.id,
        points=-points,
        type="BURN",
        source_transaction_id=source_transaction_id,
    )

    db.add(movement)

    customer.status_points = max(0, int(customer.status_points or 0) - points)

    try:
        depth = int(depth or 0)
    except Exception:
        depth = 0

    update_customer_status(
        db,
        customer,
        reason="BURN_POINTS",
        source_transaction_id=source_transaction_id,
        depth=depth,
    )

    db.flush()

    return movement


def burn_wallet_points(db: Session, customer, *, points: int, source_transaction_id):
    return burn_points(db, customer, points=points, source_transaction_id=source_transaction_id)
