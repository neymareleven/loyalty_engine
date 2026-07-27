from __future__ import annotations

from datetime import date, datetime, time

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.point_movement import PointMovement


def _active_point_movement_filters(*, today: date | None = None):
    today = today or date.today()
    return (
        PointMovement.type != "EXPIRE",
        (PointMovement.expires_at.is_(None)) | (PointMovement.expires_at >= today),
    )


def get_status_points_balance(db: Session, customer_id):
    today = date.today()
    active_filters = _active_point_movement_filters(today=today)

    balance = (
        db.query(func.coalesce(func.sum(PointMovement.points), 0))
        .filter(PointMovement.customer_id == customer_id, *active_filters)
        .scalar()
    )

    return max(0, int(balance or 0))


def get_points_balance(db: Session, customer_id):
    return get_status_points_balance(db, customer_id)


def get_next_points_expiry_date(db: Session, customer_id) -> date | None:
    """Earliest expiry among still-valid positive earns; None when no points remain."""
    if get_status_points_balance(db, customer_id) <= 0:
        return None

    today = date.today()
    return (
        db.query(func.min(PointMovement.expires_at))
        .filter(
            PointMovement.customer_id == customer_id,
            PointMovement.points > 0,
            PointMovement.type != "EXPIRE",
            PointMovement.expires_at.isnot(None),
            PointMovement.expires_at >= today,
        )
        .scalar()
    )


def resolve_points_expires_at(db: Session, customer_id) -> datetime | None:
    """Display date for « Points valables jusqu'au » — null when nothing left to expire."""
    expiry_date = get_next_points_expiry_date(db, customer_id)
    if expiry_date is None:
        return None
    return datetime.combine(expiry_date, time(23, 59, 59))


def sync_customer_points_expires_at(db: Session, customer) -> None:
    customer.points_expires_at = resolve_points_expires_at(db, customer.id)


def is_point_movement_expired(movement: PointMovement, *, today: date | None = None) -> bool:
    today = today or date.today()
    if movement.type == "EXPIRE":
        return False
    if movement.points <= 0:
        return False
    if movement.expires_at is None:
        return False
    return movement.expires_at < today


def serialize_point_movement_out(movement: PointMovement, *, today: date | None = None) -> dict:
    today = today or date.today()
    expired = is_point_movement_expired(movement, today=today)
    if movement.type == "EXPIRE":
        status = "expired"
    elif expired:
        status = "expired"
    elif movement.points > 0 and movement.expires_at is not None:
        status = "active_with_expiry"
    else:
        status = "active"

    return {
        "id": movement.id,
        "customer_id": movement.customer_id,
        "points": int(movement.points or 0),
        "type": movement.type,
        "source_transaction_id": movement.source_transaction_id,
        "created_at": movement.created_at,
        "expires_at": movement.expires_at,
        "isExpired": expired or movement.type == "EXPIRE",
        "status": status,
    }
