from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import date
from app.models.point_movement import PointMovement


def get_points_balance(db: Session, customer_id):
    today = date.today()

    balance = (
        db.query(func.coalesce(func.sum(PointMovement.points), 0))
        .filter(
            PointMovement.customer_id == customer_id,
            (PointMovement.expires_at.is_(None)) | (PointMovement.expires_at >= today)
        )
        .scalar()
    )

    return int(balance or 0)
