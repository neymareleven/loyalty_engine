from datetime import date, timedelta
from sqlalchemy.orm import Session

from app.models.point_movement import PointMovement
from app.models.customer import Customer
from app.services.loyalty_status_service import update_customer_status


# ============================================================
# EARN POINTS
# ============================================================

def earn_points(db: Session, customer, transaction):
    payload = transaction.payload or {}

    amount = payload.get("amount")
    if not amount:
        return None

    points = int(amount)

    expires_at = date.today() + timedelta(days=365)

    # 🔹 sécuriser le customer attaché à la session
    customer = db.query(Customer).filter(Customer.id == customer.id).with_for_update().one()

    movement = PointMovement(
        customer_id=customer.id,
        points=points,
        type="EARN",
        source_transaction_id=transaction.id,
        expires_at=expires_at,
    )

    db.add(movement)

    # 🔹 mise à jour lifetime fiable
    customer.lifetime_points = (customer.lifetime_points or 0) + points

    customer.status_points = (customer.status_points or 0) + points

    # 🔹 recalcul statut
    depth = 0
    try:
        depth = int((transaction.payload or {}).get("_ruleDepth") or 0)
    except Exception:
        depth = 0

    update_customer_status(db, customer, reason="EARN_POINTS", source_transaction_id=transaction.id, depth=depth)

    db.flush()

    return movement


# ============================================================
# BURN POINTS
# ============================================================

def burn_points(db: Session, customer, transaction):
    payload = transaction.payload or {}

    points = int(payload.get("points", 0))
    if points <= 0:
        return None

    movement = PointMovement(
        customer_id=customer.id,
        points=-points,
        type="BURN",
        source_transaction_id=transaction.id,
    )

    db.add(movement)
    db.flush()

    return movement
