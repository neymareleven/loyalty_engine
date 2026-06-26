"""Delete loyalty customers and keep Unomi profile sync aligned."""

from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from app.models.cash_movement import CashMovement
from app.models.customer import Customer
from app.models.customer_coupon import CustomerCoupon
from app.models.customer_metrics import CustomerMetrics
from app.models.customer_reward import CustomerReward
from app.models.point_movement import PointMovement
from app.services.unomi_profile_service import delete_profile_from_unomi, set_profile_sync_source, reset_profile_sync_source

logger = logging.getLogger(__name__)


def delete_loyalty_customer(
    db: Session,
    *,
    brand: str,
    profile_id: str,
    skip_unomi: bool = True,
) -> dict:
    """Remove customer row and dependent loyalty data; optionally delete Unomi profile."""
    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .first()
    )
    if not customer:
        return {"deleted": False, "reason": "not_found"}

    customer_id = customer.id

    db.query(PointMovement).filter(PointMovement.customer_id == customer_id).delete(synchronize_session=False)
    db.query(CustomerCoupon).filter(CustomerCoupon.customer_id == customer_id).delete(synchronize_session=False)
    db.query(CustomerReward).filter(CustomerReward.customer_id == customer_id).delete(synchronize_session=False)
    db.query(CashMovement).filter(CashMovement.customer_id == customer_id).delete(synchronize_session=False)
    db.query(CustomerMetrics).filter(CustomerMetrics.customer_id == customer_id).delete(synchronize_session=False)

    db.delete(customer)
    db.flush()

    unomi_result = None
    if not skip_unomi:
        token = set_profile_sync_source("loyalty")
        try:
            unomi_result = delete_profile_from_unomi(brand=brand, profile_id=profile_id)
        finally:
            reset_profile_sync_source(token)

    logger.info("loyalty customer deleted brand=%s profile_id=%s", brand, profile_id)
    return {
        "deleted": True,
        "brand": brand,
        "profileId": profile_id,
        "unomi": unomi_result,
    }
