from datetime import datetime

from sqlalchemy.orm import Session

from app.models.loyalty_tier import LoyaltyTier


def compute_loyalty_status_from_tiers(db: Session, brand: str, status_points: int) -> str | None:
    points = int(status_points or 0)

    tier = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .filter(LoyaltyTier.min_status_points <= points)
        .order_by(LoyaltyTier.min_status_points.desc(), LoyaltyTier.rank.desc())
        .first()
    )
    if tier:
        return tier.key

    # No tiers configured for this brand: do not update loyalty_status.
    return None


def _get_tier_rank(db: Session, brand: str, tier_key: str) -> int:
    if not tier_key:
        return 0

    tier = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .filter(LoyaltyTier.key == tier_key)
        .first()
    )
    if tier:
        return int(tier.rank)

    return 0


# ============================================================
# Mise à jour du statut client
# ============================================================
def update_customer_status(db: Session, customer, *, reason: str = "EARN_POINTS", source_transaction_id=None, depth: int = 0):
    """
    Recalcule et met à jour le statut fidélité du client
    """

    new_status = compute_loyalty_status_from_tiers(db, customer.brand, customer.status_points)
    if new_status is None:
        if not customer.loyalty_status:
            customer.loyalty_status = "UNCONFIGURED"
            db.flush()
        return customer.loyalty_status

    # éviter des écritures DB inutiles
    if customer.loyalty_status != new_status:
        old_status = customer.loyalty_status
        customer.loyalty_status = new_status
        db.flush()

        old_rank = _get_tier_rank(db, customer.brand, old_status)
        new_rank = _get_tier_rank(db, customer.brand, new_status)
        event_type = "TIER_UPGRADED" if new_rank > old_rank else "TIER_DOWNGRADED"

        from app.models.event_type import EventType
        from app.services.transaction_service import create_internal_transaction

        # Only emit if the event type exists in the catalog as INTERNAL+active
        et = (
            db.query(EventType.id)
            .filter(EventType.key == event_type)
            .filter(EventType.active.is_(True))
            .filter(EventType.origin == "INTERNAL")
            .filter(EventType.brand == customer.brand)
            .first()
        )
        if et:
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
            event_id = f"tier_{customer.brand}_{customer.profile_id}_{event_type}_{ts}"
            payload = {
                "fromTier": old_status,
                "toTier": new_status,
                "reason": reason,
                "statusPoints": int(customer.status_points or 0),
                "sourceTransactionId": str(source_transaction_id) if source_transaction_id else None,
                "_ruleDepth": depth + 1,
            }
            create_internal_transaction(
                db,
                brand=customer.brand,
                profile_id=customer.profile_id,
                event_type=event_type,
                event_id=event_id,
                payload=payload,
                depth=depth,
                commit=False,
            )

    return customer.loyalty_status
