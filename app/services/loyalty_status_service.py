from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from app.models.loyalty_tier import LoyaltyTier
from app.services.loyalty_settings_service import get_loyalty_settings


def compute_loyalty_status_from_tiers(db: Session, brand: str, status_points: int) -> str | None:
    points = int(status_points or 0)

    tier = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .filter(LoyaltyTier.min_status_points <= points)
        .order_by(LoyaltyTier.min_status_points.desc())
        .first()
    )
    if tier:
        return tier.key

    # Tiers exist but points don't satisfy any min_status_points (e.g. negative status points).
    # Fallback to the lowest active tier to avoid leaving customers UNCONFIGURED when tiers are configured.
    lowest = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .order_by(LoyaltyTier.min_status_points.asc())
        .first()
    )
    if lowest:
        return lowest.key

    # No tiers configured for this brand: do not update loyalty_status.
    return None


def _get_base_tier_key(db: Session, brand: str) -> str | None:
    tier = (
        db.query(LoyaltyTier.key)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .order_by(LoyaltyTier.min_status_points.asc(), LoyaltyTier.created_at.asc())
        .first()
    )
    if not tier:
        return None
    return tier[0]


def _get_tier_min_points(db: Session, brand: str, tier_key: str) -> int | None:
    if not tier_key:
        return None

    tier = (
        db.query(LoyaltyTier.min_status_points)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .filter(LoyaltyTier.key == tier_key)
        .first()
    )
    if not tier:
        return None

    try:
        return int(tier[0])
    except Exception:
        return None


# ============================================================
# Mise à jour du statut client
# ============================================================
def update_customer_status(
    db: Session,
    customer,
    *,
    reason: str = "EARN_POINTS",
    source_transaction_id=None,
    depth: int = 0,
    refresh_window: bool = False,
    emit_events: bool = True,
    allow_downgrade_before_expiry: bool = False,
):
    """
    Recalcule et met à jour le statut fidélité du client
    """

    new_status = compute_loyalty_status_from_tiers(db, customer.brand, customer.status_points)
    if new_status is None:
        if not customer.loyalty_status:
            customer.loyalty_status = "UNCONFIGURED"
            db.flush()
        return customer.loyalty_status

    settings = get_loyalty_settings(db, brand=customer.brand)
    status_days = getattr(settings, "loyalty_status_validity_days", None) if settings else None
    base_tier_key = _get_base_tier_key(db, customer.brand)
    now = datetime.utcnow()

    old_status = customer.loyalty_status
    old_assigned_at = customer.loyalty_status_assigned_at
    old_expires_at = customer.loyalty_status_expires_at

    # Prevent automatic downgrades before the current loyalty status validity window expires.
    # Upgrades are still applied immediately.
    if customer.loyalty_status and customer.loyalty_status != new_status:
        old_min = _get_tier_min_points(db, customer.brand, customer.loyalty_status)
        new_min = _get_tier_min_points(db, customer.brand, new_status)
        is_downgrade = bool(
            new_min is not None
            and old_min is not None
            and int(new_min) < int(old_min)
        )
        not_expired = bool(customer.loyalty_status_expires_at is not None and customer.loyalty_status_expires_at > now)
        if is_downgrade and not allow_downgrade_before_expiry and not_expired:
            new_status = customer.loyalty_status

    is_base_tier = bool(base_tier_key) and (new_status == base_tier_key)

    # Safety: base tier must never expire, even if no refresh/status-change happens.
    # This fixes legacy rows that may already have an expiration set.
    if is_base_tier:
        if customer.loyalty_status_expires_at is not None:
            customer.loyalty_status_expires_at = None
        if customer.loyalty_status_assigned_at is None:
            customer.loyalty_status_assigned_at = now
    should_refresh_window = bool(refresh_window) or (customer.loyalty_status != new_status)
    if should_refresh_window:
        if is_base_tier:
            customer.loyalty_status_assigned_at = now
            customer.loyalty_status_expires_at = None
        else:
            if status_days is not None:
                customer.loyalty_status_assigned_at = now
                customer.loyalty_status_expires_at = now + timedelta(days=int(status_days))
            else:
                customer.loyalty_status_assigned_at = None
                customer.loyalty_status_expires_at = None

    did_refresh_window_without_tier_change = bool(
        should_refresh_window
        and (old_status == new_status)
        and (not is_base_tier)
        and (status_days is not None)
        and (
            (old_assigned_at != customer.loyalty_status_assigned_at)
            or (old_expires_at != customer.loyalty_status_expires_at)
        )
    )

    # éviter des écritures DB inutiles
    if customer.loyalty_status != new_status:
        old_status = customer.loyalty_status
        customer.loyalty_status = new_status
        db.flush()

        old_min = _get_tier_min_points(db, customer.brand, old_status)
        new_min = _get_tier_min_points(db, customer.brand, new_status)
        transaction_type = "TIER_UPGRADED" if (new_min is not None and (old_min is None or new_min > old_min)) else "TIER_DOWNGRADED"

        if emit_events:
            from app.models.event_type import TransactionType
            from app.services.transaction_service import create_internal_transaction

            # Only emit if the transaction type exists in the catalog as INTERNAL+active
            tt = (
                db.query(TransactionType.id)
                .filter(TransactionType.key == transaction_type)
                .filter(TransactionType.active.is_(True))
                .filter(TransactionType.origin == "INTERNAL")
                .filter(TransactionType.brand == customer.brand)
                .first()
            )
            if tt:
                ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
                transaction_id = f"tier_{customer.brand}_{customer.profile_id}_{transaction_type}_{ts}"
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
                    transaction_type=transaction_type,
                    transaction_id=transaction_id,
                    payload=payload,
                    depth=depth,
                    commit=False,
                )

    elif did_refresh_window_without_tier_change and emit_events:
        from app.models.event_type import TransactionType
        from app.services.transaction_service import create_internal_transaction

        transaction_type = "TIER_RENEWED"
        tt = (
            db.query(TransactionType.id)
            .filter(TransactionType.key == transaction_type)
            .filter(TransactionType.active.is_(True))
            .filter(TransactionType.origin == "INTERNAL")
            .filter(TransactionType.brand == customer.brand)
            .first()
        )
        if tt:
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
            transaction_id = f"tier_{customer.brand}_{customer.profile_id}_{transaction_type}_{ts}"
            payload = {
                "tier": new_status,
                "reason": reason,
                "statusPoints": int(customer.status_points or 0),
                "sourceTransactionId": str(source_transaction_id) if source_transaction_id else None,
                "previousAssignedAt": old_assigned_at.isoformat() if old_assigned_at else None,
                "previousExpiresAt": old_expires_at.isoformat() if old_expires_at else None,
                "assignedAt": customer.loyalty_status_assigned_at.isoformat() if customer.loyalty_status_assigned_at else None,
                "expiresAt": customer.loyalty_status_expires_at.isoformat() if customer.loyalty_status_expires_at else None,
                "_ruleDepth": depth + 1,
            }
            create_internal_transaction(
                db,
                brand=customer.brand,
                profile_id=customer.profile_id,
                transaction_type=transaction_type,
                transaction_id=transaction_id,
                payload=payload,
                depth=depth,
                commit=False,
            )

    return customer.loyalty_status
