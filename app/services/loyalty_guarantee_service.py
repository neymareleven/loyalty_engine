from datetime import datetime

from sqlalchemy.orm import Session

from app.models.customer_loyalty_guarantee import (
    GUARANTEE_ADMIN,
    GUARANTEE_BYPASS,
    GUARANTEE_MODE_HOLD,
    CustomerLoyaltyGuarantee,
)


def _utcnow() -> datetime:
    return datetime.utcnow()


def active_guarantees(db: Session, *, customer_id) -> list[CustomerLoyaltyGuarantee]:
    now = _utcnow()
    rows = (
        db.query(CustomerLoyaltyGuarantee)
        .filter(CustomerLoyaltyGuarantee.customer_id == customer_id)
        .filter(CustomerLoyaltyGuarantee.revoked_at.is_(None))
        .all()
    )
    return [g for g in rows if g.expires_at is None or g.expires_at > now]


def highest_guarantee_floor_key(db: Session, *, customer_id, brand: str) -> str | None:
    from app.services.loyalty_status_service import _get_tier_rank_value

    best_key = None
    best_rank = None
    for g in active_guarantees(db, customer_id=customer_id):
        rank = _get_tier_rank_value(db, brand, g.floor_tier_key)
        if rank is None:
            continue
        if best_rank is None or rank > best_rank:
            best_rank = rank
            best_key = g.floor_tier_key
    return best_key


def grant_loyalty_guarantee(
    db: Session,
    *,
    customer,
    source: str,
    floor_tier_key: str,
    expires_at: datetime | None,
) -> CustomerLoyaltyGuarantee:
    from app.services.loyalty_status_service import _get_tier_rank_value

    key = (floor_tier_key or "").strip()
    if not key:
        raise ValueError("floor_tier_key is required")
    existing = (
        db.query(CustomerLoyaltyGuarantee)
        .filter(CustomerLoyaltyGuarantee.customer_id == customer.id)
        .filter(CustomerLoyaltyGuarantee.source == source)
        .filter(CustomerLoyaltyGuarantee.revoked_at.is_(None))
        .first()
    )
    new_rank = _get_tier_rank_value(db, customer.brand, key)
    if existing:
        old_rank = _get_tier_rank_value(db, customer.brand, existing.floor_tier_key)
        if new_rank is not None and (old_rank is None or new_rank >= old_rank):
            existing.floor_tier_key = key
        if expires_at is None:
            existing.expires_at = None
        elif existing.expires_at is None:
            pass
        elif expires_at > existing.expires_at:
            existing.expires_at = expires_at
        db.flush()
        return existing
    row = CustomerLoyaltyGuarantee(
        customer_id=customer.id,
        brand=customer.brand,
        source=source,
        floor_tier_key=key,
        expires_at=expires_at,
    )
    db.add(row)
    db.flush()
    return row


def serialize_guarantees(db: Session, *, customer_id) -> list[dict]:
    return [
        {
            "source": g.source,
            "floorTierKey": g.floor_tier_key,
            "expiresAt": g.expires_at,
            "createdAt": g.created_at,
        }
        for g in active_guarantees(db, customer_id=customer_id)
    ]


ACTIVITY_REASONS = frozenset(
    {
        "EARN_POINTS",
        "PURCHASE_RECORDED",
        "FIRST_PURCHASE_BYPASS",
        "ADMIN_OVERRIDE",
        "ADMIN_SET_TIER",
    }
)

CAUSE_BY_REASON = {
    "FIRST_PURCHASE_BYPASS": "first_purchase_bypass",
    "ADMIN_OVERRIDE": "admin",
    "ADMIN_SET_TIER": "admin",
    "QUALIFICATION_MODE_CHANGED": "mode_switch",
    "LOYALTY_STATUS_EXPIRED": "expiration",
    "POINTS_EXPIRED": "expiration",
    "PURCHASES_EXPIRED": "expiration",
}


def cause_from_reason(reason: str | None, *, default: str = "thresholds") -> str:
    return CAUSE_BY_REASON.get(reason or "", default)


__all__ = [
    "ACTIVITY_REASONS",
    "CAUSE_BY_REASON",
    "GUARANTEE_ADMIN",
    "GUARANTEE_BYPASS",
    "GUARANTEE_MODE_HOLD",
    "active_guarantees",
    "cause_from_reason",
    "grant_loyalty_guarantee",
    "highest_guarantee_floor_key",
    "serialize_guarantees",
]
