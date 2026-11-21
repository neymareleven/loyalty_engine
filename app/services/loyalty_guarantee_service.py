import logging
from datetime import datetime

from sqlalchemy.orm import Session

from app.models.customer_loyalty_guarantee import (
    GUARANTEE_ADMIN,
    GUARANTEE_BYPASS,
    GUARANTEE_MODE_HOLD,
    CustomerLoyaltyGuarantee,
)

logger = logging.getLogger(__name__)


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


def revoke_consumed_guarantees(db: Session, *, customer, earned_status: str | None) -> int:
    from app.services.loyalty_status_service import _get_tier_rank_value

    earned_rank = _get_tier_rank_value(db, customer.brand, earned_status) if earned_status else None
    now = _utcnow()
    revoked = 0
    for g in active_guarantees(db, customer_id=customer.id):
        floor_rank = _get_tier_rank_value(db, customer.brand, g.floor_tier_key)
        if floor_rank is None or (earned_rank is not None and earned_rank >= floor_rank):
            g.revoked_at = now
            revoked += 1
            logger.info(
                "loyalty guarantee revoked customer_id=%s source=%s floor=%s earned=%s",
                customer.id,
                g.source,
                g.floor_tier_key,
                earned_status,
            )
    if revoked:
        db.flush()
    return revoked


def align_guarantee_expiry_to_status(db: Session, *, customer) -> int:
    aligned = 0
    status_expires = customer.loyalty_status_expires_at
    for g in active_guarantees(db, customer_id=customer.id):
        if g.expires_at != status_expires:
            g.expires_at = status_expires
            aligned += 1
    if aligned:
        db.flush()
    return aligned


def cleanup_loyalty_guarantees(db: Session) -> dict[str, int]:
    """Revoke earned floors and align remaining expiry to the status window. Idempotent."""
    from app.models.customer import Customer
    from app.services.loyalty_status_service import _get_tier_rank_value, compute_loyalty_status_from_tiers
    from app.services.purchase_service import (
        QUALIFICATION_PURCHASES,
        get_purchase_balances,
        get_qualification_mode,
    )

    rows = (
        db.query(CustomerLoyaltyGuarantee)
        .filter(CustomerLoyaltyGuarantee.revoked_at.is_(None))
        .all()
    )
    by_customer: dict = {}
    for g in rows:
        by_customer.setdefault(g.customer_id, []).append(g)

    revoked = 0
    aligned = 0
    skipped = 0
    now = _utcnow()
    for customer_id, guarantees in by_customer.items():
        customer = db.query(Customer).filter(Customer.id == customer_id).first()
        if customer is None:
            logger.info("cleanup_loyalty_guarantees skip missing customer_id=%s", customer_id)
            skipped += 1
            continue
        try:
            mode = get_qualification_mode(db, brand=customer.brand)
            purchase_count = int(getattr(customer, "purchase_count", 0) or 0)
            purchase_amount = int(getattr(customer, "purchase_amount_total", 0) or 0)
            if mode == QUALIFICATION_PURCHASES:
                purchase_count, purchase_amount = get_purchase_balances(db, customer.id)
            earned = compute_loyalty_status_from_tiers(
                db,
                customer.brand,
                int(customer.status_points or 0),
                purchase_count=purchase_count,
                purchase_amount=purchase_amount,
                qualification_mode=mode,
            )
            earned_rank = _get_tier_rank_value(db, customer.brand, earned) if earned else None
        except Exception:
            logger.exception("cleanup_loyalty_guarantees skip customer_id=%s", customer_id)
            skipped += 1
            continue
        for g in guarantees:
            floor_rank = _get_tier_rank_value(db, customer.brand, g.floor_tier_key)
            if floor_rank is None or (earned_rank is not None and earned_rank >= floor_rank):
                g.revoked_at = now
                revoked += 1
                logger.info(
                    "cleanup_loyalty_guarantees revoked customer_id=%s source=%s floor=%s earned=%s",
                    customer_id,
                    g.source,
                    g.floor_tier_key,
                    earned,
                )
            elif g.expires_at != customer.loyalty_status_expires_at:
                g.expires_at = customer.loyalty_status_expires_at
                aligned += 1
    db.flush()
    logger.info(
        "cleanup_loyalty_guarantees done revoked=%s aligned=%s skipped=%s",
        revoked,
        aligned,
        skipped,
    )
    return {"revoked": revoked, "aligned": aligned, "skipped": skipped}


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
    "align_guarantee_expiry_to_status",
    "cause_from_reason",
    "cleanup_loyalty_guarantees",
    "grant_loyalty_guarantee",
    "highest_guarantee_floor_key",
    "revoke_consumed_guarantees",
    "serialize_guarantees",
]
