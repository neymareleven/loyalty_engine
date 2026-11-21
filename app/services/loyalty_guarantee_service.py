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


def _unrevoked_guarantees(db: Session, *, customer_id) -> list[CustomerLoyaltyGuarantee]:
    return (
        db.query(CustomerLoyaltyGuarantee)
        .filter(CustomerLoyaltyGuarantee.customer_id == customer_id)
        .filter(CustomerLoyaltyGuarantee.revoked_at.is_(None))
        .all()
    )


def _guarantee_is_expired(g: CustomerLoyaltyGuarantee, now: datetime) -> bool:
    return g.expires_at is not None and g.expires_at <= now


def active_guarantees(db: Session, *, customer_id) -> list[CustomerLoyaltyGuarantee]:
    now = _utcnow()
    return [g for g in _unrevoked_guarantees(db, customer_id=customer_id) if not _guarantee_is_expired(g, now)]


def revoke_expired_guarantees(db: Session, *, customer_id, now: datetime | None = None) -> int:
    now = now or _utcnow()
    revoked = 0
    for g in _unrevoked_guarantees(db, customer_id=customer_id):
        if _guarantee_is_expired(g, now):
            g.revoked_at = now
            revoked += 1
    if revoked:
        db.flush()
    return revoked


def revoke_open_ended_guarantees_on_base(db: Session, *, customer) -> int:
    from app.services.loyalty_status_service import _get_base_tier_key

    base_key = _get_base_tier_key(db, customer.brand)
    if not base_key or customer.loyalty_status != base_key:
        return 0
    now = _utcnow()
    revoked = 0
    for g in _unrevoked_guarantees(db, customer_id=customer.id):
        if g.expires_at is None:
            g.revoked_at = now
            revoked += 1
    if revoked:
        db.flush()
    return revoked


def revoke_guarantees_above_status(db: Session, *, customer) -> int:
    from app.services.loyalty_status_service import _get_tier_rank_value

    now = _utcnow()
    current_rank = _get_tier_rank_value(db, customer.brand, customer.loyalty_status)
    revoked = 0
    for g in active_guarantees(db, customer_id=customer.id):
        floor_rank = _get_tier_rank_value(db, customer.brand, g.floor_tier_key)
        if floor_rank is None or current_rank is None:
            continue
        if floor_rank > current_rank:
            g.revoked_at = now
            revoked += 1
            logger.info(
                "loyalty guarantee revoked above status customer_id=%s source=%s floor=%s status=%s",
                customer.id,
                g.source,
                g.floor_tier_key,
                customer.loyalty_status,
            )
    if revoked:
        db.flush()
    return revoked


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


def _default_guarantee_expires_at(db: Session, *, customer) -> datetime | None:
    expires_at = getattr(customer, "loyalty_status_expires_at", None)
    if expires_at is not None:
        return expires_at
    from app.services.loyalty_settings_service import get_loyalty_settings
    from app.services.loyalty_status_service import status_window_expires_at

    settings = get_loyalty_settings(db, brand=customer.brand)
    days = getattr(settings, "loyalty_status_validity_days", None) if settings else None
    if days is None:
        return None
    return status_window_expires_at(_utcnow(), int(days))


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
    now = _utcnow()
    existing = (
        db.query(CustomerLoyaltyGuarantee)
        .filter(CustomerLoyaltyGuarantee.customer_id == customer.id)
        .filter(CustomerLoyaltyGuarantee.source == source)
        .filter(CustomerLoyaltyGuarantee.revoked_at.is_(None))
        .first()
    )
    if existing and (
        existing.expires_at is None or _guarantee_is_expired(existing, now)
    ):
        existing.revoked_at = now
        existing = None
    new_rank = _get_tier_rank_value(db, customer.brand, key)
    if existing:
        old_rank = _get_tier_rank_value(db, customer.brand, existing.floor_tier_key)
        if new_rank is not None and (old_rank is None or new_rank >= old_rank):
            existing.floor_tier_key = key
        db.flush()
        return existing
    if expires_at is None:
        expires_at = _default_guarantee_expires_at(db, customer=customer)
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
    from app.services.loyalty_status_service import _get_base_tier_key

    now = _utcnow()
    revoke_expired_guarantees(db, customer_id=customer.id, now=now)
    base_key = _get_base_tier_key(db, customer.brand)
    is_base = bool(base_key) and customer.loyalty_status == base_key
    status_expires = customer.loyalty_status_expires_at
    aligned = 0
    revoked = 0
    for g in active_guarantees(db, customer_id=customer.id):
        if is_base or status_expires is None:
            g.revoked_at = now
            revoked += 1
            logger.info(
                "loyalty guarantee revoked no status window customer_id=%s source=%s floor=%s",
                customer.id,
                g.source,
                g.floor_tier_key,
            )
            continue
        if g.expires_at != status_expires:
            g.expires_at = status_expires
            aligned += 1
    if aligned or revoked:
        db.flush()
    return aligned


def cleanup_loyalty_guarantees(db: Session) -> dict[str, int]:
    """Revoke stale/expired/above-status guarantees; align remaining to the status window. Idempotent."""
    from app.models.customer import Customer
    from app.services.loyalty_status_service import (
        _get_base_tier_key,
        _get_tier_rank_value,
        compute_loyalty_status_from_tiers,
    )
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
            current_rank = _get_tier_rank_value(db, customer.brand, customer.loyalty_status)
            base_key = _get_base_tier_key(db, customer.brand)
        except Exception:
            logger.exception("cleanup_loyalty_guarantees skip customer_id=%s", customer_id)
            skipped += 1
            continue
        is_base = bool(base_key) and customer.loyalty_status == base_key
        status_expires = customer.loyalty_status_expires_at
        for g in guarantees:
            if g.revoked_at is not None:
                continue
            floor_rank = _get_tier_rank_value(db, customer.brand, g.floor_tier_key)
            expired = _guarantee_is_expired(g, now)
            earned_covers = floor_rank is None or (earned_rank is not None and earned_rank >= floor_rank)
            above_status = (
                floor_rank is not None
                and current_rank is not None
                and floor_rank > current_rank
            )
            ghost_open_ended = g.expires_at is None and is_base
            no_window = is_base or status_expires is None
            if expired or earned_covers or above_status or ghost_open_ended or no_window:
                g.revoked_at = now
                revoked += 1
                logger.info(
                    "cleanup_loyalty_guarantees revoked customer_id=%s source=%s floor=%s earned=%s",
                    customer_id,
                    g.source,
                    g.floor_tier_key,
                    earned,
                )
            elif g.expires_at != status_expires:
                g.expires_at = status_expires
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
    "revoke_expired_guarantees",
    "revoke_guarantees_above_status",
    "revoke_open_ended_guarantees_on_base",
    "serialize_guarantees",
]
