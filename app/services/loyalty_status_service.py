from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from app.models.loyalty_tier import LoyaltyTier
from app.services.loyalty_settings_service import get_loyalty_settings

# Recalc after POINTS ↔ PURCHASES: not an upgrade, downgrade, or renew.
QUALIFICATION_MODE_CHANGED_REASON = "QUALIFICATION_MODE_CHANGED"
FIRST_PURCHASE_BYPASS_REASON = "FIRST_PURCHASE_BYPASS"


def last_change_from_tier_event(transaction_type: str | None) -> str | None:
    if transaction_type == "TIER_UPGRADED":
        return "upgrade"
    if transaction_type == "TIER_DOWNGRADED":
        return "downgrade"
    if transaction_type == "TIER_RENEWED":
        return "no_change"
    if transaction_type == "ADMIN_SET_TIER":
        return "admin_override"
    if transaction_type == "QUALIFICATION_MODE_CHANGED":
        return "mode_changed"
    if transaction_type == FIRST_PURCHASE_BYPASS_REASON:
        return "first_purchase_bypass"
    return None


def _tier_sort_key(tier: LoyaltyTier) -> tuple[int, int, int]:
    return (
        int(tier.min_status_points or 0),
        int(getattr(tier, "min_purchase_count", 0) or 0),
        int(getattr(tier, "min_purchase_amount", 0) or 0),
    )


def _tier_meets_points(tier: LoyaltyTier, status_points: int) -> bool:
    return int(tier.min_status_points or 0) <= int(status_points or 0)


def _tier_meets_purchases(tier: LoyaltyTier, purchase_count: int, purchase_amount: int) -> bool:
    return (
        int(getattr(tier, "min_purchase_count", 0) or 0) <= int(purchase_count or 0)
        and int(getattr(tier, "min_purchase_amount", 0) or 0) <= int(purchase_amount or 0)
    )


def compute_loyalty_status_from_tiers(
    db: Session,
    brand: str,
    status_points: int,
    *,
    purchase_count: int = 0,
    purchase_amount: int = 0,
    qualification_mode: str | None = None,
) -> str | None:
    from app.services.purchase_service import QUALIFICATION_PURCHASES, get_qualification_mode

    mode = qualification_mode or get_qualification_mode(db, brand=brand)
    active_tiers = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .all()
    )
    if not active_tiers:
        return None

    if mode == QUALIFICATION_PURCHASES:
        qualifying = [
            t
            for t in active_tiers
            if _tier_meets_purchases(t, purchase_count, purchase_amount)
        ]
    else:
        qualifying = [t for t in active_tiers if _tier_meets_points(t, status_points)]

    if qualifying:
        best = max(qualifying, key=_tier_sort_key)
        return best.key

    # Fallback to lowest active tier (base) to avoid UNCONFIGURED when tiers exist.
    lowest = min(active_tiers, key=_tier_sort_key)
    return lowest.key


def _get_base_tier_key(db: Session, brand: str) -> str | None:
    tiers = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .all()
    )
    if not tiers:
        return None
    return min(tiers, key=_tier_sort_key).key


def _get_tier_rank_value(db: Session, brand: str, tier_key: str) -> int | None:
    if not tier_key:
        return None
    tier = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .filter(LoyaltyTier.key == tier_key)
        .first()
    )
    if not tier:
        return None
    return int(tier.rank)


def _get_tier_sort_tuple(db: Session, brand: str, tier_key: str) -> tuple[int, int, int] | None:
    if not tier_key:
        return None
    tier = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .filter(LoyaltyTier.key == tier_key)
        .first()
    )
    if not tier:
        return None
    return _tier_sort_key(tier)


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
    sync_unomi: bool = False,
    min_tier_key: str | None = None,
):
    """
    Recalcule et met à jour le statut fidélité du client
    selon qualification_mode (POINTS ou PURCHASES).

    reason=QUALIFICATION_MODE_CHANGED applies the palier of the current
    mode immediately (no anti-downgrade holdover) and emits
    QUALIFICATION_MODE_CHANGED instead of TIER_UPGRADED/DOWNGRADED/RENEWED.
    """
    is_mode_switch = reason == QUALIFICATION_MODE_CHANGED_REASON
    if is_mode_switch:
        allow_downgrade_before_expiry = True
        refresh_window = True
    from app.services.purchase_service import (
        QUALIFICATION_PURCHASES,
        get_purchase_balances,
        get_qualification_mode,
    )

    mode = get_qualification_mode(db, brand=customer.brand)
    purchase_count = int(getattr(customer, "purchase_count", 0) or 0)
    purchase_amount = int(getattr(customer, "purchase_amount_total", 0) or 0)
    if mode == QUALIFICATION_PURCHASES:
        # Prefer live balances from movements when available.
        purchase_count, purchase_amount = get_purchase_balances(db, customer.id)
        customer.purchase_count = purchase_count
        customer.purchase_amount_total = purchase_amount

    new_status = compute_loyalty_status_from_tiers(
        db,
        customer.brand,
        customer.status_points,
        purchase_count=purchase_count,
        purchase_amount=purchase_amount,
        qualification_mode=mode,
    )
    floor_key = (min_tier_key or "").strip() or None
    if floor_key:
        floor_rank = _get_tier_sort_tuple(db, customer.brand, floor_key)
        current_rank = _get_tier_sort_tuple(db, customer.brand, new_status) if new_status else None
        if floor_rank is not None and (current_rank is None or current_rank < floor_rank):
            new_status = floor_key
            if not is_mode_switch:
                reason = FIRST_PURCHASE_BYPASS_REASON
    if new_status is None:
        if not customer.loyalty_status:
            customer.loyalty_status = "UNCONFIGURED"
            db.flush()
        if sync_unomi:
            from app.services.unomi_profile_service import sync_customer_profile_to_unomi

            sync_customer_profile_to_unomi(db, customer=customer, reason="loyalty_status")
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
        old_rank = _get_tier_sort_tuple(db, customer.brand, customer.loyalty_status)
        new_rank = _get_tier_sort_tuple(db, customer.brand, new_status)
        is_downgrade = bool(old_rank is not None and new_rank is not None and new_rank < old_rank)
        not_expired = bool(customer.loyalty_status_expires_at is not None and customer.loyalty_status_expires_at > now)
        if is_downgrade and not allow_downgrade_before_expiry and not_expired:
            new_status = customer.loyalty_status

    is_base_tier = bool(base_tier_key) and (new_status == base_tier_key)

    # Safety: base tier must never expire, even if no refresh/status-change happens.
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

    if customer.loyalty_status != new_status:
        old_status = customer.loyalty_status
        customer.loyalty_status = new_status
        db.flush()

    if emit_events:
        from app.services.transaction_service import create_internal_transaction

        status_changed = old_status != new_status
        if is_mode_switch:
            transaction_type = QUALIFICATION_MODE_CHANGED_REASON
        elif status_changed and reason == FIRST_PURCHASE_BYPASS_REASON:
            transaction_type = FIRST_PURCHASE_BYPASS_REASON
        elif status_changed:
            old_rank = _get_tier_sort_tuple(db, customer.brand, old_status)
            new_rank = _get_tier_sort_tuple(db, customer.brand, new_status)
            transaction_type = (
                "TIER_UPGRADED"
                if (new_rank is not None and (old_rank is None or new_rank > old_rank))
                else "TIER_DOWNGRADED"
            )
        elif did_refresh_window_without_tier_change:
            transaction_type = "TIER_RENEWED"
        else:
            transaction_type = None

        if transaction_type:
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
            transaction_id = f"tier_{customer.brand}_{customer.profile_id}_{transaction_type}_{ts}"
            payload = {
                "fromTier": old_status,
                "toTier": new_status,
                "reason": reason,
                "qualificationMode": mode,
                "statusPoints": int(customer.status_points or 0),
                "purchaseCount": int(getattr(customer, "purchase_count", 0) or 0),
                "purchaseAmountTotal": int(getattr(customer, "purchase_amount_total", 0) or 0),
                "sourceTransactionId": str(source_transaction_id) if source_transaction_id else None,
                "_ruleDepth": depth + 1,
            }
            if transaction_type in (QUALIFICATION_MODE_CHANGED_REASON, "TIER_RENEWED"):
                payload["tier"] = new_status
                payload["previousAssignedAt"] = old_assigned_at.isoformat() if old_assigned_at else None
                payload["previousExpiresAt"] = old_expires_at.isoformat() if old_expires_at else None
                payload["assignedAt"] = (
                    customer.loyalty_status_assigned_at.isoformat()
                    if customer.loyalty_status_assigned_at
                    else None
                )
                payload["expiresAt"] = (
                    customer.loyalty_status_expires_at.isoformat()
                    if customer.loyalty_status_expires_at
                    else None
                )
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

    if sync_unomi:
        from app.services.unomi_profile_service import sync_customer_profile_to_unomi

        sync_customer_profile_to_unomi(db, customer=customer, reason="loyalty_status")
    return customer.loyalty_status
