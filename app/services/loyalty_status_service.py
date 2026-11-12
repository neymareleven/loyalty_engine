from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from app.models.loyalty_tier import LoyaltyTier
from app.services.loyalty_settings_service import get_loyalty_settings

# Recalc after POINTS ↔ PURCHASES: not an upgrade, downgrade, or renew.
QUALIFICATION_MODE_CHANGED_REASON = "QUALIFICATION_MODE_CHANGED"
FIRST_PURCHASE_BYPASS_REASON = "FIRST_PURCHASE_BYPASS"
TIER_CHANGED_REASON = "TIER_CHANGED"
MODE_SWITCH_POLICY_IMMEDIATE = "IMMEDIATE"
MODE_SWITCH_POLICY_HOLD = "HOLD_UNTIL_WINDOW"


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


def _tier_mode_sort_key(tier: LoyaltyTier, mode: str | None) -> tuple:
    from app.services.purchase_service import QUALIFICATION_PURCHASES

    if mode == QUALIFICATION_PURCHASES:
        return (
            int(getattr(tier, "min_purchase_count", 0) or 0),
            int(getattr(tier, "min_purchase_amount", 0) or 0),
            int(tier.min_status_points or 0),
        )
    return _tier_sort_key(tier)


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
        best = max(qualifying, key=lambda t: _tier_mode_sort_key(t, mode))
        return best.key

    lowest = min(active_tiers, key=lambda t: _tier_mode_sort_key(t, mode))
    return lowest.key


def _get_base_tier_key(db: Session, brand: str) -> str | None:
    from app.services.purchase_service import get_qualification_mode

    tiers = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .all()
    )
    if not tiers:
        return None
    mode = get_qualification_mode(db, brand=brand)
    zeros = [
        t
        for t in tiers
        if int(t.min_status_points or 0) == 0
        and int(getattr(t, "min_purchase_count", 0) or 0) == 0
        and int(getattr(t, "min_purchase_amount", 0) or 0) == 0
    ]
    if zeros:
        return min(zeros, key=lambda t: _tier_mode_sort_key(t, mode)).key
    return min(tiers, key=lambda t: _tier_mode_sort_key(t, mode)).key


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


def _apply_status_window(customer, *, now: datetime, status_days, is_base_tier: bool) -> None:
    if is_base_tier:
        customer.loyalty_status_assigned_at = now
        customer.loyalty_status_expires_at = None
        return
    customer.loyalty_status_assigned_at = now
    if status_days is not None:
        customer.loyalty_status_expires_at = now + timedelta(days=int(status_days))
    else:
        customer.loyalty_status_expires_at = None


def _emit_tier_transactions(
    db: Session,
    customer,
    *,
    transaction_type: str,
    payload: dict,
    depth: int,
    also_generic: bool,
):
    from app.services.transaction_service import create_internal_transaction

    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    create_internal_transaction(
        db,
        brand=customer.brand,
        profile_id=customer.profile_id,
        transaction_type=transaction_type,
        transaction_id=f"tier_{customer.brand}_{customer.profile_id}_{transaction_type}_{ts}",
        payload=payload,
        depth=depth,
        commit=False,
    )
    if also_generic and transaction_type != TIER_CHANGED_REASON:
        ts2 = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        create_internal_transaction(
            db,
            brand=customer.brand,
            profile_id=customer.profile_id,
            transaction_type=TIER_CHANGED_REASON,
            transaction_id=f"tier_{customer.brand}_{customer.profile_id}_{TIER_CHANGED_REASON}_{ts2}",
            payload=dict(payload),
            depth=depth,
            commit=False,
        )


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
    """Effective palier = max(earned from active-mode thresholds, active guarantees).

    Upgrade is immediate. Descent waits for the status window unless explicitly
    allowed. Settings/tier CRUD must pass refresh_window=False.
    """
    from app.models.customer_loyalty_guarantee import GUARANTEE_BYPASS
    from app.services.loyalty_guarantee_service import (
        ACTIVITY_REASONS,
        active_guarantees,
        cause_from_reason,
        highest_guarantee_floor_key,
    )
    from app.services.purchase_service import (
        QUALIFICATION_PURCHASES,
        get_purchase_balances,
        get_qualification_mode,
    )

    is_mode_switch = reason == QUALIFICATION_MODE_CHANGED_REASON
    mode = get_qualification_mode(db, brand=customer.brand)
    purchase_count = int(getattr(customer, "purchase_count", 0) or 0)
    purchase_amount = int(getattr(customer, "purchase_amount_total", 0) or 0)
    if mode == QUALIFICATION_PURCHASES:
        purchase_count, purchase_amount = get_purchase_balances(db, customer.id)
        customer.purchase_count = purchase_count
        customer.purchase_amount_total = purchase_amount

    earned_status = compute_loyalty_status_from_tiers(
        db,
        customer.brand,
        customer.status_points,
        purchase_count=purchase_count,
        purchase_amount=purchase_amount,
        qualification_mode=mode,
    )
    guarantee_floor = highest_guarantee_floor_key(
        db, customer_id=customer.id, brand=customer.brand
    )
    floor_key = (min_tier_key or "").strip() or guarantee_floor
    new_status = earned_status
    if floor_key:
        floor_rank = _get_tier_rank_value(db, customer.brand, floor_key)
        earned_rank = _get_tier_rank_value(db, customer.brand, new_status) if new_status else None
        if floor_rank is not None and (earned_rank is None or earned_rank < floor_rank):
            new_status = floor_key
            sources = {g.source for g in active_guarantees(db, customer_id=customer.id)}
            if (
                not is_mode_switch
                and reason not in ("ADMIN_OVERRIDE", "ADMIN_SET_TIER")
                and (min_tier_key or GUARANTEE_BYPASS in sources)
            ):
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

    old_rank = _get_tier_rank_value(db, customer.brand, customer.loyalty_status) if customer.loyalty_status else None
    new_rank = _get_tier_rank_value(db, customer.brand, new_status)
    is_downgrade = bool(
        customer.loyalty_status
        and customer.loyalty_status != new_status
        and old_rank is not None
        and new_rank is not None
        and new_rank < old_rank
    )
    is_upgrade = bool(
        customer.loyalty_status != new_status
        and new_rank is not None
        and (old_rank is None or new_rank > old_rank)
    )
    not_expired = bool(
        customer.loyalty_status_expires_at is not None and customer.loyalty_status_expires_at > now
    )
    if is_downgrade and not allow_downgrade_before_expiry and not_expired:
        new_status = customer.loyalty_status
        is_downgrade = False
        is_upgrade = False

    is_base_tier = bool(base_tier_key) and (new_status == base_tier_key)
    status_will_change = customer.loyalty_status != new_status
    is_activity = reason in ACTIVITY_REASONS
    should_refresh_window = False
    if is_base_tier:
        if customer.loyalty_status_expires_at is not None or customer.loyalty_status_assigned_at is None:
            _apply_status_window(customer, now=now, status_days=status_days, is_base_tier=True)
    elif status_will_change and (is_upgrade or is_downgrade):
        should_refresh_window = True
        _apply_status_window(customer, now=now, status_days=status_days, is_base_tier=False)
    elif (
        (not status_will_change)
        and is_activity
        and refresh_window
        and (not is_base_tier)
        and status_days is not None
    ):
        should_refresh_window = True
        _apply_status_window(customer, now=now, status_days=status_days, is_base_tier=False)

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
        status_changed = old_status != new_status
        transaction_type = None
        if is_mode_switch and status_changed:
            transaction_type = QUALIFICATION_MODE_CHANGED_REASON
        elif status_changed and reason == FIRST_PURCHASE_BYPASS_REASON:
            transaction_type = FIRST_PURCHASE_BYPASS_REASON
        elif status_changed and reason in ("ADMIN_OVERRIDE", "ADMIN_SET_TIER"):
            transaction_type = "ADMIN_SET_TIER"
        elif status_changed:
            transaction_type = "TIER_UPGRADED" if is_upgrade else "TIER_DOWNGRADED"
        elif did_refresh_window_without_tier_change:
            transaction_type = "TIER_RENEWED"

        if transaction_type:
            payload = {
                "fromTier": old_status,
                "toTier": new_status,
                "reason": reason,
                "cause": cause_from_reason(reason),
                "earnedTier": earned_status,
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
            _emit_tier_transactions(
                db,
                customer,
                transaction_type=transaction_type,
                payload=payload,
                depth=depth,
                also_generic=status_changed,
            )

    if sync_unomi:
        from app.services.unomi_profile_service import sync_customer_profile_to_unomi

        sync_customer_profile_to_unomi(db, customer=customer, reason="loyalty_status")
    return customer.loyalty_status
