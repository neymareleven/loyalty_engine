"""Purchase-count + amount qualification (parallel to points)."""

from types import SimpleNamespace

from app.services.loyalty_status_service import (
    FIRST_PURCHASE_BYPASS_REASON,
    QUALIFICATION_MODE_CHANGED_REASON,
    _tier_meets_points,
    _tier_meets_purchases,
    _tier_sort_key,
    last_change_from_tier_event,
)
from app.services.purchase_service import (
    QUALIFICATION_PURCHASES,
    extract_purchase_amount,
    first_purchase_bypass_eligible,
    is_purchase_transaction_type,
    normalize_qualification_mode,
    purchase_expires_on,
    serialize_purchase_movement_out,
)


def _tier(**kwargs):
    return SimpleNamespace(
        min_status_points=kwargs.get("min_status_points", 0),
        min_purchase_count=kwargs.get("min_purchase_count", 0),
        min_purchase_amount=kwargs.get("min_purchase_amount", 0),
        rank=kwargs.get("rank", 0),
        key=kwargs.get("key", "base"),
    )


def test_normalize_qualification_mode():
    assert normalize_qualification_mode(None) == "POINTS"
    assert normalize_qualification_mode("points") == "POINTS"
    assert normalize_qualification_mode("PURCHASES") == QUALIFICATION_PURCHASES
    assert normalize_qualification_mode("sales") == QUALIFICATION_PURCHASES


def test_extract_purchase_amount_prefers_order_total():
    assert extract_purchase_amount({"orderTotal": 525, "total": 100}) == 525
    assert extract_purchase_amount({"total": "6 300 CFA"}) == 6300
    assert extract_purchase_amount({"properties": {"orderTotal": 1200}}) == 1200
    assert extract_purchase_amount({"amount": 99}) == 99
    assert extract_purchase_amount({"orderTotal": "1 312 500 CFA"}) == 1312500
    assert extract_purchase_amount({}) == 0
    assert extract_purchase_amount(None) == 0


def test_purchase_expiry_uses_sale_date_not_today():
    from datetime import date, datetime

    assert purchase_expires_on(datetime(2026, 1, 1, 15, 0, 0), 30) == date(2026, 1, 31)
    assert purchase_expires_on(date(2025, 12, 1), 10) == date(2025, 12, 11)
    assert purchase_expires_on(datetime(2026, 1, 1), None) is None


def test_sale_is_purchase_transaction_type():
    assert is_purchase_transaction_type("sale") is True
    assert is_purchase_transaction_type("TIER_UPGRADED") is False


def test_tier_meets_purchases_requires_both_counters():
    gold = _tier(min_purchase_count=5, min_purchase_amount=50000)
    assert _tier_meets_purchases(gold, 5, 50000) is True
    assert _tier_meets_purchases(gold, 5, 49999) is False
    assert _tier_meets_purchases(gold, 4, 50000) is False
    assert _tier_meets_points(_tier(min_status_points=100), 99) is False
    assert _tier_meets_points(_tier(min_status_points=100), 100) is True


def test_tier_sort_key_orders_higher_thresholds_last():
    base = _tier(min_status_points=0, min_purchase_count=0, min_purchase_amount=0)
    silver = _tier(min_status_points=100, min_purchase_count=3, min_purchase_amount=20000)
    assert _tier_sort_key(silver) > _tier_sort_key(base)


def test_last_change_does_not_treat_mode_switch_as_tier_move():
    assert last_change_from_tier_event("TIER_UPGRADED") == "upgrade"
    assert last_change_from_tier_event("TIER_DOWNGRADED") == "downgrade"
    assert last_change_from_tier_event("TIER_RENEWED") == "no_change"
    assert last_change_from_tier_event("ADMIN_SET_TIER") == "admin_override"
    assert last_change_from_tier_event(QUALIFICATION_MODE_CHANGED_REASON) == "mode_changed"
    assert last_change_from_tier_event(FIRST_PURCHASE_BYPASS_REASON) == "first_purchase_bypass"
    assert last_change_from_tier_event("CUSTOMER_REGISTRATION") is None


def test_first_purchase_bypass_eligible_only_first_sale_over_threshold():
    kwargs = dict(enabled=True, min_amount=50000, purchase_amount=50000, tier_key="gold")
    assert first_purchase_bypass_eligible(is_first_purchase=True, **kwargs) is True
    assert first_purchase_bypass_eligible(is_first_purchase=False, **kwargs) is False
    assert first_purchase_bypass_eligible(
        enabled=True, min_amount=50000, purchase_amount=49999, is_first_purchase=True, tier_key="gold"
    ) is False
    assert first_purchase_bypass_eligible(
        enabled=False, min_amount=50000, purchase_amount=50000, is_first_purchase=True, tier_key="gold"
    ) is False
    assert first_purchase_bypass_eligible(
        enabled=True, min_amount=50000, purchase_amount=50000, is_first_purchase=True, tier_key=""
    ) is False
    assert first_purchase_bypass_eligible(
        enabled=True, min_amount=None, purchase_amount=50000, is_first_purchase=True, tier_key="gold"
    ) is False
    # Un client qui a déjà un achat n'est jamais éligible, même au-dessus du seuil.
    assert first_purchase_bypass_eligible(
        enabled=True, min_amount=1, purchase_amount=10_000_000, is_first_purchase=False, tier_key="gold"
    ) is False


def test_serialize_purchase_movement_expire_type():
    from datetime import date, datetime
    from uuid import uuid4

    movement = SimpleNamespace(
        id=uuid4(),
        customer_id=uuid4(),
        quantity=-1,
        amount=-500,
        type="EXPIRE",
        source_transaction_id=None,
        created_at=datetime.utcnow(),
        expires_at=None,
    )
    out = serialize_purchase_movement_out(movement)
    assert out["isExpired"] is True
    assert out["status"] == "expired"
    assert out["quantity"] == -1
    assert out["amount"] == -500


def test_unique_min_status_points_avoids_duplicate_zero():
    from app.routes.loyalty_tiers import unique_min_status_points

    assert unique_min_status_points(set(), 0) == 0
    assert unique_min_status_points({0}, 0) == 1
    assert unique_min_status_points({0, 1, 5}, 1) == 6
    assert unique_min_status_points({0, 100}, 100) == 101
    assert unique_min_status_points({0, 100}, 50) == 50
