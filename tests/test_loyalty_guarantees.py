from datetime import datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

from app.models.customer import Customer
from app.models.customer_loyalty_guarantee import (
    GUARANTEE_ADMIN,
    GUARANTEE_BYPASS,
    GUARANTEE_MODE_HOLD,
    CustomerLoyaltyGuarantee,
)
from app.services.loyalty_guarantee_service import (
    cleanup_loyalty_guarantees,
    grant_loyalty_guarantee,
    serialize_guarantees,
)
from app.services.loyalty_status_service import update_customer_status


RANKS = {"bronze": 1, "or": 2, "platine": 3}


class _Q:
    def __init__(self, rows=None, first=None):
        self.rows = list(rows or [])
        self._first = first

    def filter(self, *a, **k):
        return self

    def first(self):
        if self._first is not None:
            return self._first
        return self.rows[0] if self.rows else None

    def all(self):
        return list(self.rows)


class _DB:
    def __init__(self, *, guarantees=None, customers=None):
        self.guarantees = list(guarantees or [])
        self.customers = {c.id: c for c in (customers or [])}
        self.flushed = 0

    def query(self, model):
        if model is CustomerLoyaltyGuarantee:
            return _Q([g for g in self.guarantees if g.revoked_at is None])
        if model is Customer:
            return _Q(list(self.customers.values()), first=next(iter(self.customers.values()), None))
        return _Q([])

    def add(self, row):
        self.guarantees.append(row)

    def flush(self):
        self.flushed += 1


def _guarantee(*, customer_id, source=GUARANTEE_BYPASS, floor="or", expires_at=None, revoked_at=None):
    return SimpleNamespace(
        id=uuid4(),
        customer_id=customer_id,
        brand="batira",
        source=source,
        floor_tier_key=floor,
        expires_at=expires_at,
        created_at=datetime(2026, 1, 1),
        revoked_at=revoked_at,
    )


def _customer(**kwargs):
    now = datetime.utcnow()
    data = dict(
        id=uuid4(),
        brand="batira",
        profile_id="wp_1",
        loyalty_status="or",
        status_points=0,
        purchase_count=0,
        purchase_amount_total=0,
        loyalty_status_assigned_at=now - timedelta(days=10),
        loyalty_status_expires_at=now + timedelta(days=355),
    )
    data.update(kwargs)
    return SimpleNamespace(**data)


def _patch_status(monkeypatch, *, earned, events, mode="POINTS"):
    monkeypatch.setattr(
        "app.services.loyalty_status_service.compute_loyalty_status_from_tiers",
        lambda *a, **k: earned,
    )
    monkeypatch.setattr(
        "app.services.loyalty_status_service._get_tier_rank_value",
        lambda db, brand, key: RANKS.get(key),
    )
    monkeypatch.setattr(
        "app.services.loyalty_status_service._get_base_tier_key",
        lambda db, brand: "bronze",
    )
    monkeypatch.setattr(
        "app.services.loyalty_status_service.get_loyalty_settings",
        lambda db, brand: SimpleNamespace(loyalty_status_validity_days=365),
    )
    monkeypatch.setattr(
        "app.services.purchase_service.get_qualification_mode",
        lambda db, brand: mode,
    )
    monkeypatch.setattr(
        "app.services.purchase_service.get_purchase_balances",
        lambda db, cid: (0, 0),
    )

    def _emit(db, customer, *, transaction_type, payload, depth, also_generic):
        events.append(transaction_type)

    monkeypatch.setattr("app.services.loyalty_status_service._emit_tier_transactions", _emit)


def test_grant_does_not_extend_independent_expiry(monkeypatch):
    monkeypatch.setattr(
        "app.services.loyalty_status_service._get_tier_rank_value",
        lambda db, brand, key: RANKS.get(key),
    )
    customer = _customer()
    existing = _guarantee(
        customer_id=customer.id,
        expires_at=datetime(2026, 6, 1),
    )
    db = _DB(guarantees=[existing])
    later = datetime(2027, 11, 19)
    grant_loyalty_guarantee(
        db,
        customer=customer,
        source=GUARANTEE_BYPASS,
        floor_tier_key="or",
        expires_at=later,
    )
    assert existing.expires_at == datetime(2026, 6, 1)


def test_bypass_revoked_when_or_is_earned(monkeypatch):
    events = []
    _patch_status(monkeypatch, earned="or", events=events)
    customer = _customer(status_points=100)
    g = _guarantee(customer_id=customer.id, expires_at=datetime(2027, 11, 19))
    db = _DB(guarantees=[g])
    expires_before = customer.loyalty_status_expires_at
    update_customer_status(db, customer, reason="PURCHASE_RECORDED", refresh_window=False)
    assert g.revoked_at is not None
    assert serialize_guarantees(db, customer_id=customer.id) == []
    assert customer.loyalty_status == "or"
    assert customer.loyalty_status_expires_at == expires_before
    assert "TIER_RENEWED" not in events


def test_bypass_then_platine_revokes_and_uses_status_window(monkeypatch):
    events = []
    _patch_status(monkeypatch, earned="platine", events=events)
    customer = _customer(loyalty_status="or", status_points=500)
    g = _guarantee(customer_id=customer.id, expires_at=datetime(2027, 11, 19))
    db = _DB(guarantees=[g])
    update_customer_status(db, customer, reason="EARN_POINTS", refresh_window=False)
    assert g.revoked_at is not None
    assert customer.loyalty_status == "platine"
    assert "TIER_UPGRADED" in events
    assert g.expires_at == datetime(2027, 11, 19) or g.revoked_at is not None

    events.clear()
    _patch_status(monkeypatch, earned="bronze", events=events)
    expired = datetime.utcnow() - timedelta(days=1)
    customer.loyalty_status_expires_at = expired
    update_customer_status(
        db,
        customer,
        reason="LOYALTY_STATUS_EXPIRED",
        refresh_window=True,
    )
    assert customer.loyalty_status == "bronze"
    assert customer.loyalty_status_expires_at is None
    assert "TIER_DOWNGRADED" in events


def test_bypass_holds_until_status_window_ends(monkeypatch):
    events = []
    _patch_status(monkeypatch, earned="bronze", events=events, mode="PURCHASES")
    future = datetime.utcnow() + timedelta(days=200)
    customer = _customer(loyalty_status="or", loyalty_status_expires_at=future)
    g = _guarantee(customer_id=customer.id, expires_at=datetime(2027, 1, 1))
    db = _DB(guarantees=[g])
    update_customer_status(db, customer, reason="PURCHASE_RECORDED", refresh_window=False)
    assert g.revoked_at is None
    assert customer.loyalty_status == "or"
    assert g.expires_at == customer.loyalty_status_expires_at
    assert events == []

    events.clear()
    customer.loyalty_status_expires_at = datetime.utcnow() - timedelta(hours=1)
    g.expires_at = customer.loyalty_status_expires_at
    update_customer_status(db, customer, reason="LOYALTY_STATUS_EXPIRED", refresh_window=True)
    assert customer.loyalty_status == "bronze"
    assert g.revoked_at is not None
    assert serialize_guarantees(db, customer_id=customer.id) == []


def test_renewal_aligns_guarantee_expiry(monkeypatch):
    events = []
    _patch_status(monkeypatch, earned="bronze", events=events)
    old_exp = datetime.utcnow() + timedelta(days=10)
    customer = _customer(loyalty_status="or", loyalty_status_expires_at=old_exp)
    g = _guarantee(customer_id=customer.id, expires_at=old_exp)
    db = _DB(guarantees=[g])
    update_customer_status(
        db,
        customer,
        reason="ADMIN_SET_TIER",
        refresh_window=True,
        allow_downgrade_before_expiry=True,
        min_tier_key="or",
    )
    assert customer.loyalty_status == "or"
    assert "TIER_RENEWED" in events
    assert g.expires_at == customer.loyalty_status_expires_at
    assert g.expires_at != old_exp


def test_admin_guarantee_revoked_when_earned(monkeypatch):
    events = []
    _patch_status(monkeypatch, earned="or", events=events)
    customer = _customer(status_points=120)
    g = _guarantee(customer_id=customer.id, source=GUARANTEE_ADMIN, floor="or")
    db = _DB(guarantees=[g])
    update_customer_status(db, customer, reason="ADMIN_SET_TIER", refresh_window=True)
    assert g.revoked_at is not None
    assert serialize_guarantees(db, customer_id=customer.id) == []


def test_mode_hold_guarantee_revoked_when_earned(monkeypatch):
    events = []
    _patch_status(monkeypatch, earned="or", events=events)
    customer = _customer(status_points=120)
    g = _guarantee(customer_id=customer.id, source=GUARANTEE_MODE_HOLD, floor="or")
    db = _DB(guarantees=[g])
    update_customer_status(db, customer, reason="QUALIFICATION_MODE_CHANGED")
    assert g.revoked_at is not None


def test_two_guarantees_highest_floor_and_partial_revoke(monkeypatch):
    events = []
    _patch_status(monkeypatch, earned="or", events=events)
    customer = _customer(loyalty_status="platine", status_points=100)
    gold = _guarantee(customer_id=customer.id, source=GUARANTEE_BYPASS, floor="or")
    plat = _guarantee(customer_id=customer.id, source=GUARANTEE_ADMIN, floor="platine")
    db = _DB(guarantees=[gold, plat])
    future = datetime.utcnow() + timedelta(days=100)
    customer.loyalty_status_expires_at = future
    update_customer_status(db, customer, reason="EARN_POINTS")
    assert gold.revoked_at is not None
    assert plat.revoked_at is None
    assert customer.loyalty_status == "platine"
    assert plat.expires_at == customer.loyalty_status_expires_at


def test_cleanup_revokes_earned_or_and_is_idempotent(monkeypatch):
    monkeypatch.setattr(
        "app.services.loyalty_status_service.compute_loyalty_status_from_tiers",
        lambda *a, **k: "platine",
    )
    monkeypatch.setattr(
        "app.services.loyalty_status_service._get_tier_rank_value",
        lambda db, brand, key: RANKS.get(key),
    )
    monkeypatch.setattr(
        "app.services.purchase_service.get_qualification_mode",
        lambda db, brand: "POINTS",
    )
    customer = _customer(loyalty_status="platine", status_points=500)
    g = _guarantee(customer_id=customer.id, expires_at=datetime(2027, 11, 19))
    db = _DB(guarantees=[g], customers=[customer])
    first = cleanup_loyalty_guarantees(db)
    assert first["revoked"] == 1
    assert g.revoked_at is not None
    second = cleanup_loyalty_guarantees(db)
    assert second["revoked"] == 0
    assert second == {"revoked": 0, "aligned": 0, "skipped": 0}


def test_status_expiry_renews_when_still_qualified(monkeypatch):
    events = []
    _patch_status(monkeypatch, earned="or", events=events)
    past = datetime.utcnow() - timedelta(hours=2)
    customer = _customer(loyalty_status="or", status_points=150, loyalty_status_expires_at=past)
    db = _DB()
    update_customer_status(db, customer, reason="LOYALTY_STATUS_EXPIRED", refresh_window=True)
    assert customer.loyalty_status == "or"
    assert "TIER_RENEWED" in events
    assert customer.loyalty_status_expires_at > datetime.utcnow()


def test_status_expiry_downgrades_when_not_qualified(monkeypatch):
    events = []
    _patch_status(monkeypatch, earned="bronze", events=events, mode="PURCHASES")
    past = datetime.utcnow() - timedelta(hours=2)
    customer = _customer(loyalty_status="or", loyalty_status_expires_at=past)
    g = _guarantee(customer_id=customer.id, expires_at=past)
    db = _DB(guarantees=[g])
    update_customer_status(db, customer, reason="LOYALTY_STATUS_EXPIRED", refresh_window=True)
    assert customer.loyalty_status == "bronze"
    assert "TIER_DOWNGRADED" in events
    assert customer.loyalty_status_expires_at is None
    assert g.revoked_at is not None
    assert serialize_guarantees(db, customer_id=customer.id) == []


def test_purchase_does_not_renew_window(monkeypatch):
    events = []
    _patch_status(monkeypatch, earned="or", events=events)
    exp = datetime.utcnow() + timedelta(days=40)
    customer = _customer(loyalty_status="or", status_points=200, loyalty_status_expires_at=exp)
    db = _DB()
    update_customer_status(db, customer, reason="PURCHASE_RECORDED", refresh_window=False)
    assert customer.loyalty_status_expires_at == exp
    assert events == []


def test_base_tier_has_no_expiry_event(monkeypatch):
    events = []
    _patch_status(monkeypatch, earned="bronze", events=events)
    customer = _customer(loyalty_status="bronze", loyalty_status_expires_at=None)
    db = _DB()
    update_customer_status(db, customer, reason="LOYALTY_STATUS_EXPIRED", refresh_window=True)
    assert customer.loyalty_status == "bronze"
    assert customer.loyalty_status_expires_at is None
    assert events == []


def _patch_cleanup(monkeypatch, *, earned="bronze"):
    monkeypatch.setattr(
        "app.services.loyalty_status_service.compute_loyalty_status_from_tiers",
        lambda *a, **k: earned,
    )
    monkeypatch.setattr(
        "app.services.loyalty_status_service._get_tier_rank_value",
        lambda db, brand, key: RANKS.get(key),
    )
    monkeypatch.setattr(
        "app.services.loyalty_status_service._get_base_tier_key",
        lambda db, brand: "bronze",
    )
    monkeypatch.setattr(
        "app.services.purchase_service.get_qualification_mode",
        lambda db, brand: "PURCHASES",
    )
    monkeypatch.setattr(
        "app.services.purchase_service.get_purchase_balances",
        lambda db, cid: (0, 0),
    )


def test_cleanup_expired_guarantee_on_bronze_is_revoked_not_open_ended(monkeypatch):
    _patch_cleanup(monkeypatch, earned="bronze")
    past = datetime.utcnow() - timedelta(days=1)
    customer = _customer(
        loyalty_status="bronze",
        loyalty_status_expires_at=None,
        status_points=0,
        purchase_count=0,
    )
    g = _guarantee(customer_id=customer.id, expires_at=past)
    db = _DB(guarantees=[g], customers=[customer])
    stats = cleanup_loyalty_guarantees(db)
    assert stats["revoked"] == 1
    assert g.revoked_at is not None
    assert g.expires_at == past
    assert serialize_guarantees(db, customer_id=customer.id) == []

    ghost = _guarantee(customer_id=customer.id, expires_at=None)
    db2 = _DB(guarantees=[ghost], customers=[customer])
    cleanup_loyalty_guarantees(db2)
    assert ghost.revoked_at is not None
    assert ghost.expires_at is None
    assert serialize_guarantees(db2, customer_id=customer.id) == []


def test_invariant_revokes_or_floor_while_customer_is_bronze(monkeypatch):
    events = []
    _patch_status(monkeypatch, earned="bronze", events=events, mode="PURCHASES")
    customer = _customer(loyalty_status="bronze", loyalty_status_expires_at=None)
    g = _guarantee(
        customer_id=customer.id,
        expires_at=datetime.utcnow() + timedelta(days=200),
    )
    db = _DB(guarantees=[g])
    update_customer_status(db, customer, reason="PURCHASE_RECORDED", refresh_window=False)
    assert customer.loyalty_status == "bronze"
    assert g.revoked_at is not None
    assert serialize_guarantees(db, customer_id=customer.id) == []
    assert "TIER_UPGRADED" not in events
    assert "FIRST_PURCHASE_BYPASS" not in events

    _patch_cleanup(monkeypatch, earned="bronze")
    live = _guarantee(
        customer_id=customer.id,
        expires_at=datetime.utcnow() + timedelta(days=200),
    )
    db_cleanup = _DB(guarantees=[live], customers=[customer])
    cleanup_loyalty_guarantees(db_cleanup)
    assert live.revoked_at is not None
    assert customer.loyalty_status == "bronze"


def test_valid_or_hold_is_unchanged_and_aligned(monkeypatch):
    events = []
    _patch_status(monkeypatch, earned="bronze", events=events, mode="PURCHASES")
    future = datetime.utcnow() + timedelta(days=180)
    customer = _customer(loyalty_status="or", loyalty_status_expires_at=future)
    g = _guarantee(customer_id=customer.id, expires_at=datetime(2027, 6, 1))
    db = _DB(guarantees=[g])
    update_customer_status(db, customer, reason="PURCHASE_RECORDED", refresh_window=False)
    assert customer.loyalty_status == "or"
    assert g.revoked_at is None
    assert g.expires_at == future
    payload = serialize_guarantees(db, customer_id=customer.id)
    assert len(payload) == 1
    assert payload[0]["floorTierKey"] == "or"
    assert payload[0]["expiresAt"] == future
