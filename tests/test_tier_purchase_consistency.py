from types import SimpleNamespace
from uuid import uuid4

from fastapi import HTTPException

from app.routes.loyalty_tiers import (
    diagnose_tier_consistency,
    purchase_threshold_errors,
    target_mode_threshold_errors,
    _validate_tier_payload,
)
from app.routes.storefront import (
    _tier_brief,
    _wallet_effective_status,
    _wallet_reservation_fields,
    _wallet_standalone_reward,
)


def _rows(*pairs):
    out = []
    for name, count, amount in pairs:
        out.append({"name": name, "key": name.lower(), "count": count, "amount": amount})
    return out


def test_purchase_errors_gold_lower_amount():
    errors = purchase_threshold_errors(_rows(("Argent", 3, 500000), ("Or", 5, 100000)))
    assert errors
    assert "Argent" in errors[0] and "Or" in errors[0]


def test_purchase_ok_increasing_both():
    assert purchase_threshold_errors(_rows(("Argent", 3, 100000), ("Or", 5, 300000))) == []


def test_purchase_ok_same_count_higher_amount():
    assert purchase_threshold_errors(_rows(("Argent", 5, 100000), ("Or", 5, 300000))) == []


def test_purchase_duplicate_thresholds():
    errors = purchase_threshold_errors(_rows(("Argent", 5, 100000), ("Or", 5, 100000)))
    assert errors
    assert "mêmes seuils" in errors[0]


def test_purchase_errors_same_count_lower_amount():
    # Same count + different amounts is ordered by amount after the A2 sort,
    # so (5, 300000) then (5, 100000) is the same valid pair as case 3.
    # The inverse that must be refused is a *higher count* with a *lower amount*.
    errors = purchase_threshold_errors(_rows(("Argent", 5, 300000), ("Or", 6, 100000)))
    assert errors
    assert "montant plus faible" in errors[0]


def _db_with_tiers(tiers, *, mode="PURCHASES"):
    class Q:
        def __init__(self, result=None, rows=None):
            self.result = result
            self.rows = rows if rows is not None else tiers

        def filter(self, *a, **k):
            return self

        def first(self):
            return self.result

        def all(self):
            return list(self.rows)

    class DB:
        def query(self, *a, **k):
            return Q()

    return DB()


def _tier(name, count, amount, *, points=0, active=True, key=None):
    return SimpleNamespace(
        id=uuid4(),
        name=name,
        key=key or name.lower(),
        min_status_points=points,
        min_purchase_count=count,
        min_purchase_amount=amount,
        active=active,
        rank=0,
    )


def test_validate_rejects_incoherent_gold(monkeypatch):
    monkeypatch.setattr(
        "app.routes.loyalty_tiers.get_qualification_mode",
        lambda db, brand: "PURCHASES",
    )
    db = _db_with_tiers(
        [_tier("Base", 0, 0, points=0), _tier("Argent", 3, 500000, points=1)]
    )
    try:
        _validate_tier_payload(
            db=db,
            brand="batira",
            tier_id=None,
            key="or",
            name="Or",
            rank=2,
            min_status_points=2,
            min_purchase_count=5,
            min_purchase_amount=100000,
            active=True,
        )
        assert False, "expected 400"
    except HTTPException as exc:
        assert exc.status_code == 400
        assert "Argent" in str(exc.detail) and "Or" in str(exc.detail)


def test_validate_accepts_coherent_and_same_count(monkeypatch):
    monkeypatch.setattr(
        "app.routes.loyalty_tiers.get_qualification_mode",
        lambda db, brand: "PURCHASES",
    )
    for count, amount in ((5, 300000), (5, 300000)):
        pass
    db = _db_with_tiers(
        [_tier("Base", 0, 0, points=0), _tier("Argent", 3, 100000, points=1)]
    )
    _validate_tier_payload(
        db=db,
        brand="batira",
        tier_id=None,
        key="or",
        name="Or",
        rank=2,
        min_status_points=2,
        min_purchase_count=5,
        min_purchase_amount=300000,
        active=True,
    )
    db = _db_with_tiers(
        [_tier("Base", 0, 0, points=0), _tier("Argent", 5, 100000, points=1)]
    )
    _validate_tier_payload(
        db=db,
        brand="batira",
        tier_id=None,
        key="or",
        name="Or",
        rank=2,
        min_status_points=2,
        min_purchase_count=5,
        min_purchase_amount=300000,
        active=True,
    )


def test_inactive_tier_does_not_block_validation(monkeypatch):
    monkeypatch.setattr(
        "app.routes.loyalty_tiers.get_qualification_mode",
        lambda db, brand: "PURCHASES",
    )
    db = _db_with_tiers(
        [
            _tier("Base", 0, 0, points=0),
            _tier("Vieux", 3, 500000, points=9, active=False),
        ]
    )
    _validate_tier_payload(
        db=db,
        brand="batira",
        tier_id=None,
        key="or",
        name="Or",
        rank=1,
        min_status_points=1,
        min_purchase_count=5,
        min_purchase_amount=100000,
        active=True,
    )


def test_mode_switch_refuses_incoherent_purchases():
    db = _db_with_tiers(
        [
            _tier("Base", 0, 0, points=0),
            _tier("Argent", 3, 500000, points=1),
            _tier("Or", 5, 100000, points=2),
        ]
    )
    errors = target_mode_threshold_errors(db, "batira", "PURCHASES")
    assert errors
    assert any("montant plus faible" in e or "mêmes seuils" in e for e in errors)


def test_consistency_detects_incoherent_and_silent_on_ok():
    bad = _db_with_tiers(
        [
            _tier("Base", 0, 0, points=0),
            _tier("Argent", 3, 500000, points=1),
            _tier("Or", 5, 100000, points=2),
        ]
    )
    out = diagnose_tier_consistency(bad, "batira")
    assert out["ok"] is False
    assert any(p["mode"] == "PURCHASES" for p in out["problems"])

    bad2 = _db_with_tiers(
        [
            _tier("Base", 0, 0, points=0),
            _tier("Argent", 5, 300000, points=1),
            _tier("Or", 6, 100000, points=2),
        ]
    )
    out2 = diagnose_tier_consistency(bad2, "batira")
    assert out2["ok"] is False

    good = _db_with_tiers(
        [
            _tier("Base", 0, 0, points=0),
            _tier("Argent", 3, 100000, points=1),
            _tier("Or", 5, 300000, points=2),
        ]
    )
    assert diagnose_tier_consistency(good, "batira")["ok"] is True
    good2 = _db_with_tiers(
        [
            _tier("Base", 0, 0, points=0),
            _tier("Argent", 5, 100000, points=1),
            _tier("Or", 5, 300000, points=2),
        ]
    )
    assert diagnose_tier_consistency(good2, "batira")["ok"] is True


def test_points_mode_still_requires_strictly_increasing(monkeypatch):
    monkeypatch.setattr(
        "app.routes.loyalty_tiers.get_qualification_mode",
        lambda db, brand: "POINTS",
    )
    db = _db_with_tiers([_tier("Base", 0, 0, points=0), _tier("Argent", 0, 0, points=100)])
    try:
        _validate_tier_payload(
            db=db,
            brand="batira",
            tier_id=None,
            key="or",
            name="Or",
            rank=2,
            min_status_points=100,
            min_purchase_count=0,
            min_purchase_amount=0,
            active=True,
        )
        assert False, "expected 400"
    except HTTPException as exc:
        assert exc.status_code == 400
        assert "status points" in str(exc.detail)
    _validate_tier_payload(
        db=db,
        brand="batira",
        tier_id=None,
        key="or",
        name="Or",
        rank=2,
        min_status_points=200,
        min_purchase_count=0,
        min_purchase_amount=0,
        active=True,
    )


def test_tier_brief_includes_rank():
    tier = SimpleNamespace(key="or", name="Or", rank=2)
    assert _tier_brief(tier)["rank"] == 2


def test_wallet_effective_status_expired_issued():
    from datetime import datetime, timedelta

    now = datetime.utcnow()
    row = SimpleNamespace(status="ISSUED", expires_at=now - timedelta(days=1))
    assert _wallet_effective_status(row, now) == "EXPIRED"
    live = SimpleNamespace(
        status="RESERVED",
        expires_at=now - timedelta(days=1),
        reserved_until=now + timedelta(hours=1),
    )
    assert _wallet_effective_status(live, now) == "RESERVED"
    used = SimpleNamespace(status="USED", expires_at=None)
    assert _wallet_effective_status(used, now) == "USED"


def test_wallet_effective_status_stale_reservation():
    from datetime import datetime, timedelta

    now = datetime.utcnow()
    stale_empty = SimpleNamespace(status="RESERVED", expires_at=now + timedelta(days=1), reserved_until=None)
    assert _wallet_effective_status(stale_empty, now) == "ISSUED"
    stale_past = SimpleNamespace(
        status="RESERVED",
        expires_at=now + timedelta(days=1),
        reserved_until=now - timedelta(minutes=1),
    )
    assert _wallet_effective_status(stale_past, now) == "ISSUED"
    stale_and_expired = SimpleNamespace(
        status="RESERVED",
        expires_at=now - timedelta(days=1),
        reserved_until=now - timedelta(minutes=1),
    )
    assert _wallet_effective_status(stale_and_expired, now) == "EXPIRED"


def test_wallet_reservation_fields_only_when_active():
    from datetime import datetime, timedelta
    from uuid import uuid4

    now = datetime.utcnow()
    rid = uuid4()
    until = now + timedelta(minutes=20)
    active = SimpleNamespace(
        id=uuid4(),
        status="RESERVED",
        expires_at=None,
        reserved_until=until,
        reserved_by_redemption_id=rid,
    )
    fields = _wallet_reservation_fields(active, now, {})
    assert fields["reservedRedemptionId"] == str(rid)
    assert fields["reservedUntil"] == until
    stale = SimpleNamespace(
        id=uuid4(),
        status="RESERVED",
        expires_at=None,
        reserved_until=now - timedelta(minutes=1),
        reserved_by_redemption_id=rid,
    )
    assert _wallet_reservation_fields(stale, now, {}) == {}


def test_wallet_standalone_reward_stale_vs_active_reservation():
    from datetime import datetime, timedelta
    from uuid import uuid4

    now = datetime.utcnow()
    rid = uuid4()
    until = now + timedelta(minutes=10)
    payload = {"name": "Gift", "benefit": {"benefitType": "PERCENT_DISCOUNT", "benefitValue": 10}}
    active = SimpleNamespace(
        id=uuid4(),
        reward_id=None,
        payload=payload,
        status="RESERVED",
        expires_at=None,
        used_at=None,
        reserved_until=until,
        reserved_by_redemption_id=rid,
    )
    presented = _wallet_standalone_reward(active, {}, now=now)
    assert presented["status"] == "RESERVED"
    assert presented["reservedRedemptionId"] == str(rid)
    assert presented["reservedUntil"] == until
    stale = SimpleNamespace(
        id=uuid4(),
        reward_id=None,
        payload=payload,
        status="RESERVED",
        expires_at=None,
        used_at=None,
        reserved_until=now - timedelta(minutes=1),
        reserved_by_redemption_id=rid,
    )
    presented_stale = _wallet_standalone_reward(stale, {}, now=now)
    assert presented_stale["status"] == "ISSUED"
    assert "reservedRedemptionId" not in presented_stale
    assert "reservedUntil" not in presented_stale


def test_wallet_reservation_fields_uses_latest_redemption():
    from datetime import datetime, timedelta

    now = datetime.utcnow()
    until = now + timedelta(minutes=5)
    rid = uuid4()
    row = SimpleNamespace(
        id=uuid4(),
        status="RESERVED",
        expires_at=None,
        reserved_until=until,
        reserved_by_redemption_id=None,
    )
    fields = _wallet_reservation_fields(
        row,
        now,
        {str(row.id): SimpleNamespace(id=rid, reserved_until=until)},
    )
    assert fields["reservedRedemptionId"] == str(rid)
    assert fields["reservedUntil"] == until


def test_wallet_coupon_nested_reward_reservation(monkeypatch):
    from datetime import datetime, timedelta

    from app.routes.storefront import _wallet_coupon

    now = datetime.utcnow()
    until = now + timedelta(minutes=8)
    coupon_id = uuid4()
    cr_id = uuid4()
    rid = uuid4()
    coupon = SimpleNamespace(
        id=coupon_id,
        coupon_type_id=None,
        payload=None,
        code="LOY-TEST",
        status="RESERVED",
        expires_at=None,
        used_at=None,
        shareable=False,
        reserved_until=until,
        reserved_by_redemption_id=rid,
    )
    nested = SimpleNamespace(
        id=cr_id,
        status="RESERVED",
        expires_at=None,
        reserved_until=until,
        reserved_by_redemption_id=rid,
    )

    class Q:
        def filter(self, *a, **k):
            return self

        def first(self):
            return None

        def all(self):
            return [nested]

    class DB:
        def query(self, model):
            return Q()

    monkeypatch.setattr(
        "app.routes.storefront.coupon_reward_snapshots",
        lambda db, coupon: [
            {
                "customerRewardId": str(cr_id),
                "name": "Offre",
                "benefitType": "PERCENT_DISCOUNT",
                "benefitValue": 10,
            }
        ],
    )
    item = _wallet_coupon(DB(), coupon, now=now)
    assert item["status"] == "RESERVED"
    assert item["reservedRedemptionId"] == str(rid)
    assert item["rewards"][0]["status"] == "RESERVED"
    assert item["rewards"][0]["reservedRedemptionId"] == str(rid)

    coupon.reserved_until = now - timedelta(minutes=1)
    nested.reserved_until = now - timedelta(minutes=1)
    stale_item = _wallet_coupon(DB(), coupon, now=now)
    assert stale_item["status"] == "ISSUED"
    assert "reservedRedemptionId" not in stale_item
    assert stale_item["rewards"][0]["status"] == "ISSUED"
    assert "reservedRedemptionId" not in stale_item["rewards"][0]
