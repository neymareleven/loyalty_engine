from types import SimpleNamespace
from uuid import uuid4

from fastapi import HTTPException

from app.routes.loyalty_tiers import (
    diagnose_tier_consistency,
    purchase_threshold_errors,
    target_mode_threshold_errors,
    _validate_tier_payload,
)
from app.routes.storefront import _tier_brief, _wallet_effective_status


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
    live = SimpleNamespace(status="RESERVED", expires_at=now - timedelta(days=1))
    assert _wallet_effective_status(live, now) == "RESERVED"
    used = SimpleNamespace(status="USED", expires_at=None)
    assert _wallet_effective_status(used, now) == "USED"
