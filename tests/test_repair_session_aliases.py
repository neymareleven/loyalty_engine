"""Unit tests for repair_session_aliases audit classification helpers."""

from __future__ import annotations

import importlib.util
import os
import sys
import uuid
from datetime import date
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _load_repair_module() -> ModuleType:
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "scripts",
        "repair_session_aliases.py",
    )
    spec = importlib.util.spec_from_file_location("repair_session_aliases", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["repair_session_aliases"] = mod
    spec.loader.exec_module(mod)
    return mod


repair = _load_repair_module()


def test_correction_note_format():
    note = repair._correction_note(order_number="7026", audit_date=date(2026, 7, 10))
    assert note == "correction alias erroné - order 7026 - voir audit du 2026-07-10"


def test_audit_suspicious_alias_empty_when_no_conflicting_sales():
    db = MagicMock()
    alias_query = MagicMock()
    alias_query.filter.return_value = alias_query
    alias_query.all.return_value = []
    db.query.return_value = alias_query

    assert repair.audit_suspicious_session_aliases(db, brand="batira") == []


def test_audit_misattributed_requires_positive_net_points(monkeypatch):
    db = MagicMock()
    wrong_id = uuid.uuid4()
    right_id = uuid.uuid4()
    tx_uuid = uuid.uuid4()

    wrong = SimpleNamespace(
        id=wrong_id,
        email="wrong@example.com",
        profile_id="master-wrong",
    )
    right = SimpleNamespace(
        id=right_id,
        email="right@example.com",
        profile_id="master-right",
    )
    tx = SimpleNamespace(
        id=tx_uuid,
        brand="batira",
        transaction_id="sale-7026",
        profile_id="alias-on-wrong",
        status="PROCESSED",
        payload={"orderNumber": "7026", "billing_email": "right@example.com"},
    )

    tx_query = MagicMock()
    tx_query.filter.return_value = tx_query
    tx_query.order_by.return_value = tx_query
    tx_query.all.return_value = [tx]
    db.query.return_value = tx_query

    monkeypatch.setattr(repair, "get_customer", lambda _db, _brand, _pid: wrong)
    monkeypatch.setattr(repair, "_customer_by_email", lambda _db, *, brand, email: right)
    monkeypatch.setattr(repair, "_net_points_for_transaction", lambda *a, **k: 2)
    monkeypatch.setattr(repair, "_earn_expires_at_for_transaction", lambda *a, **k: date(2027, 1, 1))
    monkeypatch.setattr(repair, "_correction_already_applied", lambda *a, **k: False)
    monkeypatch.setattr(repair, "_is_alias_profile_for_customer", lambda *a, **k: True)

    reprocess, mis = repair.audit_sale_transactions(db, brand="batira", order_number="7026")
    assert reprocess == []
    assert len(mis) == 1
    assert mis[0].points_on_wrong_customer == 2
    assert mis[0].target_customer_id == right_id


def test_audit_skips_processed_when_profile_and_email_agree(monkeypatch):
    db = MagicMock()
    customer_id = uuid.uuid4()
    customer = SimpleNamespace(
        id=customer_id,
        email="same@example.com",
        profile_id="master",
    )
    tx = SimpleNamespace(
        id=uuid.uuid4(),
        brand="batira",
        transaction_id="sale-1",
        profile_id="master",
        status="PROCESSED",
        payload={"orderNumber": "1", "billing_email": "same@example.com"},
    )

    tx_query = MagicMock()
    tx_query.filter.return_value = tx_query
    tx_query.order_by.return_value = tx_query
    tx_query.all.return_value = [tx]
    db.query.return_value = tx_query

    monkeypatch.setattr(repair, "get_customer", lambda _db, _brand, _pid: customer)
    monkeypatch.setattr(repair, "_customer_by_email", lambda _db, *, brand, email: customer)

    reprocess, mis = repair.audit_sale_transactions(db, brand="batira")
    assert reprocess == []
    assert mis == []


def test_classify_disposable_domain():
    cls = repair._classify_billing_email(
        "nanaco7131@hacknapp.com",
        disposable_domains=repair.DEFAULT_DISPOSABLE_DOMAINS,
    )
    assert cls == "disposable"


def test_classify_test_pattern():
    cls = repair._classify_billing_email(
        "test17@gmail.com",
        disposable_domains=repair.DEFAULT_DISPOSABLE_DOMAINS,
    )
    assert cls == "test_pattern"


def test_classify_likely_real():
    cls = repair._classify_billing_email(
        "client@entreprise.fr",
        disposable_domains=repair.DEFAULT_DISPOSABLE_DOMAINS,
    )
    assert cls == "likely_real"


def test_audit_masked_alias_ingest_detected_with_include_masked(monkeypatch):
    """Email overwrite masks mismatch: same customer id, but alias ingest flags robust [C]."""
    db = MagicMock()
    customer_id = uuid.uuid4()
    tx_uuid = uuid.uuid4()
    customer = SimpleNamespace(
        id=customer_id,
        email="test17@gmail.com",
        profile_id="master-wrong",
    )
    tx = SimpleNamespace(
        id=tx_uuid,
        brand="batira",
        transaction_id="sale-7026",
        profile_id="alias-session",
        status="PROCESSED",
        payload={"orderNumber": "7026", "billing_email": "test17@gmail.com"},
    )

    tx_query = MagicMock()
    tx_query.filter.return_value = tx_query
    tx_query.order_by.return_value = tx_query
    tx_query.all.return_value = [tx]
    db.query.return_value = tx_query

    monkeypatch.setattr(repair, "get_customer", lambda _db, _brand, _pid: customer)
    monkeypatch.setattr(repair, "_customer_by_email", lambda _db, *, brand, email: customer)
    monkeypatch.setattr(repair, "_is_alias_profile_for_customer", lambda *a, **k: True)
    monkeypatch.setattr(repair, "_net_points_for_transaction", lambda *a, **k: 2)
    monkeypatch.setattr(repair, "_earn_expires_at_for_transaction", lambda *a, **k: date(2027, 1, 1))
    monkeypatch.setattr(repair, "_correction_already_applied", lambda *a, **k: False)

    _, naive = repair.audit_sale_transactions(db, brand="batira", order_number="7026")
    _, robust = repair.audit_sale_transactions(
        db, brand="batira", order_number="7026", include_masked=True
    )
    assert naive == []
    assert len(robust) == 1
    assert robust[0].detection == "masked_alias_ingest"
    assert robust[0].email_overwrite_masked is True


def test_force_transfer_refuses_when_correction_exists(monkeypatch):
    db = MagicMock()
    sale_uuid = uuid.uuid4()
    monkeypatch.setattr(
        repair,
        "_find_sale_transaction",
        lambda *a, **k: SimpleNamespace(
            id=sale_uuid,
            transaction_id="sale-7026",
            status="PROCESSED",
            profile_id="alias",
            payload={"orderNumber": "7026"},
        ),
    )
    monkeypatch.setattr(repair, "_correction_already_applied_for_sale", lambda *a, **k: True)

    ok = repair.force_transfer_points(
        db,
        brand="batira",
        from_customer_id=uuid.uuid4(),
        to_customer_id=uuid.uuid4(),
        order_number="7026",
        commit=False,
    )
    assert ok is False


def test_force_transfer_uses_net_points_on_source(monkeypatch):
    db = MagicMock()
    from_id = uuid.uuid4()
    to_id = uuid.uuid4()
    sale_uuid = uuid.uuid4()
    wrong = SimpleNamespace(id=from_id, brand="batira", profile_id="6b8555e2", email="test@gmail.com")
    target = SimpleNamespace(id=to_id, brand="batira", profile_id="87a759c2", email="test17@gmail.com")
    sale_tx = SimpleNamespace(
        id=sale_uuid,
        transaction_id="sale-7026",
        status="PROCESSED",
        profile_id="87a759c2",
        payload={"orderNumber": "7026", "billing_email": "test17@gmail.com"},
    )
    movement = SimpleNamespace(
        id=uuid.uuid4(),
        points=15000,
        type="EARN",
        expires_at=date(2027, 7, 9),
    )

    customer_query = MagicMock()
    customer_query.filter.return_value = customer_query
    customer_query.first.side_effect = [wrong, target]

    monkeypatch.setattr(repair, "_find_sale_transaction", lambda *a, **k: sale_tx)
    monkeypatch.setattr(repair, "_correction_already_applied_for_sale", lambda *a, **k: False)
    monkeypatch.setattr(repair, "_point_movements_for_sale_on_customer", lambda *a, **k: [movement])
    monkeypatch.setattr(db, "query", lambda *a, **k: customer_query)

    captured = {}

    def fake_execute(*args, **kwargs):
        captured.update(kwargs)
        return True

    monkeypatch.setattr(repair, "_execute_point_transfer", fake_execute)

    ok = repair.force_transfer_points(
        db,
        brand="batira",
        from_customer_id=from_id,
        to_customer_id=to_id,
        order_number="7026",
        commit=False,
    )
    assert ok is True
    assert captured["amount"] == 15000
    assert captured["mode"] == "force_transfer"
    assert captured["wrong"] is wrong
    assert captured["target"] is target
