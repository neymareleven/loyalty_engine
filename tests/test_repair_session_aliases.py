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
