"""PURCHASE_RECORDED event + rule-condition catalog for purchase coupons."""

from types import SimpleNamespace
from uuid import uuid4

from app.services.payload_schema_service import get_transaction_type_rule_hints
from app.services.purchase_service import emit_purchase_recorded_event, PURCHASE_RECORDED_TYPE
from app.services.rule_condition_fields import (
    extra_payload_condition_fields,
    extra_payload_field_meta,
    purchase_customer_field_meta,
)
from app.services.transaction_protection import SYSTEM_MANAGED_TRANSACTION_TYPE_KEYS


def test_purchase_recorded_is_system_managed_type():
    assert "PURCHASE_RECORDED" in SYSTEM_MANAGED_TRANSACTION_TYPE_KEYS


def test_catalog_exposes_purchase_customer_fields():
    meta = purchase_customer_field_meta()
    assert "customer.purchase_count" in meta
    assert "customer.purchase_amount_total" in meta
    assert meta["customer.purchase_count"]["valueKind"] == "number"


def test_catalog_purchase_recorded_payload_fields():
    fields = extra_payload_condition_fields("PURCHASE_RECORDED")
    assert "payload.orderAmount" in fields
    assert "payload.purchaseCount" in fields
    assert "payload.purchaseAmountTotal" in fields
    assert "payload.isFirstPurchase" in fields


def test_catalog_tier_changed_cause_options():
    fields = extra_payload_condition_fields("TIER_CHANGED")
    assert "payload.cause" in fields
    assert "payload.fromTier" in fields
    assert "payload.toTier" in fields
    meta = extra_payload_field_meta("TIER_CHANGED")
    assert meta["payload.cause"]["ui"]["options"] == [
        "thresholds",
        "first_purchase_bypass",
        "mode_switch",
        "admin",
        "expiration",
    ]


def test_rule_hints_discourage_coupons_on_mode_switch():
    hints = get_transaction_type_rule_hints("QUALIFICATION_MODE_CHANGED")
    assert hints is not None
    assert "issue_coupon" in hints.get("discourageActions", [])
    recorded = get_transaction_type_rule_hints("PURCHASE_RECORDED")
    assert recorded is not None
    assert "payload.purchaseCount" in recorded["preferredCountPath"]


def test_emit_purchase_recorded_is_idempotent_by_sale_id(monkeypatch):
    captured = {}

    def fake_create(db, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(id=uuid4(), transaction_id=kwargs["transaction_id"])

    monkeypatch.setattr(
        "app.services.transaction_service.create_internal_transaction",
        fake_create,
    )
    monkeypatch.setattr(
        "app.services.purchase_service.get_qualification_mode",
        lambda db, brand: "PURCHASES",
    )

    sale_id = uuid4()
    customer = SimpleNamespace(brand="batira", profile_id="p1", loyalty_status="argent")
    sale = SimpleNamespace(id=sale_id)

    emit_purchase_recorded_event(
        db=None,
        customer=customer,
        source_transaction=sale,
        order_amount=25000,
        purchase_count=3,
        purchase_amount_total=140000,
        previous_purchase_count=2,
        previous_purchase_amount_total=115000,
        is_first_purchase=False,
        depth=0,
    )

    assert captured["transaction_type"] == PURCHASE_RECORDED_TYPE
    assert captured["transaction_id"] == f"purchase_recorded:{sale_id}"
    assert captured["source"] == "INTERNAL"
    assert captured["depth"] == 1
    payload = captured["payload"]
    assert payload["purchaseCount"] == 3
    assert payload["previousPurchaseCount"] == 2
    assert payload["orderAmount"] == 25000
    assert payload["isFirstPurchase"] is False
    assert payload["_ruleDepth"] == 1


def test_emit_purchase_recorded_skipped_without_sale_id(monkeypatch):
    called = []

    def fake_create(*args, **kwargs):
        called.append(1)
        return None

    monkeypatch.setattr(
        "app.services.transaction_service.create_internal_transaction",
        fake_create,
    )
    monkeypatch.setattr(
        "app.services.purchase_service.get_qualification_mode",
        lambda db, brand: "PURCHASES",
    )
    customer = SimpleNamespace(brand="batira", profile_id="p1", loyalty_status="base")
    assert emit_purchase_recorded_event(
        db=None,
        customer=customer,
        source_transaction=SimpleNamespace(id=None),
        order_amount=1,
        purchase_count=1,
        purchase_amount_total=1,
        previous_purchase_count=0,
        previous_purchase_amount_total=0,
        is_first_purchase=True,
        depth=0,
    ) is None
    assert called == []


def test_create_internal_transaction_depth_allows_purchase_recorded_then_coupon():
    """sale (0) → TIER_CHANGED (0) → PURCHASE_RECORDED (1) → issue_coupon stays under max_depth 3."""
    import inspect

    from app.services.transaction_service import create_internal_transaction

    params = inspect.signature(create_internal_transaction).parameters
    assert params["max_depth"].default == 3
    assert params["depth"].default == 0
    # PURCHASE_RECORDED is created at sale_depth + 1 (typically 1), which is below 3.
