"""Unit tests for catalog invalidation helpers."""

from types import SimpleNamespace

from app.services.catalog_invalidation_service import (
    coupon_admin_allowed_transitions,
    invalidate_product_in_snapshot,
    reward_admin_allowed_transitions,
    stamp_catalog_removed,
)
from app.services.customer_entitlement_serialization import serialize_customer_coupon_out


def test_stamp_catalog_removed_sets_flags():
    payload = stamp_catalog_removed(
        {"foo": "bar"},
        reason="REWARD_DELETED",
        entity_type="reward",
        entity_id="abc",
        entity_name="Café",
    )
    assert payload["catalogRemoved"] is True
    assert payload["catalogRemovedReason"] == "REWARD_DELETED"
    assert payload["catalogRemovedEntityName"] == "Café"
    assert payload["foo"] == "bar"


def test_invalidate_product_in_snapshot_marks_only_target():
    payload = {
        "productSnapshots": [
            {"id": "p1", "name": "Café", "catalogRemoved": False},
            {"id": "p2", "name": "Dessert", "catalogRemoved": False},
        ]
    }
    updated = invalidate_product_in_snapshot(payload, product_id="p2", product_name="Dessert")
    assert updated["productSnapshots"][0]["catalogRemoved"] is False
    assert updated["productSnapshots"][1]["catalogRemoved"] is True
    assert updated["productSnapshots"][1]["invalidatedAt"]


def test_coupon_admin_allowed_transitions_invalidated_is_empty():
    coupon = SimpleNamespace(status="INVALIDATED", payload={})
    assert coupon_admin_allowed_transitions(coupon) == []


def test_coupon_admin_allowed_transitions_catalog_removed_is_empty():
    coupon = SimpleNamespace(status="USED", payload={"catalogRemoved": True})
    assert coupon_admin_allowed_transitions(coupon) == []


def test_coupon_admin_allowed_transitions_active_issued():
    coupon = SimpleNamespace(status="ISSUED", payload={})
    assert coupon_admin_allowed_transitions(coupon) == ["USED", "EXPIRED"]


def test_reward_admin_allowed_transitions_invalidated():
    reward = SimpleNamespace(status="INVALIDATED", payload={})
    assert reward_admin_allowed_transitions(reward) == []


def test_serialize_customer_coupon_out_invalidated_label():
    coupon = SimpleNamespace(
        id="00000000-0000-0000-0000-000000000001",
        customer_id="00000000-0000-0000-0000-000000000002",
        coupon_type_id=None,
        status="INVALIDATED",
        issued_at=None,
        expires_at=None,
        used_at=None,
        source_transaction_id=None,
        rule_id=None,
        rule_execution_id=None,
        payload={
            "couponTypeSnapshot": {"id": "x", "name": "Anniversaire"},
            "catalogRemoved": True,
        },
        created_at=None,
        updated_at=None,
    )

    class _Db:
        def query(self, *_args, **_kwargs):
            return self

        def filter(self, *_args, **_kwargs):
            return self

        def first(self):
            return None

    data = serialize_customer_coupon_out(_Db(), coupon=coupon)
    assert data["display_label"] == "Anniversaire"
    assert data["admin_actions_enabled"] is False
    assert data["status_label"] == "Invalidé"
