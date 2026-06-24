"""earn_points dynamic resolution ($path, $fn)."""

from types import SimpleNamespace

from app.services.rule_engine import _resolve_action_number


def _tx(**payload):
    return SimpleNamespace(brand="batira", payload=payload)


def test_resolve_order_total_path():
    tx = _tx(orderTotal="15000")
    points = {"$path": "payload.orderTotal"}
    assert _resolve_action_number(db=None, customer=None, action_value=points, transaction=tx) == 15000


def test_resolve_order_total_to_number_fn():
    tx = _tx(orderTotal="25000 CFA")
    points = {"$fn": "to_number", "args": [{"$path": "payload.orderTotal"}]}
    assert _resolve_action_number(db=None, customer=None, action_value=points, transaction=tx) == 25000


def test_resolve_order_total_with_nbsp():
    from app.services.rule_engine import _as_number_from_text

    assert _as_number_from_text("6\u00a0300 CFA") == 6300
    tx = _tx(orderTotal="525 CFA")
    points = {"$fn": "to_number", "args": [{"$path": "payload.orderTotal"}]}
    assert _resolve_action_number(db=None, customer=None, action_value=points, transaction=tx) == 525


def test_resolve_coalesce_order_total_or_total():
    tx = _tx(total="800")
    points = {
        "$fn": "to_number",
        "args": [
            {
                "$fn": "coalesce",
                "args": [{"$path": "payload.orderTotal"}, {"$path": "payload.total"}],
            }
        ],
    }
    assert _resolve_action_number(db=None, customer=None, action_value=points, transaction=tx) == 800

    tx2 = _tx(orderTotal="1200", total="800")
    assert _resolve_action_number(db=None, customer=None, action_value=points, transaction=tx2) == 1200


def test_resolve_sum_product_points_without_db_returns_zero():
    tx = _tx(productNames=["sku-a"], productQuantities=[2])
    points = {
        "$fn": "sum_product_points_unomi",
        "args": [{"$path": "payload.productNames"}, {"$path": "payload.productQuantities"}],
    }
    # No DB / no products configured → 0 (unknown products ignored)
    assert _resolve_action_number(db=None, customer=None, action_value=points, transaction=tx) == 0
