"""Sale payload normalization for rule engine."""

from app.services.rule_engine import _as_number_from_text
from app.services.sale_payload_service import normalize_sale_payload


def test_normalize_order_total_cfa():
    out = normalize_sale_payload({"orderTotal": "525 CFA", "total": "525 CFA"})
    assert out["orderTotal"] == 525
    assert out["orderTotalRaw"] == "525 CFA"


def test_normalize_nbsp_thousands():
    out = normalize_sale_payload({"orderTotal": "6\u00a0300 CFA"})
    assert out["orderTotal"] == 6300


def test_normalize_product_prices():
    out = normalize_sale_payload(
        {
            "productPrices": ["475 CFA", "5 500 CFA"],
            "productQuantities": ["1", "2"],
        }
    )
    assert out["productPrices"] == [475, 5500]
    assert out["productQuantities"] == [1, 2]
