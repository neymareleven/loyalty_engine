from app.services.benefit_service import compute_benefit, storefront_product_ref
from app.services.match_key import normalize_match_key


def _percent_on_savon(cart: dict):
    return compute_benefit(
        benefit_type="PERCENT_DISCOUNT",
        benefit_value=10,
        max_discount_amount=None,
        min_cart_amount=None,
        applies_to="PRODUCTS",
        free_product_quantity=1,
        cart=cart,
        product_ids={"savon"},
        category_ids=set(),
    )


def test_normalize_match_key_examples():
    assert normalize_match_key("Savon d'Alep Crème 100g") == "savon-d-alep-cr-me-100g"
    assert normalize_match_key("Ciment CPJ 42,5 (50kg)") == "ciment-cpj-42-5-50kg"


def test_product_percent_matches_match_key():
    cart = {
        "subtotal": 30000,
        "items": [
            {"matchKey": "savon", "unitPrice": 10000, "quantity": 1},
            {"matchKey": "ciment", "unitPrice": 20000, "quantity": 1},
        ],
    }
    benefit, reason = _percent_on_savon(cart)
    assert reason is None
    assert benefit["discountAmount"] == 1000


def test_product_percent_matches_normalized_name():
    cart = {
        "subtotal": 10000,
        "items": [{"name": "Savon", "unitPrice": 10000, "quantity": 1}],
    }
    benefit, reason = _percent_on_savon(cart)
    assert reason is None
    assert benefit["discountAmount"] == 1000


def test_product_percent_matches_sku():
    cart = {
        "subtotal": 10000,
        "items": [{"sku": "savon", "unitPrice": 10000, "quantity": 1}],
    }
    benefit, reason = _percent_on_savon(cart)
    assert reason is None
    assert benefit["discountAmount"] == 1000


def test_product_percent_old_product_id_does_not_match():
    cart = {
        "subtotal": 10000,
        "items": [{"productId": "123", "unitPrice": 10000, "quantity": 1}],
    }
    benefit, reason = _percent_on_savon(cart)
    assert benefit is None
    assert reason == "NO_ELIGIBLE_ITEMS"


def test_product_percent_only_eligible_line_counts():
    cart = {
        "subtotal": 35000,
        "items": [
            {"matchKey": "savon", "unitPrice": 5000, "quantity": 2},
            {"name": "Autre chose", "unitPrice": 25000, "quantity": 1},
        ],
    }
    benefit, reason = _percent_on_savon(cart)
    assert reason is None
    assert benefit["discountAmount"] == 1000


def test_duplicate_match_keys_both_count():
    cart = {
        "subtotal": 20000,
        "items": [
            {"matchKey": "savon", "unitPrice": 8000, "quantity": 1},
            {"name": "Savon", "unitPrice": 12000, "quantity": 1},
        ],
    }
    benefit, reason = _percent_on_savon(cart)
    assert reason is None
    assert benefit["discountAmount"] == 2000


def test_storefront_product_ref_always_has_name():
    product = type("P", (), {"id": "uuid", "match_key": "savon", "sku": "SKU-1", "name": None})()
    ref = storefront_product_ref(product, 1)
    assert ref["name"] == "savon"
    assert ref["productId"] == "savon"
