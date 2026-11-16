from __future__ import annotations

from typing import Any

DISCOUNT_TYPES = frozenset({"PERCENT_DISCOUNT", "FIXED_DISCOUNT"})


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def cart_subtotal(cart: dict, *, discount_base: str = "SUBTOTAL_INCL_TAX") -> int:
    if discount_base == "SUBTOTAL_EXCL_TAX" and cart.get("taxIncluded") is True:
        return _int(cart.get("subtotalExclTax"), _int(cart.get("subtotal")))
    return _int(cart.get("subtotal"))


def cart_shipping(cart: dict) -> int:
    methods = cart.get("shippingMethods")
    if isinstance(methods, list) and methods:
        return sum(_int(m.get("amount")) for m in methods if isinstance(m, dict))
    return _int(cart.get("shippingAmount"))


def eligible_amount(cart: dict, *, applies_to: str, product_ids: set[str], category_ids: set[str], discount_base: str) -> int:
    subtotal = cart_subtotal(cart, discount_base=discount_base)
    applies = (applies_to or "CART").upper()
    if applies == "CART" or not product_ids and not category_ids:
        return subtotal
    items = cart.get("items") if isinstance(cart.get("items"), list) else []
    total = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        pid = str(item.get("productId") or item.get("id") or "")
        sku = str(item.get("sku") or "")
        cats = {str(c) for c in (item.get("categoryIds") or [])}
        line = _int(item.get("unitPrice")) * max(1, _int(item.get("quantity"), 1))
        if applies == "PRODUCTS" and (pid in product_ids or sku in product_ids):
            total += line
        elif applies == "CATEGORIES" and cats.intersection(category_ids):
            total += line
    return total


def compute_benefit(
    *,
    benefit_type: str,
    benefit_value: int | None,
    max_discount_amount: int | None,
    min_cart_amount: int | None,
    applies_to: str,
    free_product_quantity: int,
    cart: dict,
    product_ids: set[str],
    category_ids: set[str],
    discount_base: str = "SUBTOTAL_INCL_TAX",
) -> tuple[dict | None, str | None]:
    btype = (benefit_type or "CUSTOM").upper()
    subtotal = cart_subtotal(cart, discount_base=discount_base)
    if min_cart_amount is not None and subtotal < int(min_cart_amount):
        return None, "MIN_CART_NOT_REACHED"

    eligible = eligible_amount(
        cart,
        applies_to=applies_to,
        product_ids=product_ids,
        category_ids=category_ids,
        discount_base=discount_base,
    )
    if btype in DISCOUNT_TYPES and eligible <= 0 and (applies_to or "CART").upper() != "CART":
        return None, "NO_ELIGIBLE_ITEMS"

    discount_amount = 0
    shipping_discount = 0
    free_items: list[dict] = []

    if btype == "PERCENT_DISCOUNT":
        pct = max(0, _int(benefit_value))
        discount_amount = (eligible * pct) // 100
        if max_discount_amount is not None:
            discount_amount = min(discount_amount, int(max_discount_amount))
        discount_amount = min(discount_amount, eligible, subtotal)
    elif btype == "FIXED_DISCOUNT":
        discount_amount = max(0, _int(benefit_value))
        if max_discount_amount is not None:
            discount_amount = min(discount_amount, int(max_discount_amount))
        discount_amount = min(discount_amount, eligible if eligible else subtotal, subtotal)
    elif btype == "FREE_SHIPPING":
        shipping_discount = cart_shipping(cart)
    elif btype == "FREE_PRODUCT":
        if not product_ids:
            return None, "NO_ELIGIBLE_ITEMS"
        qty = max(1, _int(free_product_quantity, 1))
        pid = next(iter(product_ids))
        free_items = [{"productId": pid, "quantity": qty}]
    else:
        return {
            "type": btype,
            "discountAmount": 0,
            "shippingDiscount": 0,
            "freeItems": [],
        }, None

    return {
        "type": btype,
        "discountAmount": int(discount_amount),
        "shippingDiscount": int(shipping_discount),
        "freeItems": free_items,
    }, None
