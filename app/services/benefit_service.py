from __future__ import annotations

import logging
from typing import Any

from app.services.match_key import normalize_match_key

logger = logging.getLogger(__name__)

DISCOUNT_TYPES = frozenset({"PERCENT_DISCOUNT", "FIXED_DISCOUNT"})
SHIPPING_TYPES = frozenset(
    {"FREE_SHIPPING", "SHIPPING_PERCENT_DISCOUNT", "SHIPPING_FIXED_DISCOUNT"}
)
BENEFIT_TYPES = frozenset(
    {
        "PERCENT_DISCOUNT",
        "FIXED_DISCOUNT",
        "FREE_SHIPPING",
        "SHIPPING_PERCENT_DISCOUNT",
        "SHIPPING_FIXED_DISCOUNT",
        "FREE_PRODUCT",
        "CUSTOM",
    }
)


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def format_int_fr(value: int) -> str:
    sign = "-" if int(value) < 0 else ""
    digits = str(abs(int(value)))
    parts: list[str] = []
    while digits:
        parts.append(digits[-3:])
        digits = digits[:-3]
    return sign + " ".join(reversed(parts))


def benefit_scope(benefit_type: str | None, applies_to: str | None = None) -> str:
    btype = (benefit_type or "CUSTOM").upper()
    if btype in SHIPPING_TYPES:
        return "SHIPPING"
    if btype == "FREE_PRODUCT":
        return "PRODUCT"
    if btype == "CUSTOM":
        return "OTHER"
    applies = (applies_to or "CART").upper()
    if applies in {"PRODUCTS", "CATEGORIES"}:
        return "PRODUCTS"
    return "CART"


def describe_benefit(
    *,
    benefit_type: str | None,
    benefit_value: int | None = None,
    max_discount_amount: int | None = None,
    min_cart_amount: int | None = None,
    name: str | None = None,
    free_product_quantity: int | None = None,
    applies_to: str | None = None,
) -> str:
    btype = (benefit_type or "CUSTOM").upper()
    if btype == "CUSTOM":
        return (name or "Avantage").strip() or "Avantage"

    if btype == "PERCENT_DISCOUNT":
        head = f"-{format_int_fr(_int(benefit_value))} % sur le panier"
    elif btype == "FIXED_DISCOUNT":
        head = f"-{format_int_fr(_int(benefit_value))} sur le panier"
    elif btype == "FREE_SHIPPING":
        head = "Livraison offerte"
    elif btype == "SHIPPING_PERCENT_DISCOUNT":
        head = f"-{format_int_fr(_int(benefit_value))} % sur la livraison"
    elif btype == "SHIPPING_FIXED_DISCOUNT":
        head = f"-{format_int_fr(_int(benefit_value))} sur la livraison"
    elif btype == "FREE_PRODUCT":
        qty = max(1, _int(free_product_quantity, 1))
        head = "Produit offert" if qty == 1 else f"{format_int_fr(qty)} produits offerts"
    else:
        head = (name or btype).strip() or btype

    extras: list[str] = []
    if min_cart_amount is not None and int(min_cart_amount) > 0:
        extras.append(f"dès {format_int_fr(int(min_cart_amount))} de panier")
    if max_discount_amount is not None and int(max_discount_amount) > 0:
        extras.append(f"plafonné à {format_int_fr(int(max_discount_amount))}")
    if not extras:
        return head
    if len(extras) == 1 and extras[0].startswith("dès "):
        return f"{head}, {extras[0]}"
    if len(extras) == 1:
        return f"{head} ({extras[0]})"
    return f"{head}, {extras[0]} ({extras[1]})"


def decorate_benefit(
    benefit: dict,
    *,
    applies_to: str | None = None,
    benefit_value: int | None = None,
    max_discount_amount: int | None = None,
    min_cart_amount: int | None = None,
    name: str | None = None,
    free_product_quantity: int | None = None,
) -> dict:
    btype = str(benefit.get("type") or "CUSTOM")
    benefit["scope"] = benefit_scope(btype, applies_to)
    benefit["description"] = describe_benefit(
        benefit_type=btype,
        benefit_value=benefit_value,
        max_discount_amount=max_discount_amount,
        min_cart_amount=min_cart_amount,
        name=name,
        free_product_quantity=free_product_quantity,
        applies_to=applies_to,
    )
    return benefit


def cart_subtotal(cart: dict, *, discount_base: str = "SUBTOTAL_INCL_TAX") -> int:
    if discount_base == "SUBTOTAL_EXCL_TAX" and cart.get("taxIncluded") is True:
        return _int(cart.get("subtotalExclTax"), _int(cart.get("subtotal")))
    return _int(cart.get("subtotal"))


def cart_shipping(cart: dict) -> int:
    methods = cart.get("shippingMethods")
    if isinstance(methods, list) and methods:
        return sum(_int(m.get("amount")) for m in methods if isinstance(m, dict))
    return _int(cart.get("shippingAmount"))


def cart_item_match_candidates(item: dict) -> set[str]:
    out: set[str] = set()
    match_key = str(item.get("matchKey") or "").strip().lower()
    if match_key:
        out.add(match_key)
    name_key = normalize_match_key(item.get("name"))
    if name_key:
        out.add(name_key)
    sku = str(item.get("sku") or "").strip()
    if sku:
        out.add(sku)
        sku_key = normalize_match_key(sku)
        if sku_key:
            out.add(sku_key)
    pid = str(item.get("productId") or item.get("id") or "").strip()
    if pid:
        out.add(pid)
    return out


def cart_match_keys_for_log(cart: dict) -> list[str]:
    items = cart.get("items") if isinstance(cart.get("items"), list) else []
    keys: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        keys.extend(sorted(cart_item_match_candidates(item)))
    return keys


def eligible_amount(cart: dict, *, applies_to: str, product_ids: set[str], category_ids: set[str], discount_base: str) -> int:
    subtotal = cart_subtotal(cart, discount_base=discount_base)
    applies = (applies_to or "CART").upper()
    if applies == "CART" or not product_ids and not category_ids:
        return subtotal
    expected = {str(x) for x in product_ids}
    items = cart.get("items") if isinstance(cart.get("items"), list) else []
    total = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        cats = {str(c) for c in (item.get("categoryIds") or [])}
        line = _int(item.get("unitPrice")) * max(1, _int(item.get("quantity"), 1))
        if applies == "PRODUCTS" and cart_item_match_candidates(item).intersection(expected):
            total += line
        elif applies == "CATEGORIES" and cats.intersection(category_ids):
            total += line
    return total


def storefront_product_ref(product, quantity: int = 1) -> dict:
    engine_id = str(getattr(product, "id", "") or "")
    match_key = str(getattr(product, "match_key", None) or "").strip()
    sku = str(getattr(product, "sku", None) or "").strip() or None
    name = getattr(product, "name", None)
    external = match_key or sku or ""
    item = {
        "productId": external,
        "name": (str(name).strip() if name else "") or match_key or sku or "",
        "quantity": max(1, int(quantity or 1)),
    }
    if sku:
        item["sku"] = sku
    if not external:
        item["productId"] = ""
        item["engineProductId"] = engine_id
    return item


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
    name: str | None = None,
    product_refs: list[dict] | None = None,
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
        if (applies_to or "CART").upper() == "PRODUCTS":
            logger.info(
                "NO_ELIGIBLE_ITEMS applies_to=PRODUCTS cart_keys=%s reward_keys=%s",
                cart_match_keys_for_log(cart),
                sorted(str(x) for x in product_ids),
            )
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
    elif btype == "SHIPPING_PERCENT_DISCOUNT":
        shipping = cart_shipping(cart)
        pct = max(0, _int(benefit_value))
        shipping_discount = (shipping * pct) // 100
        if max_discount_amount is not None:
            shipping_discount = min(shipping_discount, int(max_discount_amount))
        shipping_discount = min(shipping_discount, shipping)
    elif btype == "SHIPPING_FIXED_DISCOUNT":
        shipping = cart_shipping(cart)
        shipping_discount = max(0, _int(benefit_value))
        if max_discount_amount is not None:
            shipping_discount = min(shipping_discount, int(max_discount_amount))
        shipping_discount = min(shipping_discount, shipping)
    elif btype == "FREE_PRODUCT":
        if product_refs:
            free_items = [dict(ref) for ref in product_refs]
        else:
            if not product_ids:
                return None, "NO_ELIGIBLE_ITEMS"
            qty = max(1, _int(free_product_quantity, 1))
            pid = next(iter(product_ids))
            free_items = [{"productId": pid, "quantity": qty}]
    else:
        return decorate_benefit(
            {
                "type": btype,
                "discountAmount": 0,
                "shippingDiscount": 0,
                "freeItems": [],
            },
            applies_to=applies_to,
            benefit_value=benefit_value,
            max_discount_amount=max_discount_amount,
            min_cart_amount=min_cart_amount,
            name=name,
            free_product_quantity=free_product_quantity,
        ), None

    return decorate_benefit(
        {
            "type": btype,
            "discountAmount": int(discount_amount),
            "shippingDiscount": int(shipping_discount),
            "freeItems": free_items,
        },
        applies_to=applies_to,
        benefit_value=benefit_value,
        max_discount_amount=max_discount_amount,
        min_cart_amount=min_cart_amount,
        name=name,
        free_product_quantity=free_product_quantity,
    ), None
