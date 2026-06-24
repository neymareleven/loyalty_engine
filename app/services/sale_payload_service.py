"""Normalize WooCommerce / Unomi sale payloads before rule evaluation."""

from __future__ import annotations

from typing import Any

from app.services.rule_engine import _as_number_from_text

_MONETARY_SCALAR_KEYS = ("orderTotal", "total", "order_total", "tva", "remise")
_MONETARY_LIST_KEYS = ("productPrices", "productSubtotals")


def normalize_sale_payload(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    """
    Convert amount strings (\"525 CFA\", \"6\\u00a0300 CFA\") to integers on the payload
    so rules and earn_points see stable numeric values.
    """
    if not isinstance(payload, dict):
        return payload

    out = dict(payload)

    for key in _MONETARY_SCALAR_KEYS:
        val = out.get(key)
        if isinstance(val, str):
            parsed = _as_number_from_text(val)
            if parsed is not None:
                out[f"{key}Raw"] = val
                out[key] = parsed

    for key in _MONETARY_LIST_KEYS:
        vals = out.get(key)
        if not isinstance(vals, list):
            continue
        normalized: list[Any] = []
        for item in vals:
            if isinstance(item, str):
                parsed = _as_number_from_text(item)
                normalized.append(parsed if parsed is not None else item)
            else:
                normalized.append(item)
        out[key] = normalized

    qtys = out.get("productQuantities")
    if isinstance(qtys, list):
        out["productQuantities"] = [
            int(str(q).strip()) if q is not None and str(q).strip().isdigit() else q for q in qtys
        ]

    return out
