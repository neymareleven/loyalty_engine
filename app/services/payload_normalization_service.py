"""Normalize inbound transaction payloads to stable JSON types before storage and schema inference."""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any

from app.services.rule_engine import _as_number_from_text

_MONETARY_SCALAR_KEYS = frozenset(
    {
        "ordertotal",
        "total",
        "order_total",
        "subtotal",
        "tva",
        "remise",
        "expedition",
        "shipping_total",
        "discount_total",
        "grand_total",
    }
)
_MONETARY_LIST_KEYS = frozenset({"productprices", "productsubtotals"})
_QUANTITY_LIST_KEYS = frozenset({"productquantities"})
_INTEGER_SCALAR_KEYS = frozenset({"quantity", "qty"})
_BOOLEAN_KEYS = frozenset({"isloggedin", "active", "optin", "newsletter"})
_EMAIL_KEYS = frozenset(
    {
        "email",
        "billing_email",
        "billingemail",
        "scopeemail",
        "recipientemail",
    }
)
_PHONE_KEYS = frozenset({"phone", "billing_phone", "billingphone", "mobile"})
_DATE_KEYS = frozenset({"orderdate", "order_date", "birthdate", "birth_date"})
_DATETIME_SUFFIXES = ("at",)


def field_key_lower(key: str) -> str:
    return (key or "").strip().lower().replace("-", "_")


def is_datetime_field(key: str) -> bool:
    k = field_key_lower(key)
    if k in _DATE_KEYS:
        return True
    if k.endswith("date"):
        return True
    return any(k.endswith(suffix) for suffix in _DATETIME_SUFFIXES)


def is_monetary_scalar_field(key: str) -> bool:
    k = field_key_lower(key)
    if k in _MONETARY_SCALAR_KEYS:
        return True
    if k.endswith("raw"):
        return False
    return k.endswith("total") or k.endswith("amount")


def is_boolean_field(key: str) -> bool:
    k = field_key_lower(key)
    if k in _BOOLEAN_KEYS:
        return True
    return k.startswith("is_") or k.startswith("has_")


def is_email_field(key: str) -> bool:
    k = field_key_lower(key)
    return k in _EMAIL_KEYS or k.endswith("email")


def is_phone_field(key: str) -> bool:
    k = field_key_lower(key)
    return k in _PHONE_KEYS or "phone" in k


def semantic_schema_for_field(name: str) -> dict[str, Any] | None:
    """Target JSON Schema fragment for a known business field name."""
    k = field_key_lower(name)
    if k.endswith("raw"):
        return {"type": "string"}
    if is_email_field(name):
        return {"type": "string", "format": "email"}
    if is_phone_field(name):
        return {"type": "string", "format": "phone"}
    if is_datetime_field(name):
        return {"type": "string", "format": "date-time"}
    if is_boolean_field(name):
        return {"type": "boolean"}
    if is_monetary_scalar_field(name):
        return {"type": "integer"}
    if k in _MONETARY_LIST_KEYS:
        return {"type": "array", "items": {"type": "integer"}}
    if k in _QUANTITY_LIST_KEYS:
        return {"type": "array", "items": {"type": "integer"}}
    if k in _INTEGER_SCALAR_KEYS:
        return {"type": "integer"}
    if k == "productnames":
        return {"type": "array", "items": {"type": "string"}}
    return None


def _coerce_monetary(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value == int(value) else value
    if isinstance(value, str):
        parsed = _as_number_from_text(value)
        if parsed is not None:
            return parsed
    return value


def _coerce_integer(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value == int(value):
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if s.isdigit():
            return int(s)
        parsed = _as_number_from_text(s)
        if parsed is not None:
            return parsed
    return value


def _coerce_boolean(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        s = value.strip().lower()
        if s in ("true", "1", "yes", "on"):
            return True
        if s in ("false", "0", "no", "off"):
            return False
    return value


def _coerce_email(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return value.strip().lower()


def _coerce_phone(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    s = value.strip()
    if not s:
        return s
    plus = s.startswith("+")
    digits = re.sub(r"\D", "", s)
    if not digits:
        return s
    return f"+{digits}" if plus else digits


def _coerce_datetime(value: Any) -> Any:
    if value is None or value == "":
        return value
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return (
            datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            ts = float(value)
            if ts > 1e12:
                ts /= 1000.0
            return (
                datetime.fromtimestamp(ts, tz=timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
            )
        except (OSError, ValueError, OverflowError):
            return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return value
        try:
            iso = s[:-1] + "+00:00" if s.endswith("Z") else s
            dt = datetime.fromisoformat(iso)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        except ValueError:
            pass
        for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                d = datetime.strptime(s[:19] if " " in fmt else s[:10], fmt)
                return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
            except ValueError:
                continue
    return value


def normalize_field_value(key: str, value: Any) -> Any:
    if is_email_field(key):
        return _coerce_email(value)
    if is_phone_field(key):
        return _coerce_phone(value)
    if is_datetime_field(key):
        return _coerce_datetime(value)
    if is_boolean_field(key):
        return _coerce_boolean(value)
    if is_monetary_scalar_field(key):
        return _coerce_monetary(value)
    k = field_key_lower(key)
    if k in _INTEGER_SCALAR_KEYS:
        return _coerce_integer(value)
    return value


def normalize_transaction_payload(
    payload: dict[str, Any] | None,
    *,
    transaction_type: str | None = None,
) -> dict[str, Any] | None:
    """Coerce payload values to stable types for any inbound transaction type."""
    del transaction_type  # reserved for per-type overrides later
    if not isinstance(payload, dict):
        return payload

    out: dict[str, Any] = {}
    for key, value in payload.items():
        if not isinstance(key, str):
            out[key] = value
            continue

        k = field_key_lower(key)

        if is_monetary_scalar_field(key) and isinstance(value, str):
            parsed = _as_number_from_text(value)
            if parsed is not None:
                out[f"{key}Raw"] = value
                out[key] = parsed
                continue

        if k in _MONETARY_LIST_KEYS and isinstance(value, list):
            out[key] = [
                _coerce_monetary(item) if isinstance(item, str) else normalize_field_value(key, item)
                for item in value
            ]
            continue

        if k in _QUANTITY_LIST_KEYS and isinstance(value, list):
            out[key] = [_coerce_integer(item) for item in value]
            continue

        out[key] = normalize_field_value(key, value)

    return out
