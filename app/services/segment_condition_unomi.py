"""Translate loyalty-engine segment AST (customer.*) to Apache Unomi segment conditions."""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

# Loyalty operator -> Unomi profilePropertyCondition comparisonOperator
_INTEGER_CUSTOMER_FIELDS = frozenset(
    {
        "customer.status_points",
        "customer.metrics.transactions_count_30d",
        "customer.metrics.transactions_count_90d",
    }
)

_DATE_CUSTOMER_FIELDS = frozenset(
    {
        "customer.birthdate",
        "customer.birthday",
    }
)

_ISO_DATETIME_CUSTOMER_FIELDS = frozenset(
    {
        "customer.created_at",
        "customer.last_activity_at",
        "customer.metrics.last_transaction_at",
    }
)

_FIELD_ALIASES = {
    "customer.birthday": "customer.birthdate",
}

_FULL_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_PARTIAL_DATE_RE = re.compile(r"^\d{2}-\d{2}$")

_OP_MAP = {
    "eq": "equals",
    "equals": "equals",
    "neq": "notEquals",
    "ne": "notEquals",
    "gt": "greaterThan",
    "gte": "greaterThanOrEqualTo",
    "lt": "lessThan",
    "lte": "lessThanOrEqualTo",
    "contains": "contains",
    "starts_with": "startsWith",
    "ends_with": "endsWith",
    "exists": "exists",
    "in": "in",
}


def _normalize_customer_field(field: str) -> str:
    key = str(field or "").strip()
    return _FIELD_ALIASES.get(key, key)


def _full_date_to_epoch_ms(value: str) -> int:
    d = date.fromisoformat(value)
    dt = datetime(d.year, d.month, d.day)
    return int(dt.timestamp() * 1000)


def _customer_field_to_unomi_property(field: str) -> str:
    field = _normalize_customer_field(field)
    if not field.startswith("customer."):
        raise ValueError(f"Segment Unomi translator supports customer.* only, got: {field}")
    path = field[len("customer.") :]
    if path.startswith("metrics."):
        return f"properties.metrics.{path[len('metrics.'):]}"
    # Align with loyalty → Unomi profile sync (build_unomi_profile_payload).
    camel = {
        "loyalty_status": "loyaltyStatus",
        "status_points": "statusPoints",
        "last_activity_at": "lastActivityAt",
        "created_at": "loyaltyCreatedAt",
        "birthdate": "birthDate",
    }
    leaf = camel.get(path, path)
    return f"properties.{leaf}"


def _is_loyalty_ast(node: dict) -> bool:
    return "field" in node or "and" in node or "or" in node or "not" in node


def _is_unomi_condition(node: dict) -> bool:
    return "type" in node and "parameterValues" in node


def loyalty_ast_to_unomi_condition(node: dict[str, Any] | None) -> dict[str, Any]:
    if node is None:
        raise ValueError("conditions are required")
    if not isinstance(node, dict):
        raise ValueError("conditions must be an object")
    if _is_unomi_condition(node):
        return node
    if not _is_loyalty_ast(node):
        raise ValueError("conditions must be loyalty AST or Unomi condition JSON (type + parameterValues)")

    return _translate_node(node)


def _unomi_value_params(field: str, value: Any) -> dict[str, Any]:
    """Map AST value to the Unomi propertyValue* key expected by the CDP."""
    if value is None:
        return {}
    field_key = _normalize_customer_field(str(field))
    raw = str(value).strip()

    if field_key in _INTEGER_CUSTOMER_FIELDS:
        try:
            return {"propertyValueInteger": int(value)}
        except (TypeError, ValueError) as e:
            raise ValueError(f"Integer value required for {field_key}") from e

    if field_key in _DATE_CUSTOMER_FIELDS:
        if _FULL_DATE_RE.match(raw):
            return {"propertyValueInteger": _full_date_to_epoch_ms(raw)}
        if _PARTIAL_DATE_RE.match(raw):
            return {"propertyValue": raw}
        raise ValueError(
            f"birthdate/birthday value must be YYYY-MM-DD or MM-DD for Unomi sync, got: {value!r}"
        )

    if field_key in _ISO_DATETIME_CUSTOMER_FIELDS:
        return {"propertyValueDate": raw}

    if isinstance(value, bool):
        return {"propertyValueBoolean": value}
    if isinstance(value, int) and not isinstance(value, bool):
        return {"propertyValueInteger": value}
    if isinstance(value, float):
        return {"propertyValueDouble": value}
    return {"propertyValue": value}


def _translate_node(node: dict[str, Any]) -> dict[str, Any]:
    if "and" in node:
        subs = [_translate_node(x) for x in node["and"]]
        return {
            "type": "booleanCondition",
            "parameterValues": {"operator": "and", "subConditions": subs},
        }
    if "or" in node:
        subs = [_translate_node(x) for x in node["or"]]
        return {
            "type": "booleanCondition",
            "parameterValues": {"operator": "or", "subConditions": subs},
        }
    if "not" in node:
        return {
            "type": "notCondition",
            "parameterValues": {},
            "subConditions": [_translate_node(node["not"])],
        }

    field = _normalize_customer_field(str(node.get("field") or ""))
    op = (node.get("operator") or node.get("op") or "").lower()
    value = node.get("value")

    if not field or not op:
        raise ValueError("Invalid loyalty AST leaf: field and operator required")

    if field.startswith("system.") or field.startswith("payload."):
        raise ValueError(f"Field not supported for Unomi segment translation: {field}")

    unomi_op = _OP_MAP.get(op)
    if not unomi_op:
        raise ValueError(f"Operator not supported for Unomi segment translation: {op}")

    prop = _customer_field_to_unomi_property(field)

    if unomi_op == "exists":
        return {
            "type": "profilePropertyCondition",
            "parameterValues": {
                "propertyName": prop,
                "comparisonOperator": "exists",
            },
        }

    if unomi_op == "in":
        if not isinstance(value, list):
            raise ValueError("'in' operator requires a list value")
        return {
            "type": "booleanCondition",
            "parameterValues": {
                "operator": "or",
                "subConditions": [
                    {
                        "type": "profilePropertyCondition",
                        "parameterValues": {
                            "propertyName": prop,
                            "comparisonOperator": "equals",
                            **_unomi_value_params(field, v),
                        },
                    }
                    for v in value
                ],
            },
        }

    params: dict[str, Any] = {
        "propertyName": prop,
        "comparisonOperator": unomi_op,
    }
    if unomi_op not in {"exists"}:
        params.update(_unomi_value_params(field, value))

    return {"type": "profilePropertyCondition", "parameterValues": params}


def resolve_unomi_condition_for_segment(
    *,
    is_dynamic: bool,
    conditions: dict | None,
    manual_profile_ids: list[str] | None,
) -> dict[str, Any]:
    if not is_dynamic:
        from app.services.unomi_segment_service import profile_ids_or_condition

        return profile_ids_or_condition(manual_profile_ids or [])
    return loyalty_ast_to_unomi_condition(conditions)


# Unomi comparisonOperator -> loyalty operator (subset used by the segment builder).
_UNOMI_OP_REVERSE = {
    "equals": "eq",
    "notEquals": "neq",
    "greaterThan": "gt",
    "greaterThanOrEqualTo": "gte",
    "lessThan": "lt",
    "lessThanOrEqualTo": "lte",
    "contains": "contains",
    "startsWith": "starts_with",
    "endsWith": "ends_with",
    "exists": "exists",
}


def _camel_leaf_to_snake(leaf: str) -> str:
    out: list[str] = []
    for i, ch in enumerate(leaf):
        if ch.isupper() and i > 0 and (leaf[i - 1].islower() or (i + 1 < len(leaf) and leaf[i + 1].islower())):
            out.append("_")
        out.append(ch.lower())
    return "".join(out)


def _unomi_property_to_customer_field(property_name: str) -> str | None:
    prop = (property_name or "").strip()
    if not prop.startswith("properties."):
        return None
    rest = prop[len("properties.") :]
    if rest.startswith("metrics."):
        return f"customer.metrics.{rest[len('metrics.') :]}"
    snake_from_camel = {
        "loyaltyStatus": "loyalty_status",
        "statusPoints": "status_points",
        "lastActivityAt": "last_activity_at",
        "lastActivityDate": "last_activity_at",
        "loyaltyCreatedAt": "created_at",
        "creationDate": "created_at",
        "birthDate": "birthdate",
    }
    if rest in snake_from_camel:
        return f"customer.{snake_from_camel[rest]}"
    if "." not in rest:
        return f"customer.{_camel_leaf_to_snake(rest)}"
    return None


def _unomi_subconditions(node: dict[str, Any]) -> list[dict[str, Any]] | None:
    params = node.get("parameterValues")
    if isinstance(params, dict):
        subs = params.get("subConditions")
        if isinstance(subs, list):
            return [x for x in subs if isinstance(x, dict)]
    subs = node.get("subConditions")
    if isinstance(subs, list):
        return [x for x in subs if isinstance(x, dict)]
    return None


def _epoch_ms_to_iso_date(ms: int) -> str:
    """Inverse of _full_date_to_epoch_ms (same naive-local semantics as profile sync)."""
    try:
        dt = datetime.fromtimestamp(int(ms) / 1000.0)
        return dt.date().isoformat()
    except Exception:
        return str(ms)


def _unomi_property_value(params: dict[str, Any], *, field: str | None = None) -> Any:
    field_key = _normalize_customer_field(field or "")
    for key in (
        "propertyValue",
        "propertyValueInteger",
        "propertyValueDouble",
        "propertyValueDate",
        "propertyValueBoolean",
    ):
        if key in params and params[key] is not None:
            val = params[key]
            if (
                field_key in _DATE_CUSTOMER_FIELDS
                and key == "propertyValueInteger"
                and isinstance(val, int)
            ):
                return _epoch_ms_to_iso_date(val)
            return val
    return None


def _try_unomi_or_as_in(field: str, subs: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Detect OR-of-equals on one property (loyalty 'in' translated to Unomi)."""
    values: list[Any] = []
    for sub in subs:
        if str(sub.get("type") or "") != "profilePropertyCondition":
            return None
        params = sub.get("parameterValues") or {}
        if not isinstance(params, dict):
            return None
        if _unomi_property_to_customer_field(str(params.get("propertyName") or "")) != field:
            return None
        if str(params.get("comparisonOperator") or "") != "equals":
            return None
        values.append(_unomi_property_value(params, field=field))
    if not values:
        return None
    return {"field": field, "operator": "in", "value": values}


def unomi_condition_to_loyalty_ast(node: dict[str, Any] | None) -> dict[str, Any] | None:
    """Best-effort inverse of loyalty_ast_to_unomi_condition for CDP → engine UI."""
    if node is None or not isinstance(node, dict):
        return None
    if _is_loyalty_ast(node):
        return node
    if not _is_unomi_condition(node):
        return None
    try:
        return _unomi_to_loyalty_node(node)
    except (ValueError, TypeError):
        return None


def _unomi_to_loyalty_node(node: dict[str, Any]) -> dict[str, Any]:
    cond_type = str(node.get("type") or "").strip()

    if cond_type == "booleanCondition":
        params = node.get("parameterValues") or {}
        if not isinstance(params, dict):
            raise ValueError("invalid booleanCondition")
        op = str(params.get("operator") or "").lower()
        subs = _unomi_subconditions(node) or []
        if not subs:
            raise ValueError("booleanCondition without subConditions")
        if op == "or":
            field = _unomi_property_to_customer_field(
                str((subs[0].get("parameterValues") or {}).get("propertyName") or "")
            )
            if field:
                as_in = _try_unomi_or_as_in(field, subs)
                if as_in:
                    return as_in
        key = "and" if op == "and" else "or"
        return {key: [_unomi_to_loyalty_node(s) for s in subs]}

    if cond_type == "notCondition":
        subs = _unomi_subconditions(node) or []
        if len(subs) != 1:
            raise ValueError("notCondition requires exactly one subCondition")
        return {"not": _unomi_to_loyalty_node(subs[0])}

    if cond_type == "profilePropertyCondition":
        params = node.get("parameterValues") or {}
        if not isinstance(params, dict):
            raise ValueError("invalid profilePropertyCondition")
        field = _unomi_property_to_customer_field(str(params.get("propertyName") or ""))
        if not field:
            raise ValueError(f"unsupported Unomi property: {params.get('propertyName')}")
        unomi_op = str(params.get("comparisonOperator") or "")
        loyalty_op = _UNOMI_OP_REVERSE.get(unomi_op)
        if not loyalty_op:
            raise ValueError(f"unsupported Unomi operator: {unomi_op}")
        if loyalty_op == "exists":
            return {"field": field, "operator": "exists", "value": True}
        return {"field": field, "operator": loyalty_op, "value": _unomi_property_value(params, field=field)}

    raise ValueError(f"unsupported Unomi condition type: {cond_type}")
