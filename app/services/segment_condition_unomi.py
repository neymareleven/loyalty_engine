"""Translate loyalty-engine segment AST (customer.*) to Apache Unomi segment conditions."""

from __future__ import annotations

from typing import Any

# Loyalty operator -> Unomi profilePropertyCondition comparisonOperator
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


def _customer_field_to_unomi_property(field: str) -> str:
    if not field.startswith("customer."):
        raise ValueError(f"Segment Unomi translator supports customer.* only, got: {field}")
    path = field[len("customer.") :]
    if path.startswith("metrics."):
        return f"properties.metrics.{path[len('metrics.'):]}"
    # Unomi common profile properties live under properties.*
    camel = {
        "loyalty_status": "loyaltyStatus",
        "status_points": "statusPoints",
        "last_activity_at": "lastActivityDate",
        "created_at": "creationDate",
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

    field = node.get("field")
    op = (node.get("operator") or node.get("op") or "").lower()
    value = node.get("value")

    if field is None or not op:
        raise ValueError("Invalid loyalty AST leaf: field and operator required")

    if str(field).startswith("system.") or str(field).startswith("payload."):
        raise ValueError(f"Field not supported for Unomi segment translation: {field}")

    unomi_op = _OP_MAP.get(op)
    if not unomi_op:
        raise ValueError(f"Operator not supported for Unomi segment translation: {op}")

    prop = _customer_field_to_unomi_property(str(field))

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
                            "propertyValue": v,
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
        params["propertyValue"] = value

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
