"""Payload schema helpers — JSON Schema, manual field maps, rule paths, ingest merge."""

from __future__ import annotations

from typing import Any

from app.services.payload_normalization_service import semantic_schema_for_field

_RESERVED_JSON_SCHEMA_KEYS = frozenset({"type", "properties", "items", "required", "additionalProperties", "anyOf", "oneOf", "allOf"})

_TYPE_ALIASES = {
    "text": "string",
    "texte": "string",
    "string": "string",
    "number": "number",
    "integer": "integer",
    "int": "integer",
    "bool": "boolean",
    "boolean": "boolean",
    "array": "array",
    "object": "object",
}

# Rule authoring hints per EXTERNAL transaction type key (sale / WooCommerce, etc.).
TRANSACTION_TYPE_RULE_HINTS: dict[str, dict[str, Any]] = {
    "sale": {
        "preferredOrderTotalPath": "payload.orderTotal",
        "orderTotalFallbackPaths": ["payload.total"],
        "productNamesPath": "payload.productNames",
        "productQuantitiesPath": "payload.productQuantities",
        "earnPointsExamples": [
            {
                "id": "sale.order_total",
                "label": "Points = montant orderTotal (recommandé)",
                "points": {"$fn": "to_number", "args": [{"$path": "payload.orderTotal"}]},
            },
            {
                "id": "sale.order_total_with_fallback",
                "label": "Points = orderTotal sinon total",
                "points": {
                    "$fn": "to_number",
                    "args": [
                        {
                            "$fn": "coalesce",
                            "args": [{"$path": "payload.orderTotal"}, {"$path": "payload.total"}],
                        }
                    ],
                },
            },
            {
                "id": "sale.product_points",
                "label": "Points catalogue = Σ (produit × quantité)",
                "points": {
                    "$fn": "sum_product_points_unomi",
                    "args": [{"$path": "payload.productNames"}, {"$path": "payload.productQuantities"}],
                },
            },
        ],
    },
}


def _normalize_field_type(value: Any) -> str:
    if isinstance(value, dict):
        raw = value.get("type")
    else:
        raw = value
    if isinstance(raw, list):
        raw = next((x for x in raw if x != "null"), raw[0] if raw else "string")
    key = str(raw or "string").strip().lower()
    return _TYPE_ALIASES.get(key, key or "string")


def is_canonical_json_schema(schema: Any) -> bool:
    if not isinstance(schema, dict):
        return False
    if schema.get("type") == "object" and isinstance(schema.get("properties"), dict):
        return True
    return False


def is_manual_field_map(schema: Any) -> bool:
    if not isinstance(schema, dict) or not schema:
        return False
    if is_canonical_json_schema(schema):
        return False
    return any(
        isinstance(k, str)
        and k
        and isinstance(v, dict)
        and ("type" in v or "description" in v)
        for k, v in schema.items()
    )


def is_mistaken_json_schema_root_as_fields(schema: Any) -> bool:
    """Detect UI bug: {type: {type:string}, properties: {type:string}}."""
    if not isinstance(schema, dict):
        return False

    if is_manual_field_map(schema):
        keys = set(schema.keys())
        if keys and keys <= {"type", "properties"}:
            for spec in schema.values():
                if isinstance(spec, dict) and _normalize_field_type(spec) == "string":
                    return True
        return False

    if is_canonical_json_schema(schema):
        props = schema.get("properties")
        if not isinstance(props, dict):
            return False
        # Valid inferred schemas have multiple business fields.
        if len(props) > 2:
            return False
        if set(props.keys()) <= {"type", "properties"}:
            for spec in props.values():
                if isinstance(spec, dict) and _normalize_field_type(spec) == "string":
                    return True
    return False


def normalize_payload_schema_for_storage(schema: Any) -> dict[str, Any] | None:
    if schema is None:
        return None
    if not isinstance(schema, dict):
        raise ValueError("payload_schema must be an object")

    if is_mistaken_json_schema_root_as_fields(schema):
        raise ValueError(
            "payload_schema invalide : le JSON Schema racine (type/properties) a été enregistré comme champs. "
            "Utilisez payload_fields renvoyé par l'API ou le format fieldName -> {type, description}."
        )

    if is_canonical_json_schema(schema):
        return schema

    if is_manual_field_map(schema):
        props: dict[str, Any] = {}
        for name, spec in schema.items():
            if not isinstance(name, str) or not name.strip():
                continue
            if name in _RESERVED_JSON_SCHEMA_KEYS and name in {"type", "properties"}:
                raise ValueError(
                    f"payload_schema: le champ réservé '{name}' ne peut pas être un nom de champ manuel"
                )
            if not isinstance(spec, dict):
                spec = {"type": _normalize_field_type(spec)}
            field = dict(spec)
            field["type"] = _normalize_field_type(field)
            props[name.strip()] = field
        return {"type": "object", "properties": props}

    raise ValueError(
        "payload_schema must be a JSON Schema ({type: object, properties: {...}}) "
        "or a manual map {fieldName: {type, description?}}"
    )


def get_transaction_type_rule_hints(transaction_type_key: str) -> dict[str, Any] | None:
    key = (transaction_type_key or "").strip().lower()
    return TRANSACTION_TYPE_RULE_HINTS.get(key)


def _safe_normalize_payload_schema(schema: Any) -> dict[str, Any] | None:
    if schema is None:
        return None
    if is_mistaken_json_schema_root_as_fields(schema):
        return None
    try:
        return normalize_payload_schema_for_storage(schema)
    except ValueError:
        return None


def _schema_type_rank(schema: dict | None) -> int:
    """Higher rank = more specific / preferred when healing legacy string types."""
    if not isinstance(schema, dict):
        return 0
    if "anyOf" in schema:
        return 1
    t = _normalize_field_type(schema)
    return {
        "boolean": 4,
        "integer": 3,
        "number": 3,
        "array": 2,
        "object": 2,
        "string": 1,
        "null": 0,
    }.get(t, 1)


def _apply_semantic_schema(name: str, schema: dict | None) -> dict | None:
    semantic = semantic_schema_for_field(name)
    if not semantic:
        return schema
    if not isinstance(schema, dict) or not schema:
        return dict(semantic)
    current_rank = _schema_type_rank(schema)
    target_rank = _schema_type_rank(semantic)
    if current_rank < target_rank:
        return dict(semantic)
    if current_rank == target_rank and isinstance(schema, dict):
        merged = dict(schema)
        for key, val in semantic.items():
            if key not in merged or merged.get(key) in (None, "", {}):
                merged[key] = val
        return merged
    return schema


def heal_payload_schema(schema: dict | None) -> dict | None:
    """Correct field types in stored schema using business field semantics."""
    if schema is None:
        return None
    normalized = _safe_normalize_payload_schema(schema)
    if not normalized or not is_canonical_json_schema(normalized):
        return schema
    props = dict(normalized.get("properties") or {})
    if not isinstance(props, dict):
        return normalized
    changed = False
    healed_props: dict[str, Any] = {}
    for name, spec in props.items():
        if not isinstance(name, str):
            continue
        healed = _apply_semantic_schema(name, spec if isinstance(spec, dict) else {"type": "string"})
        healed_props[name] = healed
        if healed != spec:
            changed = True
    if not changed:
        return normalized
    return {**normalized, "properties": healed_props}


def _merge_property_schemas(name: str, existing: dict | None, new: dict | None) -> dict | None:
    merged = merge_json_schemas(existing, new)
    return _apply_semantic_schema(name, merged)


def infer_json_schema_from_payload(value: Any, *, _depth: int = 0, _max_depth: int = 6, _field_name: str | None = None) -> dict | None:
    if _depth >= _max_depth:
        return {}
    if value is None:
        inferred = {"type": "null"}
    elif isinstance(value, bool):
        inferred = {"type": "boolean"}
    elif isinstance(value, int) and not isinstance(value, bool):
        inferred = {"type": "integer"}
    elif isinstance(value, float):
        inferred = {"type": "number"}
    elif isinstance(value, str):
        inferred = {"type": "string"}
    elif isinstance(value, dict):
        props: dict[str, Any] = {}
        for k, v in value.items():
            if not isinstance(k, str):
                continue
            child = infer_json_schema_from_payload(v, _depth=_depth + 1, _max_depth=_max_depth, _field_name=k) or {}
            props[k] = _apply_semantic_schema(k, child) or child
        return {"type": "object", "properties": props}
    elif isinstance(value, list):
        items_schema: dict[str, Any] | None = None
        for item in value[:50]:
            s = infer_json_schema_from_payload(item, _depth=_depth + 1, _max_depth=_max_depth, _field_name=_field_name) or {}
            items_schema = merge_json_schemas(items_schema, s)
        inferred = {"type": "array", "items": items_schema or {}}
        if _field_name:
            return _apply_semantic_schema(_field_name, inferred) or inferred
        return inferred
    else:
        inferred = {}

    if _field_name:
        return _apply_semantic_schema(_field_name, inferred) or inferred
    return inferred


def merge_json_schemas(a: dict | None, b: dict | None) -> dict | None:
    if not a:
        return b
    if not b:
        return a
    if not isinstance(a, dict) or not isinstance(b, dict):
        return a
    a_type = a.get("type")
    b_type = b.get("type")
    if a_type and b_type and a_type != b_type:
        a_rank = _schema_type_rank(a)
        b_rank = _schema_type_rank(b)
        if b_rank > a_rank:
            return b
        if a_rank > b_rank:
            return a
        return {"anyOf": [a, b]}
    out = dict(a)
    if out.get("type") == "object":
        out_props = dict(out.get("properties") or {})
        b_props = b.get("properties") or {}
        if isinstance(b_props, dict):
            for k, v in b_props.items():
                if k in out_props:
                    out_props[k] = _merge_property_schemas(k, out_props.get(k), v) or out_props[k]
                else:
                    out_props[k] = _apply_semantic_schema(k, v) or v
        out["properties"] = out_props
        return out
    if out.get("type") == "array":
        out["items"] = merge_json_schemas(out.get("items"), b.get("items")) or out.get("items") or {}
        return out
    return out


def enrich_payload_schema_on_ingest(existing: dict | None, payload: dict | None) -> dict | None:
    """Merge inferred payload shape into stored schema; heal corrupted or mistyped fields."""
    inferred = infer_json_schema_from_payload(payload) if payload is not None else None
    if not inferred:
        return heal_payload_schema(existing)
    if existing is None or is_mistaken_json_schema_root_as_fields(existing):
        return heal_payload_schema(inferred)
    normalized = _safe_normalize_payload_schema(existing)
    if normalized is None:
        return heal_payload_schema(inferred)
    merged = merge_json_schemas(normalized, inferred)
    return heal_payload_schema(merged)


def payload_schema_format(schema: Any) -> str | None:
    if schema is None:
        return None
    if is_canonical_json_schema(schema):
        return "json_schema"
    if is_manual_field_map(schema):
        return "manual_map"
    return "unknown"


def payload_schema_to_manual_map(schema: Any) -> dict[str, Any]:
    """Flat field map for transaction-type editor UI."""
    normalized = _safe_normalize_payload_schema(schema)
    if not normalized:
        return {}
    props = normalized.get("properties") or {}
    if not isinstance(props, dict):
        return {}
    out: dict[str, Any] = {}
    for name, spec in props.items():
        if not isinstance(name, str) or not isinstance(spec, dict):
            continue
        entry: dict[str, Any] = {"type": _normalize_field_type(spec)}
        if spec.get("description"):
            entry["description"] = str(spec["description"])
        out[name] = entry
    return out


def payload_schema_field_paths(schema: Any, *, prefix: str = "payload") -> list[str]:
    out: list[str] = []

    def walk(node: Any, path: str, depth: int = 0):
        if depth > 12 or not isinstance(node, dict):
            return
        node_type = node.get("type")
        if node_type == "object" or "properties" in node:
            props = node.get("properties")
            if isinstance(props, dict):
                for k, v in props.items():
                    if not isinstance(k, str) or not k:
                        continue
                    next_path = f"{path}.{k}" if path else k
                    out.append(next_path)
                    walk(v, next_path, depth + 1)
            return
        if node_type == "array":
            items = node.get("items")
            if items is not None:
                walk(items, path, depth + 1)

    normalized = _safe_normalize_payload_schema(schema)
    if normalized:
        walk(normalized, prefix)
    return sorted({p for p in out if p.startswith(prefix)})


def _field_recommendation(name: str, transaction_type_key: str | None) -> dict[str, Any] | None:
    hints = get_transaction_type_rule_hints(transaction_type_key or "")
    if not hints:
        return None
    rec: dict[str, Any] = {}
    if name == hints.get("productNamesPath", "").replace("payload.", ""):
        rec["role"] = "product_names"
    if name == hints.get("productQuantitiesPath", "").replace("payload.", ""):
        rec["role"] = "product_quantities"
    preferred = hints.get("preferredOrderTotalPath", "").replace("payload.", "")
    if name == preferred:
        rec["role"] = "order_total"
        rec["preferred"] = True
    elif name in {p.replace("payload.", "") for p in hints.get("orderTotalFallbackPaths") or []}:
        rec["role"] = "order_total_fallback"
    return rec or None


def payload_schema_field_catalog(schema: Any, *, transaction_type_key: str | None = None) -> list[dict[str, Any]]:
    """Machine-readable payload fields for UI + rules ($path)."""
    manual = payload_schema_to_manual_map(schema)
    hints = get_transaction_type_rule_hints(transaction_type_key or "")
    items: list[dict[str, Any]] = []
    for name in sorted(manual.keys()):
        spec = manual[name]
        entry: dict[str, Any] = {
            "name": name,
            "type": spec.get("type", "string"),
            "description": spec.get("description"),
            "conditionField": f"payload.{name}",
            "earnPointsPath": name,
            "earnPointsPathFull": f"payload.{name}",
            "dynamicValue": {"$path": f"payload.{name}"},
        }
        rec = _field_recommendation(name, transaction_type_key)
        if rec:
            entry.update(rec)
        items.append(entry)
    if hints:
        for ex in hints.get("earnPointsExamples") or []:
            for item in items:
                if item.get("role") == "order_total" and ex.get("id") == "sale.order_total":
                    item["earnPointsExample"] = ex.get("points")
                if item.get("role") == "product_names" and ex.get("id") == "sale.product_points":
                    item["earnPointsExample"] = ex.get("points")
    return items
