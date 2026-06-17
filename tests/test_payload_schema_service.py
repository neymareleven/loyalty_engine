"""Payload schema parsing for transaction types and rules."""

import pytest

from app.services.payload_schema_service import (
    enrich_payload_schema_on_ingest,
    get_transaction_type_rule_hints,
    is_mistaken_json_schema_root_as_fields,
    merge_json_schemas,
    normalize_payload_schema_for_storage,
    payload_schema_field_catalog,
    payload_schema_field_paths,
    payload_schema_to_manual_map,
)


SALE_SCHEMA = {
    "type": "object",
    "properties": {
        "brand": {"type": "string"},
        "orderTotal": {"type": "string"},
        "total": {"type": "string"},
        "tva": {"type": "integer"},
        "productNames": {"type": "array", "items": {"type": "string"}},
        "productQuantities": {"type": "array", "items": {"type": "integer"}},
    },
}


def test_field_paths_from_json_schema():
    paths = payload_schema_field_paths(SALE_SCHEMA, prefix="payload")
    assert "payload.brand" in paths
    assert "payload.total" in paths
    assert "payload.tva" in paths
    assert "payload.productNames" in paths
    assert "payload.type" not in paths


def test_manual_map_for_editor():
    manual = payload_schema_to_manual_map(SALE_SCHEMA)
    assert set(manual.keys()) == {
        "brand",
        "orderTotal",
        "total",
        "tva",
        "productNames",
        "productQuantities",
    }
    assert manual["orderTotal"]["type"] == "string"


def test_field_catalog_includes_earn_points_path():
    catalog = payload_schema_field_catalog(SALE_SCHEMA, transaction_type_key="sale")
    order_total = next(x for x in catalog if x["name"] == "orderTotal")
    assert order_total["earnPointsPath"] == "orderTotal"
    assert order_total["dynamicValue"] == {"$path": "payload.orderTotal"}
    assert order_total.get("role") == "order_total"
    assert order_total.get("preferred") is True
    assert order_total.get("earnPointsExample") == {
        "$fn": "to_number",
        "args": [{"$path": "payload.orderTotal"}],
    }
    products = next(x for x in catalog if x["name"] == "productNames")
    assert products.get("role") == "product_names"


def test_reject_mistaken_root_as_fields():
    bad = {"type": {"type": "string"}, "properties": {"type": "string"}}
    assert is_mistaken_json_schema_root_as_fields(bad) is True
    with pytest.raises(ValueError, match="JSON Schema racine"):
        normalize_payload_schema_for_storage(bad)


def test_manual_map_normalizes_to_json_schema():
    stored = normalize_payload_schema_for_storage(
        {"total": {"type": "string", "description": "Montant"}}
    )
    assert stored["type"] == "object"
    assert stored["properties"]["total"]["type"] == "string"


def test_enrich_schema_from_empty_on_first_ingest():
    payload = {"orderTotal": "12000", "productNames": ["sku-a"]}
    merged = enrich_payload_schema_on_ingest(None, payload)
    assert merged is not None
    props = merged["properties"]
    assert "orderTotal" in props
    assert "productNames" in props


def test_enrich_schema_merges_new_fields_on_subsequent_ingest():
    existing = {
        "type": "object",
        "properties": {
            "orderTotal": {"type": "string"},
            "brand": {"type": "string"},
        },
    }
    payload = {"orderTotal": "5000", "productQuantities": [2, 1]}
    merged = enrich_payload_schema_on_ingest(existing, payload)
    props = merged["properties"]
    assert "orderTotal" in props
    assert "brand" in props
    assert "productQuantities" in props


def test_enrich_schema_heals_corrupted_root_as_fields():
    corrupted = {"type": {"type": "string"}, "properties": {"type": "string"}}
    assert is_mistaken_json_schema_root_as_fields(corrupted)
    payload = {"orderTotal": "99", "productNames": ["x"]}
    healed = enrich_payload_schema_on_ingest(corrupted, payload)
    props = healed["properties"]
    assert "orderTotal" in props
    assert "productNames" in props


def test_sale_rule_hints_order_total_and_products():
    hints = get_transaction_type_rule_hints("sale")
    assert hints["preferredOrderTotalPath"] == "payload.orderTotal"
    assert hints["productNamesPath"] == "payload.productNames"
    assert hints["productQuantitiesPath"] == "payload.productQuantities"
    example_ids = {ex["id"] for ex in hints["earnPointsExamples"]}
    assert "sale.order_total" in example_ids
    assert "sale.product_points" in example_ids


def test_merge_json_schemas_keeps_both_object_properties():
    a = {"type": "object", "properties": {"orderTotal": {"type": "string"}}}
    b = {"type": "object", "properties": {"productNames": {"type": "array", "items": {"type": "string"}}}}
    merged = merge_json_schemas(a, b)
    assert set(merged["properties"].keys()) == {"orderTotal", "productNames"}
