"""Payload normalization and schema typing on ingest."""

from app.services.payload_normalization_service import normalize_transaction_payload
from app.services.payload_schema_service import enrich_payload_schema_on_ingest, heal_payload_schema


def test_normalize_sale_monetary_and_quantities():
    out = normalize_transaction_payload(
        {
            "orderTotal": "525 CFA",
            "subtotal": "500",
            "productPrices": ["475 CFA", "50 CFA"],
            "productQuantities": ["1", "2"],
            "orderDate": "15/03/2024",
            "billing_email": " Ada@Example.COM ",
            "billing_phone": "+237 6 99 00 11 22",
            "isLoggedIn": "true",
        },
        transaction_type="sale",
    )
    assert out["orderTotal"] == 525
    assert out["orderTotalRaw"] == "525 CFA"
    assert out["subtotal"] == 500
    assert out["productPrices"] == [475, 50]
    assert out["productQuantities"] == [1, 2]
    assert out["orderDate"].endswith("Z")
    assert out["billing_email"] == "ada@example.com"
    assert out["billing_phone"] == "+237699001122"
    assert out["isLoggedIn"] is True


def test_enrich_schema_heals_legacy_string_types():
    existing = {
        "type": "object",
        "properties": {
            "orderTotal": {"type": "string"},
            "orderDate": {"type": "string"},
            "billing_email": {"type": "string"},
            "billing_phone": {"type": "string"},
            "isLoggedIn": {"type": "string"},
            "productQuantities": {"type": "array", "items": {"type": "string"}},
        },
    }
    payload = normalize_transaction_payload(
        {
            "orderTotal": 525,
            "orderDate": "2024-03-15T00:00:00Z",
            "billing_email": "ada@example.com",
            "billing_phone": "699001122",
            "isLoggedIn": True,
            "productQuantities": [1, 2],
        },
        transaction_type="sale",
    )
    merged = enrich_payload_schema_on_ingest(existing, payload)
    props = merged["properties"]
    assert props["orderTotal"]["type"] == "integer"
    assert props["orderDate"]["format"] == "date-time"
    assert props["billing_email"]["format"] == "email"
    assert props["billing_phone"]["format"] == "phone"
    assert props["isLoggedIn"]["type"] == "boolean"
    assert props["productQuantities"]["items"]["type"] == "integer"


def test_heal_payload_schema_without_new_payload():
    schema = heal_payload_schema(
        {
            "type": "object",
            "properties": {
                "total": {"type": "string"},
                "orderTotalRaw": {"type": "string"},
            },
        }
    )
    assert schema["properties"]["total"]["type"] == "integer"
    assert schema["properties"]["orderTotalRaw"]["type"] == "string"
