"""Post-processing after business transactions."""

from app.services.transaction_service import _maybe_normalize_business_payload


def test_normalize_sale_payload_on_ingest():
    payload = _maybe_normalize_business_payload(
        transaction_type="sale",
        payload={"orderTotal": "525 CFA"},
    )
    assert payload["orderTotal"] == 525
