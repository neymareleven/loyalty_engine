"""Post-processing sync after business transactions."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services.transaction_service import (
    _maybe_normalize_business_payload,
    _sync_customer_after_business_transaction,
)


def test_normalize_sale_payload_on_ingest():
    payload = _maybe_normalize_business_payload(
        transaction_type="sale",
        payload={"orderTotal": "525 CFA"},
    )
    assert payload["orderTotal"] == 525


@patch("app.services.transaction_service.sync_customer_profile_to_unomi")
@patch("app.services.transaction_service.resolve_customer_for_transaction")
def test_sync_after_processed_sale(mock_resolve, mock_sync):
    db = MagicMock()
    customer = SimpleNamespace(id="c1", brand="batira", profile_id="p1")
    mock_resolve.return_value = customer
    tx = SimpleNamespace(
        status="PROCESSED",
        transaction_type="sale",
        brand="batira",
        profile_id="p1",
        payload={"orderTotal": 525},
    )

    _sync_customer_after_business_transaction(db, transaction=tx)

    mock_sync.assert_called_once()
    assert mock_sync.call_args.kwargs["transport_override"] == "profiles"
    assert mock_sync.call_args.kwargs["reason"] == "transaction_sale"
