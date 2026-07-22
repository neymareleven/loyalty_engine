"""Business events for unregistered customers are ignored (no rules, no points)."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services.transaction_service import create_transaction


@patch("app.services.transaction_service.process_transaction_rules")
@patch("app.services.transaction_service.resolve_customer_for_transaction", return_value=None)
@patch("app.services.transaction_service._find_transaction_type")
def test_create_transaction_ignores_unregistered_customer(mock_find_tt, mock_resolve, mock_process):
    db = MagicMock()
    query = MagicMock()
    query.filter.return_value = query
    query.first.return_value = None
    db.query.return_value = query
    db.refresh.side_effect = lambda obj: None

    mock_find_tt.return_value = SimpleNamespace(payload_schema=None, key="sale")

    captured = []

    def capture_add(obj):
        captured.append(obj)

    db.add.side_effect = capture_add

    event = SimpleNamespace(
        brand="batira",
        profileId="unknown-profile",
        eventType="sale",
        eventId="order-9001",
        source="UNOMI",
        payload={"billing_email": "guest@example.com", "orderNumber": "9001"},
    )

    tx = create_transaction(db, event)

    assert tx is captured[0]
    assert tx.status == "IGNORED"
    assert tx.error_code == "CUSTOMER_NOT_REGISTERED"
    mock_process.assert_not_called()
