"""Sale ingest: resolve customer by email / profile merge / retry BLOCKED."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services.contact_service import (
    _extract_email_from_payload,
    resolve_customer_for_transaction,
)
from app.services.transaction_service import _retry_blocked_customer_not_found


def test_extract_email_from_billing_and_scope():
    assert _extract_email_from_payload({"billing_email": "A@B.com"}, brand="batira") == "a@b.com"
    assert (
        _extract_email_from_payload({"scopeEmail": "batira-new@gmail.com"}, brand="batira")
        == "new@gmail.com"
    )


@patch("app.services.contact_service.register_unomi_profile_alias")
@patch("app.services.contact_service.get_customer")
def test_resolve_links_customer_by_email_when_profile_id_changed(mock_get_customer, mock_register):
    db = MagicMock()
    merged_customer = SimpleNamespace(
        id="cust-1",
        brand="batira",
        profile_id="old-profile-id",
        email="new@gmail.com",
    )
    mock_get_customer.return_value = None
    customer_query = MagicMock()
    customer_query.filter.return_value = customer_query
    customer_query.first.return_value = merged_customer
    db.query.return_value = customer_query
    mock_register.return_value = True

    out = resolve_customer_for_transaction(
        db,
        brand="batira",
        profile_id="new-unomi-profile-id",
        payload={"billing_email": "new@gmail.com", "orderNumber": "7001"},
    )

    assert out is merged_customer
    assert merged_customer.profile_id == "old-profile-id"
    mock_register.assert_called_once()


@patch("app.services.contact_service.register_unomi_profile_alias")
@patch("app.services.contact_service.get_customer")
def test_resolve_transaction_email_wins_over_different_profile_master(mock_get_customer, mock_register):
    db = MagicMock()
    master = SimpleNamespace(
        id="cust-master",
        brand="batira",
        profile_id="6b8555e2-master",
        email="test17@gmail.com",
    )
    orphan = SimpleNamespace(
        id="cust-orphan",
        brand="batira",
        profile_id="87a759c2-session",
        email="other@gmail.com",
    )

    def get_customer_side_effect(_db, brand, pid):
        if pid == "87a759c2-session":
            return orphan
        return None

    mock_get_customer.side_effect = get_customer_side_effect

    email_query = MagicMock()
    email_query.filter.return_value = email_query
    email_query.first.return_value = master
    db.query.return_value = email_query

    out = resolve_customer_for_transaction(
        db,
        brand="batira",
        profile_id="87a759c2-session",
        payload={"billing_email": "test17@gmail.com", "orderNumber": "7026"},
    )

    assert out is master
    mock_register.assert_called_once()


@patch("app.services.transaction_service.process_transaction_rules")
def test_retry_blocked_reprocesses_when_customer_now_exists(mock_process):
    db = MagicMock()
    tx = SimpleNamespace(
        status="BLOCKED",
        error_code="CUSTOMER_NOT_FOUND",
        error_message="Customer not found",
        brand="batira",
        profile_id="p1",
        payload={"billing_email": "x@y.com"},
        processed_at=None,
        error_code_set=None,
    )

    with patch(
        "app.services.transaction_service.resolve_customer_for_transaction",
        return_value=SimpleNamespace(id="cust-1"),
    ):
        result = _retry_blocked_customer_not_found(db, tx)

    assert result.status == "PENDING"
    mock_process.assert_called_once()
