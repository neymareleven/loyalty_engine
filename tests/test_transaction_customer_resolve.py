"""Sale ingest: resolve customer by email / profile merge / retry BLOCKED."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services.contact_service import (
    _extract_email_from_payload,
    _extract_trusted_identity_email_from_payload,
    resolve_customer_for_transaction,
)
from app.services.transaction_service import (
    _ignore_unregistered_customer,
    _retry_ignored_unregistered_customer,
)


def test_extract_email_from_billing_and_scope():
    assert _extract_email_from_payload({"billing_email": "A@B.com"}, brand="batira") == "a@b.com"
    assert (
        _extract_email_from_payload({"scopeEmail": "batira-new@gmail.com"}, brand="batira")
        == "new@gmail.com"
    )


def test_trusted_identity_email_ignores_billing():
    assert _extract_trusted_identity_email_from_payload({"billing_email": "A@B.com"}, brand="batira") is None
    assert (
        _extract_trusted_identity_email_from_payload({"email": "user@example.com"}, brand="batira")
        == "user@example.com"
    )
    assert (
        _extract_trusted_identity_email_from_payload(
            {"scopeEmail": "batira-user@example.com"}, brand="batira"
        )
        == "user@example.com"
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
        payload={"email": "new@gmail.com", "orderNumber": "7001"},
    )

    assert out is merged_customer
    assert merged_customer.profile_id == "old-profile-id"
    mock_register.assert_called_once()


@patch("app.services.contact_service.register_unomi_profile_alias")
@patch("app.services.contact_service.get_customer")
def test_resolve_transaction_rejects_when_trusted_email_contradicts_profile_owner(
    mock_get_customer, mock_register
):
    db = MagicMock()
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

    out = resolve_customer_for_transaction(
        db,
        brand="batira",
        profile_id="87a759c2-session",
        payload={"email": "test17@gmail.com", "orderNumber": "7026"},
    )

    assert out is None
    mock_register.assert_not_called()


@patch("app.services.transaction_service.process_transaction_rules")
def test_retry_ignored_reprocesses_when_customer_now_exists(mock_process):
    db = MagicMock()
    tx = SimpleNamespace(
        status="IGNORED",
        error_code="CUSTOMER_NOT_REGISTERED",
        error_message="Customer not enrolled",
        brand="batira",
        profile_id="p1",
        payload={"billing_email": "x@y.com"},
        processed_at=None,
    )

    with patch(
        "app.services.transaction_service.resolve_customer_for_transaction",
        return_value=SimpleNamespace(id="cust-1"),
    ):
        result = _retry_ignored_unregistered_customer(db, tx)

    assert result.status == "PENDING"
    mock_process.assert_called_once()


@patch("app.services.transaction_service.process_transaction_rules")
def test_retry_blocked_legacy_reprocesses_when_customer_now_exists(mock_process):
    db = MagicMock()
    tx = SimpleNamespace(
        status="BLOCKED",
        error_code="CUSTOMER_NOT_FOUND",
        error_message="Customer not found",
        brand="batira",
        profile_id="p1",
        payload={"billing_email": "x@y.com"},
        processed_at=None,
    )

    with patch(
        "app.services.transaction_service.resolve_customer_for_transaction",
        return_value=SimpleNamespace(id="cust-1"),
    ):
        result = _retry_ignored_unregistered_customer(db, tx)

    assert result.status == "PENDING"
    mock_process.assert_called_once()


def test_ignore_unregistered_customer_sets_status():
    tx = SimpleNamespace(status="PENDING", error_code=None, error_message=None, processed_at=None)
    _ignore_unregistered_customer(tx)
    assert tx.status == "IGNORED"
    assert tx.error_code == "CUSTOMER_NOT_REGISTERED"
    assert tx.processed_at is not None
