"""Customer lookup by profileId with email fallback (Unomi merge / session mismatch)."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services.contact_service import (
    normalize_lookup_email,
    reconcile_customer_unomi_profile_id,
    resolve_customer_for_lookup,
)


def test_normalize_lookup_email_from_scope_email():
    assert normalize_lookup_email("batira-user@test.com", brand="batira") == "user@test.com"


@patch("app.services.contact_service.get_customer")
def test_resolve_customer_for_lookup_reconciles_profile_id(mock_get_customer):
    db = MagicMock()
    stored = SimpleNamespace(
        brand="batira",
        profile_id="b9c09518-old-session",
        email="new@test.com",
    )
    mock_get_customer.return_value = None
    customer_query = MagicMock()
    customer_query.filter.return_value = customer_query
    customer_query.first.return_value = stored
    tx_query = MagicMock()
    tx_query.filter.return_value = tx_query
    tx_query.update.return_value = 2
    db.query.side_effect = [customer_query, tx_query]

    customer, updated = resolve_customer_for_lookup(
        db,
        brand="batira",
        profile_id="fcbba380-canonical-unomi",
        email="new@test.com",
    )

    assert customer is stored
    assert updated is True
    assert stored.profile_id == "fcbba380-canonical-unomi"
    tx_query.update.assert_called_once()


def test_reconcile_customer_unomi_profile_id_migrates_transactions():
    db = MagicMock()
    customer = SimpleNamespace(brand="batira", profile_id="6de4e574-old", email="yes@gmail.com")
    tx_query = MagicMock()
    tx_query.filter.return_value = tx_query
    tx_query.update.return_value = 4
    db.query.return_value = tx_query

    updated = reconcile_customer_unomi_profile_id(
        db,
        brand="batira",
        customer=customer,
        new_profile_id="2289a7d1-new",
    )

    assert updated is True
    assert customer.profile_id == "2289a7d1-new"
    tx_query.update.assert_called_once()


def test_reconcile_customer_unomi_profile_id_noop_when_same_id():
    db = MagicMock()
    customer = SimpleNamespace(brand="batira", profile_id="same-id", email="yes@gmail.com")

    updated = reconcile_customer_unomi_profile_id(
        db,
        brand="batira",
        customer=customer,
        new_profile_id="same-id",
    )

    assert updated is False
    db.query.assert_not_called()
