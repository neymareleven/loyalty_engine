"""Customer lookup by profileId with email fallback (Unomi merge / session mismatch)."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services.contact_service import (
    normalize_lookup_email,
    register_unomi_profile_alias,
    resolve_customer_for_lookup,
)


def test_normalize_lookup_email_from_scope_email():
    assert normalize_lookup_email("batira-user@test.com", brand="batira") == "user@test.com"


@patch("app.services.contact_service.register_unomi_profile_alias")
@patch("app.services.contact_service.get_customer")
def test_resolve_customer_for_lookup_registers_alias_by_email(mock_get_customer, mock_register):
    db = MagicMock()
    stored = SimpleNamespace(
        brand="batira",
        profile_id="6de4e574-master",
        email="new@test.com",
    )
    mock_get_customer.return_value = None
    customer_query = MagicMock()
    customer_query.filter.return_value = customer_query
    customer_query.first.return_value = stored
    db.query.return_value = customer_query
    mock_register.return_value = True

    customer, updated = resolve_customer_for_lookup(
        db,
        brand="batira",
        profile_id="2289a7d1-session",
        email="new@test.com",
    )

    assert customer is stored
    assert updated is True
    assert stored.profile_id == "6de4e574-master"
    mock_register.assert_called_once_with(
        db,
        brand="batira",
        customer=stored,
        incoming_profile_id="2289a7d1-session",
    )


@patch("app.services.contact_service.get_customer")
def test_register_unomi_profile_alias_keeps_master_and_does_not_touch_transactions(mock_get_customer):
    db = MagicMock()
    customer = SimpleNamespace(
        id="cust-1",
        brand="batira",
        profile_id="6de4e574-master",
        email="yes@gmail.com",
    )
    mock_get_customer.return_value = None

    alias_query = MagicMock()
    alias_query.filter.return_value = alias_query
    alias_query.first.return_value = None
    db.query.return_value = alias_query
    db.add = MagicMock()

    registered = register_unomi_profile_alias(
        db,
        brand="batira",
        customer=customer,
        incoming_profile_id="2289a7d1-session",
        source="session",
    )

    assert registered is True
    assert customer.profile_id == "6de4e574-master"
    db.add.assert_called_once()


def test_register_unomi_profile_alias_noop_when_same_id():
    db = MagicMock()
    customer = SimpleNamespace(brand="batira", profile_id="same-id", email="yes@gmail.com")

    registered = register_unomi_profile_alias(
        db,
        brand="batira",
        customer=customer,
        incoming_profile_id="same-id",
    )

    assert registered is False
    db.query.assert_not_called()
