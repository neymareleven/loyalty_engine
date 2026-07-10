"""Non-regression: session aliases require email corroboration."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.services.contact_service import (
    register_unomi_profile_alias,
    resolve_customer_for_lookup,
    resolve_customer_for_transaction,
)


def test_session_alias_refused_without_corroborating_email():
    db = MagicMock()
    customer = SimpleNamespace(
        id="cust-1",
        brand="batira",
        profile_id="master-a",
        email="test17@gmail.com",
    )

    registered = register_unomi_profile_alias(
        db,
        brand="batira",
        customer=customer,
        incoming_profile_id="session-b",
        source="session",
        caller="test",
    )

    assert registered is False
    db.add.assert_not_called()


def test_session_alias_refused_when_emails_differ():
    db = MagicMock()
    customer = SimpleNamespace(
        id="cust-1",
        brand="batira",
        profile_id="master-a",
        email="test@gmail.com",
    )

    registered = register_unomi_profile_alias(
        db,
        brand="batira",
        customer=customer,
        incoming_profile_id="session-b",
        source="session",
        corroborating_email="test17@gmail.com",
        caller="test",
    )

    assert registered is False
    db.add.assert_not_called()


@patch("app.services.contact_service.get_customer")
def test_session_alias_allowed_when_emails_match(mock_get_customer):
    db = MagicMock()
    customer = SimpleNamespace(
        id="cust-1",
        brand="batira",
        profile_id="master-a",
        email="test17@gmail.com",
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
        incoming_profile_id="session-b",
        source="session",
        corroborating_email="test17@gmail.com",
        caller="test",
    )

    assert registered is True
    db.add.assert_called_once()


@patch("app.services.contact_service.register_unomi_profile_alias")
@patch("app.services.contact_service.get_customer")
def test_lookup_trusts_email_when_profile_alias_points_to_other_customer(
    mock_get_customer,
    mock_register,
):
    db = MagicMock()
    wrong_customer = SimpleNamespace(
        id="cust-old",
        brand="batira",
        profile_id="6b8555e2-master",
        email="test@gmail.com",
    )
    right_customer = SimpleNamespace(
        id="cust-new",
        brand="batira",
        profile_id="new-master",
        email="test17@gmail.com",
    )

    def get_customer_side_effect(_db, brand, profile_id):
        if profile_id == "87a759c2-session":
            return wrong_customer
        return None

    mock_get_customer.side_effect = get_customer_side_effect

    email_query = MagicMock()
    email_query.filter.return_value = email_query
    email_query.first.return_value = right_customer
    db.query.return_value = email_query
    mock_register.return_value = True

    customer, registered = resolve_customer_for_lookup(
        db,
        brand="batira",
        profile_id="87a759c2-session",
        email="test17@gmail.com",
    )

    assert customer is right_customer
    assert registered is True
    mock_register.assert_called_once()
    kwargs = mock_register.call_args.kwargs
    assert kwargs["corroborating_email"] == "test17@gmail.com"
    assert kwargs["caller"] == "resolve_customer_for_lookup"


@patch("app.services.contact_service.get_customer")
def test_lookup_rejects_profile_when_email_contradicts(mock_get_customer):
    db = MagicMock()
    profile_customer = SimpleNamespace(
        id="cust-old",
        brand="batira",
        profile_id="6b8555e2-master",
        email="test@gmail.com",
    )
    mock_get_customer.return_value = profile_customer

    email_query = MagicMock()
    email_query.filter.return_value = email_query
    email_query.first.return_value = None
    db.query.return_value = email_query

    customer, registered = resolve_customer_for_lookup(
        db,
        brand="batira",
        profile_id="6b8555e2-master",
        email="test17@gmail.com",
    )

    assert customer is None
    assert registered is False


@patch("app.services.contact_service.register_unomi_profile_alias")
@patch("app.services.contact_service.get_customer")
def test_transaction_rejects_profile_owner_with_different_email(mock_get_customer, mock_register):
    db = MagicMock()
    orphan = SimpleNamespace(
        id="cust-orphan",
        brand="batira",
        profile_id="87a759c2-session",
        email="test@gmail.com",
    )
    mock_get_customer.return_value = orphan

    email_query = MagicMock()
    email_query.filter.return_value = email_query
    email_query.first.return_value = None
    db.query.return_value = email_query

    out = resolve_customer_for_transaction(
        db,
        brand="batira",
        profile_id="87a759c2-session",
        payload={"billing_email": "test17@gmail.com", "orderNumber": "7026"},
    )

    assert out is None
    mock_register.assert_not_called()
