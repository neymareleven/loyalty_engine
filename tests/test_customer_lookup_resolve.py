"""Customer lookup by profileId with email fallback (Unomi merge / session mismatch)."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services.contact_service import normalize_lookup_email, resolve_customer_for_lookup


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
    query = MagicMock()
    query.filter.return_value = query
    query.first.return_value = stored
    db.query.return_value = query

    customer, updated = resolve_customer_for_lookup(
        db,
        brand="batira",
        profile_id="fcbba380-canonical-unomi",
        email="new@test.com",
    )

    assert customer is stored
    assert updated is True
    assert stored.profile_id == "fcbba380-canonical-unomi"
