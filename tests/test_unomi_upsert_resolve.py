"""Unomi upsert must not hijack an existing customer when session profileId is recycled."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services.contact_service import resolve_customer_for_unomi_upsert


@patch("app.services.unomi_profile_service.get_unomi_profile_client")
def test_unomi_upsert_creates_new_customer_when_profile_id_recycled(mock_client):
    db = MagicMock()
    user1 = SimpleNamespace(
        brand="batira",
        profile_id="session-profile-a",
        email="user1@test.com",
        gender=None,
        birthdate=None,
        birth_month=None,
        birth_day=None,
        birth_year=None,
    )

    def query_side(model):
        q = MagicMock()
        if model.__name__ == "Customer":
            q.filter.return_value = q

            def first_impl():
                if not hasattr(first_impl, "n"):
                    first_impl.n = 0
                first_impl.n += 1
                if first_impl.n == 1:
                    return None  # by email user2
                return user1  # by profile session-profile-a

            q.first.side_effect = first_impl
        return q

    db.query.side_effect = query_side
    client = MagicMock()
    client.resolve_canonical_profile_id.return_value = "canonical-profile-b"
    mock_client.return_value = client

    created = []

    def add_side(obj):
        created.append(obj)

    db.add.side_effect = add_side

    customer, existed, profile_id = resolve_customer_for_unomi_upsert(
        db,
        brand="batira",
        incoming_profile_id="session-profile-a",
        norm_email="user2@test.com",
        identity_payload={"email": "user2@test.com"},
    )

    assert existed is False
    assert profile_id == "canonical-profile-b"
    assert customer is created[0]
    assert customer.email == "user2@test.com"
    assert user1.email == "user1@test.com"
