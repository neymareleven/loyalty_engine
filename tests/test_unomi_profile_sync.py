"""Unomi profile payload build + contact property merge."""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from app.services.unomi_profile_service import (
    build_customer_identity_unomi_properties,
    build_unomi_eventcollector_payload,
    build_unomi_profile_payload,
    merge_unomi_profile_properties,
    reset_profile_sync_source,
    set_profile_sync_source,
    should_skip_unomi_profile_push,
    _derive_scope_email,
)


def _customer(**kwargs):
    defaults = {
        "brand": "batira",
        "profile_id": "prof-1",
        "email": "ada@example.com",
        "gender": "F",
        "birthdate": None,
        "birth_month": 6,
        "birth_day": 12,
        "birth_year": 1990,
        "status": "ACTIVE",
        "loyalty_status": "BRONZE",
        "status_points": 100,
        "last_activity_at": None,
        "loyalty_status_assigned_at": None,
        "loyalty_status_expires_at": None,
        "points_expires_at": None,
        "status_points_reset_at": None,
        "created_at": None,
        "updated_at": None,
        "id": "00000000-0000-0000-0000-000000000001",
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_build_payload_merges_contact_properties():
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = None

    body = build_unomi_profile_payload(
        db,
        customer=_customer(),
        scope="batira",
        include_points_balance=False,
        extra_properties={},
    )

    props = body["properties"]
    assert props["email"] == "ada@example.com"
    assert props["scopeEmail"] == "batira-ada@example.com"
    assert props["gender"] == "F"
    assert props["loyaltyStatus"] == "BRONZE"
    assert props["brand"] == "batira"
    assert "firstName" not in props
    assert props["unomiProfileId"] == "prof-1"
    assert body["itemId"] == "prof-1"
    assert body["itemType"] == "profile"
    assert body["systemProperties"]["scope"] == "batira"
    assert body["systemProperties"]["mergeIdentifier"] == "batira-ada@example.com"
    assert body["segments"] == []


def test_identity_from_customer_row():
    identity = build_customer_identity_unomi_properties(_customer())
    assert identity["email"] == "ada@example.com"
    assert identity["brand"] == "batira"
    assert identity["scopeEmail"] == "batira-ada@example.com"
    assert identity["gender"] == "F"


def test_merge_sets_visit_fields_from_loyalty_timestamps():
    created = datetime(2026, 6, 23, 8, 25, 27)
    updated = datetime(2026, 6, 23, 13, 8, 29)
    merged = merge_unomi_profile_properties(
        existing_profile=None,
        customer=_customer(
            created_at=created,
            updated_at=updated,
            last_activity_at=updated,
        ),
        loyalty_program_properties={"loyaltyStatus": "base"},
        extra_properties={},
        profile_id="prof-x",
    )
    assert merged["firstVisit"] == "2026-06-23T08:25:27Z"
    assert merged["lastVisit"] == "2026-06-23T13:08:29Z"


def test_merge_preserves_cdp_first_visit():
    existing = {
        "properties": {
            "firstVisit": "2026-06-18T11:10:47Z",
            "lastVisit": "2026-06-20T10:00:00Z",
        }
    }
    merged = merge_unomi_profile_properties(
        existing_profile=existing,
        customer=_customer(
            email="kevine@gmail.com",
            created_at=datetime(2026, 6, 23, 8, 0, 0),
            updated_at=datetime(2026, 6, 23, 13, 0, 0),
            last_activity_at=datetime(2026, 6, 23, 13, 0, 0),
        ),
        loyalty_program_properties={"loyaltyStatus": "base", "statusPoints": 0},
        extra_properties={},
        profile_id="prof-x",
    )
    assert merged["firstVisit"] == "2026-06-18T11:10:47Z"
    assert merged["lastVisit"] == "2026-06-23T13:00:00Z"


def test_merge_preserves_cdp_visit_fields_when_loyalty_has_no_timestamps():
    existing = {
        "properties": {
            "lastVisit": "2026-06-18T11:10:47Z",
            "firstVisit": "2026-06-18T11:10:47Z",
            "nbOfVisits": 3,
            "email": "kevine@gmail.com",
        }
    }
    merged = merge_unomi_profile_properties(
        existing_profile=existing,
        customer=_customer(email="kevine@gmail.com"),
        loyalty_program_properties={"loyaltyStatus": "base", "statusPoints": 0},
        extra_properties={},
        profile_id="prof-x",
    )
    assert merged["lastVisit"] == "2026-06-18T11:10:47Z"
    assert merged["firstVisit"] == "2026-06-18T11:10:47Z"
    assert merged["nbOfVisits"] == 3
    assert merged["scopeEmail"] == "batira-kevine@gmail.com"
    assert merged["loyaltyStatus"] == "base"
    assert merged["email"] == "kevine@gmail.com"
    assert merged["gender"] == "F"


def test_derive_scope_email_respects_explicit_value():
    assert _derive_scope_email(brand="batira", email="a@b.com", scope_email="custom-scope") == "custom-scope"
    assert _derive_scope_email(brand="batira", email="a@b.com") == "batira-a@b.com"


def test_skip_push_when_sync_source_is_unomi():
    token = set_profile_sync_source("unomi")
    try:
        assert should_skip_unomi_profile_push() is True
    finally:
        reset_profile_sync_source(token)


def test_eventcollector_contact_info_submitted_payload():
    payload = build_unomi_eventcollector_payload(
        profile_id="prof-abc",
        scope="batira",
        properties={"email": "a@b.com", "loyaltyStatus": "BRONZE", "brand": "batira"},
        event_type="contactInfoSubmitted",
    )
    assert payload["profileId"] == "prof-abc"
    assert payload["sessionId"] == "loyalty-prof-abc"
    event = payload["events"][0]
    assert event["eventType"] == "contactInfoSubmitted"
    assert event["scope"] == "batira"
    assert event["properties"]["email"] == "a@b.com"
    assert event["source"]["itemId"] == "loyalty-engine"


def test_eventcollector_update_properties_payload():
    payload = build_unomi_eventcollector_payload(
        profile_id="prof-abc",
        scope="batira",
        properties={"email": "a@b.com", "loyaltyStatus": "GOLD"},
        event_type="updateProperties",
    )
    event = payload["events"][0]
    assert event["eventType"] == "updateProperties"
    assert event["properties"]["targetId"] == "prof-abc"
    assert event["properties"]["add"]["properties.email"] == "a@b.com"
    assert event["properties"]["add"]["properties.loyaltyStatus"] == "GOLD"
