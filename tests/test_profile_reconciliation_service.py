"""Profile view reconciliation (CDP profileId vs loyalty master)."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services.profile_reconciliation_service import reconcile_profile_view


def test_no_reconciliation_when_profile_ids_match():
    customer = SimpleNamespace(
        brand="batira",
        profile_id="same-id",
        email="user@test.com",
    )
    result = reconcile_profile_view(
        MagicMock(),
        brand="batira",
        requested_profile_id="same-id",
        email="user@test.com",
        customer=customer,
        alias_registered=False,
        sync_unomi=False,
    )
    assert result is None


@patch("app.services.unomi_profile_service.sync_customer_profile_to_unomi")
def test_silent_reconciliation_when_email_and_brand_match(mock_sync):
    mock_sync.return_value = {"synced": True, "profileId": "cdp-id"}
    customer = SimpleNamespace(
        brand="batira",
        profile_id="loyalty-master",
        email="user@test.com",
    )
    result = reconcile_profile_view(
        MagicMock(),
        brand="batira",
        requested_profile_id="cdp-id",
        email="user@test.com",
        customer=customer,
        alias_registered=True,
        sync_unomi=True,
    )
    assert result is not None
    assert result.show_merge_notice is False
    assert result.reconciled_by == "email"
    assert result.requested_profile_id == "cdp-id"
    assert result.loyalty_profile_id == "loyalty-master"
    mock_sync.assert_called_once()
    assert mock_sync.call_args.kwargs["target_profile_id"] == "cdp-id"


def test_show_merge_notice_when_email_missing():
    customer = SimpleNamespace(
        brand="batira",
        profile_id="loyalty-master",
        email="user@test.com",
    )
    result = reconcile_profile_view(
        MagicMock(),
        brand="batira",
        requested_profile_id="cdp-id",
        email=None,
        customer=customer,
        alias_registered=False,
        sync_unomi=False,
    )
    assert result is not None
    assert result.show_merge_notice is True
    assert result.reconciled_by is None


def test_show_merge_notice_when_email_mismatch():
    customer = SimpleNamespace(
        brand="batira",
        profile_id="loyalty-master",
        email="owner@test.com",
    )
    result = reconcile_profile_view(
        MagicMock(),
        brand="batira",
        requested_profile_id="cdp-id",
        email="other@test.com",
        customer=customer,
        alias_registered=False,
        sync_unomi=False,
    )
    assert result is not None
    assert result.show_merge_notice is True
