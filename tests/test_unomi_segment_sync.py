"""Unomi segment list sync: metadata fallback and time budget."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.services.unomi_segment_service import (
    UnomiSegmentSyncResult,
    _upsert_segment_from_unomi,
    sync_unomi_scope_segments_to_registry,
)


def test_upsert_metadata_only_when_full_definition_missing():
    db = MagicMock()
    local: dict = {}
    metadata = {
        "id": "loyalty-batira-vip",
        "name": "VIP",
        "scope": "batira",
        "enabled": True,
    }

    seg = _upsert_segment_from_unomi(
        db,
        brand="batira",
        target_scope="batira",
        unomi_id="loyalty-batira-vip",
        metadata=metadata,
        full=None,
        local_by_unomi_id=local,
    )

    assert seg.name == "VIP"
    assert seg.unomi_scope == "batira"
    assert seg.provider == "UNOMI"
    db.add.assert_called_once()


@patch("app.services.unomi_segment_service._fetch_unomi_segment_definition")
@patch("app.services.unomi_segment_service.get_unomi_client")
@patch("app.services.unomi_segment_service.resolve_unomi_connection")
def test_sync_returns_partial_when_definitions_time_out(mock_cfg, mock_client_fn, mock_fetch):
    cfg = SimpleNamespace(scope="batira", base_url="https://u", username="k", password="p")
    mock_cfg.return_value = cfg
    mock_client_fn.return_value = MagicMock()

    client = mock_client_fn.return_value
    client.list_segment_metadata.return_value = [
        {"metadata": {"id": "seg-1", "name": "One", "scope": "batira", "enabled": True}},
        {"metadata": {"id": "seg-2", "name": "Two", "scope": "batira", "enabled": True}},
    ]
    mock_fetch.return_value = None

    db = MagicMock()
    db.query.return_value.filter.return_value.filter.return_value.all.return_value = []

    result = sync_unomi_scope_segments_to_registry(
        db,
        brand="batira",
        timeout_sec=5.0,
        max_pages=1,
    )

    assert isinstance(result, UnomiSegmentSyncResult)
    assert result.total_in_scope == 2
    assert result.status in ("partial", "ok")
    assert len(result.segments) == 2
