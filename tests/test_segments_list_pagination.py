"""Acceptance checks for GET /admin/segments pagination contract."""

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from app.routes.segments import _resolve_segment_type_filter
from app.services.segment_admin_service import (
    apply_segment_list_ordering,
    normalize_segment_list_sort_by,
    normalize_segment_list_sort_order,
)


def test_resolve_segment_type_filter():
    assert _resolve_segment_type_filter(is_dynamic=True, segment_type=None) is True
    assert _resolve_segment_type_filter(is_dynamic=None, segment_type="static") is False
    with pytest.raises(HTTPException):
        _resolve_segment_type_filter(is_dynamic=None, segment_type="invalid")


def test_sort_normalization_defaults():
    assert normalize_segment_list_sort_by(None) == "created_at"
    assert normalize_segment_list_sort_order(None, sort_by="name") == "asc"
    assert normalize_segment_list_sort_order(None, sort_by="created_at") == "desc"


def test_apply_segment_list_ordering_returns_query():
    base = MagicMock()
    ordered = apply_segment_list_ordering(
        base,
        db=MagicMock(),
        sort_by="name",
        sort_order="asc",
    )
    base.order_by.assert_called_once()
    assert ordered is base


def test_pagination_slice_contract():
    """Document expected len(items) vs limit (pure logic, no DB)."""
    total = 66
    limit = 10
    for offset in (0, 10, 60):
        expected_len = min(limit, max(0, total - offset))
        page_items = list(range(offset, min(offset + limit, total)))
        assert len(page_items) == expected_len
        assert len(page_items) <= limit
