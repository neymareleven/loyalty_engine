"""Rule validity window (valid_from / valid_until)."""

from datetime import datetime
from types import SimpleNamespace

import pytest

from app.services.rule_validity_service import (
    rule_validity_status,
    transaction_evaluation_time,
    validate_rule_validity_window,
)


def _rule(**kwargs):
    return SimpleNamespace(
        valid_from=kwargs.get("valid_from"),
        valid_until=kwargs.get("valid_until"),
    )


def _tx(**kwargs):
    return SimpleNamespace(
        created_at=kwargs.get("created_at", datetime(2026, 7, 15, 12, 0, 0)),
        payload=kwargs.get("payload", {}),
    )


def test_validate_rule_validity_window_rejects_inverted_range():
    with pytest.raises(ValueError, match="valid_from"):
        validate_rule_validity_window(
            valid_from=datetime(2026, 8, 1),
            valid_until=datetime(2026, 7, 1),
        )


def test_rule_validity_before_start_is_skipped():
    rule = _rule(valid_from=datetime(2026, 8, 1), valid_until=datetime(2026, 8, 31))
    ok, reason, _ = rule_validity_status(rule, evaluation_at=datetime(2026, 7, 31, 23, 59, 59))
    assert ok is False
    assert reason == "before_valid_from"


def test_rule_validity_after_end_is_skipped():
    rule = _rule(valid_from=datetime(2026, 8, 1), valid_until=datetime(2026, 8, 31, 23, 59, 59))
    ok, reason, _ = rule_validity_status(rule, evaluation_at=datetime(2026, 9, 1, 0, 0, 1))
    assert ok is False
    assert reason == "after_valid_until"


def test_rule_validity_inclusive_boundaries():
    rule = _rule(valid_from=datetime(2026, 8, 1), valid_until=datetime(2026, 8, 31, 23, 59, 59))
    assert rule_validity_status(rule, evaluation_at=datetime(2026, 8, 1, 0, 0, 0))[0] is True
    assert rule_validity_status(rule, evaluation_at=datetime(2026, 8, 31, 23, 59, 59))[0] is True


def test_rule_without_window_always_valid():
    rule = _rule()
    ok, reason, _ = rule_validity_status(rule, evaluation_at=datetime(2026, 1, 1))
    assert ok is True
    assert reason is None


def test_transaction_evaluation_time_prefers_order_date():
    tx = _tx(
        created_at=datetime(2026, 7, 1),
        payload={"orderDate": "2026-06-15T10:30:00Z"},
    )
    assert transaction_evaluation_time(tx) == datetime(2026, 6, 15, 10, 30, 0)
