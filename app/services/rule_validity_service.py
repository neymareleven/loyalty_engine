"""Rule execution validity window (promo start / end dates)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.models.rule import Rule


def _as_naive_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _parse_datetime_value(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return _as_naive_utc(value)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            ts = float(value)
            if ts > 1e12:
                ts /= 1000.0
            return datetime.utcfromtimestamp(ts)
        except (OSError, ValueError, OverflowError):
            return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            iso = s[:-1] + "+00:00" if s.endswith("Z") else s
            return _as_naive_utc(datetime.fromisoformat(iso))
        except ValueError:
            return None
    return None


def validate_rule_validity_window(
    *,
    valid_from: datetime | None,
    valid_until: datetime | None,
) -> None:
    start = _as_naive_utc(valid_from)
    end = _as_naive_utc(valid_until)
    if start and end and start > end:
        raise ValueError("valid_from must be before or equal to valid_until")


def transaction_evaluation_time(transaction) -> datetime:
    """Instant used to decide whether a rule is inside its validity window."""
    payload = transaction.payload if isinstance(getattr(transaction, "payload", None), dict) else {}
    for key in ("orderDate", "order_date", "eventDate", "event_date"):
        parsed = _parse_datetime_value(payload.get(key))
        if parsed is not None:
            return parsed
    created = getattr(transaction, "created_at", None)
    if isinstance(created, datetime):
        return _as_naive_utc(created) or datetime.utcnow()
    return datetime.utcnow()


def rule_validity_status(
    rule: Rule,
    *,
    evaluation_at: datetime,
) -> tuple[bool, str | None, dict[str, Any]]:
    """
    Returns (is_active, skip_reason, details).
    Window is inclusive: valid_from <= evaluation_at <= valid_until.
    """
    at = _as_naive_utc(evaluation_at) or datetime.utcnow()
    start = _as_naive_utc(getattr(rule, "valid_from", None))
    end = _as_naive_utc(getattr(rule, "valid_until", None))

    details: dict[str, Any] = {
        "evaluationAt": at.isoformat(),
    }
    if start is not None:
        details["validFrom"] = start.isoformat()
    if end is not None:
        details["validUntil"] = end.isoformat()

    if start is not None and at < start:
        return False, "before_valid_from", details
    if end is not None and at > end:
        return False, "after_valid_until", details
    return True, None, details
