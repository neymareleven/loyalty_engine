"""Resolve {$system: ...} preset values for rules, segments, and Unomi translation."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Literal

PresetContext = Literal["birthdate", "datetime", "generic"]


def resolve_system_preset_value(
    value: dict[str, Any],
    *,
    context: PresetContext = "generic",
) -> Any:
    """Resolve a {$system, format?, add_days?} dict to a concrete value."""
    if not isinstance(value, dict) or "$system" not in value:
        raise ValueError("Expected system preset dict with '$system'")

    key = value.get("$system")
    if not isinstance(key, str) or not key.strip():
        raise ValueError("Invalid system preset: expected non-empty '$system' string")
    key = key.strip()

    now = datetime.utcnow()
    today = now.date()

    if key == "now":
        base: date | datetime = now
    elif key == "today":
        base = today
    elif key == "weekday":
        return int(now.weekday())
    elif key in {"customer_created_days", "customer_last_activity_days"}:
        raise ValueError(f"System preset '{key}' is not supported in this context")
    else:
        raise ValueError(f"Unknown system value preset: {key}")

    add_days = value.get("add_days")
    if add_days is not None:
        try:
            add_days_int = int(add_days)
        except (TypeError, ValueError) as e:
            raise ValueError("System preset 'add_days' must be an integer") from e
        if isinstance(base, datetime):
            base = base + timedelta(days=add_days_int)
        else:
            base = base + timedelta(days=add_days_int)

    fmt = value.get("format")
    if fmt is not None:
        fmt_norm = str(fmt).strip().lower()
        if fmt_norm == "mmdd":
            d = base.date() if isinstance(base, datetime) else base
            return f"{int(d.month):02d}-{int(d.day):02d}"
        if fmt_norm == "mm":
            d = base.date() if isinstance(base, datetime) else base
            return f"{int(d.month):02d}"
        if fmt_norm == "yyyy":
            d = base.date() if isinstance(base, datetime) else base
            return f"{int(d.year):04d}"
        raise ValueError(f"Unknown system preset format: {fmt}")

    if context == "birthdate" and key == "today":
        d = base.date() if isinstance(base, datetime) else base
        return f"{int(d.month):02d}-{int(d.day):02d}"

    if context == "datetime":
        if isinstance(base, datetime):
            return base.isoformat()
        return datetime(base.year, base.month, base.day).isoformat()

    return base
