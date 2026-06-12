"""Birthdate targeting — shared parsing, comparison, SQL and Unomi translation.

Wire formats (API conditions / selectors / customer profile):
  - full:      YYYY-MM-DD  (e.g. 1990-06-12)
  - day_month: MM-DD       (e.g. 06-12 — anniversary, year ignored)
  - month:     MM or --MM  (e.g. 06 — all born in June)
  - year:      YYYY        (e.g. 1990 — all born in 1990)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any

from sqlalchemy import ColumnElement, and_, or_
from sqlalchemy.sql import false

_FULL_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DAY_MONTH_RE = re.compile(r"^\d{2}-\d{2}$")
_DAY_MONTH_FLEX_RE = re.compile(r"^(\d{1,2})-(\d{1,2})$")
_MONTH_DASH_RE = re.compile(r"^--(\d{1,2})$")
_YEAR_RE = re.compile(r"^\d{4}$")

BIRTHDATE_WIRE_FORMATS = (
    "YYYY-MM-DD (date complète)",
    "MM-DD (jour + mois, anniversaire)",
    "MM ou --MM (mois seul)",
    "YYYY (année seule)",
)

BIRTHDATE_GRANULARITIES_META = {
    "full": {"wireFormat": "YYYY-MM-DD", "example": "1990-06-12", "uiHint": "jj/mm/aaaa"},
    "day_month": {"wireFormat": "MM-DD", "example": "06-12", "uiHint": "jj/mm"},
    "month": {"wireFormat": "MM", "example": "06", "uiHint": "mm"},
    "year": {"wireFormat": "YYYY", "example": "1990", "uiHint": "aaaa"},
}

BIRTHDATE_VALUE_PRESETS = [
    {
        "id": "system.today_mmdd",
        "label": "Anniversaire aujourd'hui (MM-DD)",
        "value": {"$system": "today", "format": "mmdd"},
        "granularity": "day_month",
    },
    {
        "id": "system.today",
        "label": "Anniversaire aujourd'hui (alias)",
        "value": {"$system": "today"},
        "granularity": "day_month",
    },
    {
        "id": "system.today_month",
        "label": "Mois en cours (MM)",
        "value": {"$system": "today", "format": "mm"},
        "granularity": "month",
    },
    {
        "id": "system.today_year",
        "label": "Année en cours (YYYY)",
        "value": {"$system": "today", "format": "yyyy"},
        "granularity": "year",
    },
]

BIRTHDATE_FIELD_META = {
    "valueKind": "date",
    "ui": {"widget": "date"},
    "granularities": BIRTHDATE_GRANULARITIES_META,
    "note": (
        "Formats API : YYYY-MM-DD, MM-DD, MM/--MM, YYYY. "
        "Dynamique anniversaire : {$system:today} → MM-DD du jour (année ignorée). "
        "customer.birthday est un alias API déprécié."
    ),
}


class BirthdateGranularity(str, Enum):
    FULL = "full"
    DAY_MONTH = "day_month"
    MONTH = "month"
    YEAR = "year"


@dataclass(frozen=True)
class CustomerBirthdateParts:
    year: int | None
    month: int | None
    day: int | None

    @property
    def mmdd(self) -> int | None:
        if self.month is None or self.day is None:
            return None
        return int(self.month) * 100 + int(self.day)


@dataclass(frozen=True)
class BirthdateTarget:
    granularity: BirthdateGranularity
    wire: str
    year: int | None = None
    month: int | None = None
    day: int | None = None
    full_date: date | None = None

    @property
    def mmdd(self) -> int | None:
        if self.month is None or self.day is None:
            return None
        return int(self.month) * 100 + int(self.day)


def customer_birthdate_parts(customer) -> CustomerBirthdateParts:
    yy = getattr(customer, "birth_year", None)
    mm = getattr(customer, "birth_month", None)
    dd = getattr(customer, "birth_day", None)
    if yy is not None or mm is not None or dd is not None:
        return CustomerBirthdateParts(
            year=int(yy) if yy is not None else None,
            month=int(mm) if mm is not None else None,
            day=int(dd) if dd is not None else None,
        )
    legacy = getattr(customer, "birthdate", None)
    if legacy is not None and hasattr(legacy, "year"):
        return CustomerBirthdateParts(
            year=int(legacy.year),
            month=int(legacy.month),
            day=int(legacy.day),
        )
    return CustomerBirthdateParts(year=None, month=None, day=None)


def format_customer_birthdate_wire(customer) -> str | None:
    parts = customer_birthdate_parts(customer)
    if parts.year is not None and parts.month is not None and parts.day is not None:
        return f"{parts.year:04d}-{parts.month:02d}-{parts.day:02d}"
    if parts.month is not None and parts.day is not None:
        return f"{parts.month:02d}-{parts.day:02d}"
    if parts.month is not None:
        return f"{parts.month:02d}"
    if parts.year is not None:
        return f"{parts.year:04d}"
    return None


def _validate_month(mm: int) -> int:
    if mm < 1 or mm > 12:
        raise ValueError("birthdate month must be between 01 and 12")
    return mm


def _validate_day(dd: int) -> int:
    if dd < 1 or dd > 31:
        raise ValueError("birthdate day must be between 01 and 31")
    return dd


def _validate_year(yy: int) -> int:
    if yy < 1000 or yy > 9999:
        raise ValueError("birthdate year must be a 4-digit value")
    return yy


def parse_birthdate_wire(value: Any) -> BirthdateTarget:
    """Parse a birthdate condition/selector/customer value into a typed target."""
    if isinstance(value, date) and not isinstance(value, datetime):
        d = value
        wire = d.isoformat()
        return BirthdateTarget(
            granularity=BirthdateGranularity.FULL,
            wire=wire,
            year=d.year,
            month=d.month,
            day=d.day,
            full_date=d,
        )
    if isinstance(value, datetime):
        d = value.date()
        wire = d.isoformat()
        return BirthdateTarget(
            granularity=BirthdateGranularity.FULL,
            wire=wire,
            year=d.year,
            month=d.month,
            day=d.day,
            full_date=d,
        )

    if value is None:
        raise ValueError("birthdate value is required")

    raw = str(value).strip()
    if not raw:
        raise ValueError("birthdate value is required")

    if "/" in raw:
        raise ValueError(
            "birthdate: use YYYY-MM-DD, MM-DD, MM/--MM, or YYYY (not DD/MM/YYYY). "
            "Example: 1990-06-12, 06-12, 06, 1990"
        )

    if _FULL_DATE_RE.match(raw):
        d = date.fromisoformat(raw)
        return BirthdateTarget(
            granularity=BirthdateGranularity.FULL,
            wire=raw,
            year=d.year,
            month=d.month,
            day=d.day,
            full_date=d,
        )

    flex = _DAY_MONTH_FLEX_RE.match(raw)
    if flex and "-" in raw:
        mm = _validate_month(int(flex.group(1)))
        dd = _validate_day(int(flex.group(2)))
        try:
            date(2000, mm, dd)
        except ValueError as e:
            raise ValueError("birthdate MM-DD is not a valid calendar date") from e
        wire = f"{mm:02d}-{dd:02d}"
        return BirthdateTarget(
            granularity=BirthdateGranularity.DAY_MONTH,
            wire=wire,
            month=mm,
            day=dd,
        )

    if _DAY_MONTH_RE.match(raw):
        mm = _validate_month(int(raw[0:2]))
        dd = _validate_day(int(raw[3:5]))
        return BirthdateTarget(
            granularity=BirthdateGranularity.DAY_MONTH,
            wire=raw,
            month=mm,
            day=dd,
        )

    month_dash = _MONTH_DASH_RE.match(raw)
    if month_dash:
        mm = _validate_month(int(month_dash.group(1)))
        wire = f"{mm:02d}"
        return BirthdateTarget(granularity=BirthdateGranularity.MONTH, wire=wire, month=mm)

    if len(raw) in {1, 2} and raw.isdigit():
        mm = _validate_month(int(raw))
        wire = f"{mm:02d}"
        return BirthdateTarget(granularity=BirthdateGranularity.MONTH, wire=wire, month=mm)

    if _YEAR_RE.match(raw):
        yy = _validate_year(int(raw))
        return BirthdateTarget(granularity=BirthdateGranularity.YEAR, wire=f"{yy:04d}", year=yy)

    raise ValueError(
        "birthdate must be YYYY-MM-DD, MM-DD, MM/--MM, or YYYY; "
        f"got: {value!r}"
    )


def parse_customer_birthdate_storage(value: str | None) -> tuple[date | None, int | None, int | None, int | None]:
    """Parse customer profile birthdate input (all granularities). Returns (full_date, month, day, year)."""
    if value is None:
        return None, None, None, None
    s = str(value).strip()
    if not s:
        return None, None, None, None

    target = parse_birthdate_wire(s)
    if target.granularity == BirthdateGranularity.FULL:
        assert target.full_date is not None
        d = target.full_date
        return d, d.month, d.day, d.year
    if target.granularity == BirthdateGranularity.DAY_MONTH:
        return None, target.month, target.day, None
    if target.granularity == BirthdateGranularity.MONTH:
        return None, target.month, None, None
    if target.granularity == BirthdateGranularity.YEAR:
        return None, None, None, target.year
    return None, None, None, None


def _customer_has_granularity(parts: CustomerBirthdateParts, granularity: BirthdateGranularity) -> bool:
    if granularity == BirthdateGranularity.FULL:
        return parts.year is not None and parts.month is not None and parts.day is not None
    if granularity == BirthdateGranularity.DAY_MONTH:
        return parts.month is not None and parts.day is not None
    if granularity == BirthdateGranularity.MONTH:
        return parts.month is not None
    if granularity == BirthdateGranularity.YEAR:
        return parts.year is not None
    return False


def _extract_customer_scalar(parts: CustomerBirthdateParts, granularity: BirthdateGranularity) -> Any:
    if granularity == BirthdateGranularity.FULL:
        if parts.year is None or parts.month is None or parts.day is None:
            return None
        return date(parts.year, parts.month, parts.day)
    if granularity == BirthdateGranularity.DAY_MONTH:
        return parts.mmdd
    if granularity == BirthdateGranularity.MONTH:
        return parts.month
    if granularity == BirthdateGranularity.YEAR:
        return parts.year
    return None


def _extract_target_scalar(target: BirthdateTarget) -> Any:
    if target.granularity == BirthdateGranularity.FULL:
        return target.full_date
    if target.granularity == BirthdateGranularity.DAY_MONTH:
        return target.mmdd
    if target.granularity == BirthdateGranularity.MONTH:
        return target.month
    if target.granularity == BirthdateGranularity.YEAR:
        return target.year
    return None


def _compare_scalars(op: str, actual: Any, expected: Any) -> bool:
    op = (op or "").lower()
    if op in {"eq", "="}:
        return actual == expected
    if op in {"neq", "!=", "ne"}:
        return actual != expected
    if op == "gt":
        return actual > expected
    if op == "gte":
        return actual >= expected
    if op == "lt":
        return actual < expected
    if op == "lte":
        return actual <= expected
    raise ValueError(f"Unsupported birthdate operator: {op}")


def _between_mmdd(actual: int, lo: int, hi: int) -> bool:
    if lo <= hi:
        return lo <= actual <= hi
    return actual >= lo or actual <= hi


def compare_birthdate(*, op: str, customer, expected: Any) -> bool:
    op_norm = (op or "").lower()
    parts = customer_birthdate_parts(customer)

    if op_norm == "exists":
        truthy = True if expected is None else bool(expected)
        has_any = parts.year is not None or parts.month is not None or parts.day is not None
        return has_any if truthy else not has_any

    if isinstance(expected, list):
        if op_norm == "in":
            targets = [parse_birthdate_wire(v) for v in expected]
            kinds = {t.granularity for t in targets}
            if len(kinds) != 1:
                raise ValueError("birthdate 'in' values must share the same granularity")
            granularity = next(iter(kinds))
            if not _customer_has_granularity(parts, granularity):
                return False
            actual = _extract_customer_scalar(parts, granularity)
            expected_set = {_extract_target_scalar(t) for t in targets}
            return actual in expected_set
        if op_norm == "between":
            if len(expected) != 2:
                raise ValueError("Operator 'between' requires [lo, hi]")
            lo = parse_birthdate_wire(expected[0])
            hi = parse_birthdate_wire(expected[1])
            if lo.granularity != hi.granularity:
                raise ValueError("birthdate 'between' bounds must share the same granularity")
            granularity = lo.granularity
            if not _customer_has_granularity(parts, granularity):
                return False
            actual = _extract_customer_scalar(parts, granularity)
            lo_v = _extract_target_scalar(lo)
            hi_v = _extract_target_scalar(hi)
            if actual is None or lo_v is None or hi_v is None:
                return False
            if granularity == BirthdateGranularity.DAY_MONTH:
                return _between_mmdd(int(actual), int(lo_v), int(hi_v))
            return lo_v <= actual <= hi_v

    target = parse_birthdate_wire(expected)
    if not _customer_has_granularity(parts, target.granularity):
        return False
    actual = _extract_customer_scalar(parts, target.granularity)
    expected_scalar = _extract_target_scalar(target)
    if actual is None or expected_scalar is None:
        return False
    return _compare_scalars(op_norm, actual, expected_scalar)


def birthdate_target_to_unomi(property_prefix: str, target: BirthdateTarget) -> tuple[str, dict[str, Any]]:
    """Map birthdate target to Unomi propertyName + propertyValue* params."""
    if target.granularity == BirthdateGranularity.FULL:
        assert target.full_date is not None
        dt = datetime(target.full_date.year, target.full_date.month, target.full_date.day)
        return (
            f"{property_prefix}.birthDate",
            {"propertyValueInteger": int(dt.timestamp() * 1000)},
        )
    if target.granularity == BirthdateGranularity.DAY_MONTH:
        return f"{property_prefix}.birthDate", {"propertyValue": target.wire}
    if target.granularity == BirthdateGranularity.MONTH:
        return f"{property_prefix}.birthMonth", {"propertyValueInteger": int(target.month)}
    if target.granularity == BirthdateGranularity.YEAR:
        return f"{property_prefix}.birthYear", {"propertyValueInteger": int(target.year)}
    raise ValueError(f"Unsupported birthdate granularity for Unomi: {target.granularity}")


def birthdate_sql_criterion(*, customer_model, op: str, value: Any) -> ColumnElement[bool]:
    """SQLAlchemy criterion for internal-job / batch selectors."""
    from app.models.customer import Customer

    if customer_model is not Customer:
        customer_model = Customer

    op_norm = (op or "").lower()
    birthdate_col = customer_model.birthdate
    month_col = customer_model.birth_month
    day_col = customer_model.birth_day
    year_col = customer_model.birth_year
    mmdd_expr = (month_col * 100) + day_col

    if op_norm == "exists":
        truthy = True if value is None else bool(value)
        has_any = or_(year_col.isnot(None), month_col.isnot(None), day_col.isnot(None))
        return has_any if truthy else ~has_any

    def _eq_full(target: BirthdateTarget) -> ColumnElement[bool]:
        assert target.full_date is not None
        d = target.full_date
        return or_(
            birthdate_col == d,
            and_(year_col == d.year, month_col == d.month, day_col == d.day),
        )

    def _eq_day_month(target: BirthdateTarget) -> ColumnElement[bool]:
        return mmdd_expr == target.mmdd

    def _eq_month(target: BirthdateTarget) -> ColumnElement[bool]:
        return month_col == target.month

    def _eq_year(target: BirthdateTarget) -> ColumnElement[bool]:
        return year_col == target.year

    def _compare_one(target: BirthdateTarget, operator: str) -> ColumnElement[bool]:
        if operator in {"eq", "="}:
            if target.granularity == BirthdateGranularity.FULL:
                return _eq_full(target)
            if target.granularity == BirthdateGranularity.DAY_MONTH:
                return _eq_day_month(target)
            if target.granularity == BirthdateGranularity.MONTH:
                return _eq_month(target)
            return _eq_year(target)
        if operator in {"neq", "!=", "ne"}:
            inner = _compare_one(target, "eq")
            return ~inner
        scalar_col = {
            BirthdateGranularity.FULL: birthdate_col,
            BirthdateGranularity.DAY_MONTH: mmdd_expr,
            BirthdateGranularity.MONTH: month_col,
            BirthdateGranularity.YEAR: year_col,
        }[target.granularity]
        scalar = _extract_target_scalar(target)
        if operator == "gt":
            return scalar_col > scalar
        if operator == "gte":
            return scalar_col >= scalar
        if operator == "lt":
            return scalar_col < scalar
        if operator == "lte":
            return scalar_col <= scalar
        raise ValueError(f"Unsupported birthdate operator: {operator}")

    if op_norm == "in":
        if not isinstance(value, list) or not value:
            raise ValueError("birthdate 'in' requires a non-empty list")
        targets = [parse_birthdate_wire(v) for v in value]
        kinds = {t.granularity for t in targets}
        if len(kinds) != 1:
            raise ValueError("birthdate 'in' values must share the same granularity")
        return or_(*[_compare_one(t, "eq") for t in targets])

    if op_norm == "between":
        if not isinstance(value, list) or len(value) != 2:
            raise ValueError("birthdate 'between' requires [lo, hi]")
        lo = parse_birthdate_wire(value[0])
        hi = parse_birthdate_wire(value[1])
        if lo.granularity != hi.granularity:
            raise ValueError("birthdate 'between' bounds must share the same granularity")
        if lo.granularity == BirthdateGranularity.DAY_MONTH:
            lo_v, hi_v = lo.mmdd, hi.mmdd
            if lo_v is None or hi_v is None:
                return false()
            if lo_v <= hi_v:
                return mmdd_expr.between(lo_v, hi_v)
            return or_(mmdd_expr.between(lo_v, 1231), mmdd_expr.between(101, hi_v))
        col = {
            BirthdateGranularity.FULL: birthdate_col,
            BirthdateGranularity.MONTH: month_col,
            BirthdateGranularity.YEAR: year_col,
        }[lo.granularity]
        lo_v = _extract_target_scalar(lo)
        hi_v = _extract_target_scalar(hi)
        return col.between(lo_v, hi_v)

    target = parse_birthdate_wire(value)
    return _compare_one(target, op_norm)
