"""Normalize POST /customers/upsert — loyalty identity fields (profileId, email, gender, birthdate)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from app.schemas.customer import CustomerUpsert

# Stored on Customer + pushed to Unomi as profile identity (not optional CDP extras).
_LOYALTY_IDENTITY_KEYS = frozenset(
    {
        "email",
        "gender",
        "birthdate",
        "birthDate",
        "brand",
        "your-brand",
        "your_brand",
        "profileId",
        "profile_id",
    }
)

_CF7_INTERNAL_PREFIXES = ("_wpcf7", "phone-cf7it")


def _is_cf7_internal_property(key: str) -> bool:
    k = (key or "").strip()
    if not k:
        return True
    return any(k.startswith(prefix) for prefix in _CF7_INTERNAL_PREFIXES)


def parse_customer_upsert_payload(payload: CustomerUpsert) -> dict[str, Any]:
    """
    Loyalty upsert base fields:
    - profileId (required, schema)
    - brand (required)
    - email, gender, birthdate (optional, top-level or properties.*)
    Remaining properties.* keys are optional CDP extras (firstName, phone, …).
    """
    props = dict(payload.properties or {})

    brand = (
        payload.brand
        or props.get("brand")
        or props.get("your-brand")
        or props.get("your_brand")
        or ""
    ).strip() or None

    email_raw = payload.email if payload.email is not None else props.get("email")
    email = str(email_raw).strip() if email_raw not in (None, "") else None

    gender_raw = payload.gender if payload.gender is not None else props.get("gender")
    gender = str(gender_raw).strip() if isinstance(gender_raw, str) and gender_raw.strip() else None

    birthdate = payload.birthdate
    if birthdate is None and props.get("birthDate") not in (None, ""):
        bd = props.get("birthDate")
        if isinstance(bd, (int, float)):
            birthdate = datetime.utcfromtimestamp(float(bd) / 1000.0).date()
        elif isinstance(bd, str) and bd.strip():
            birthdate = bd.strip()

    extra_properties = {
        k: v
        for k, v in props.items()
        if k not in _LOYALTY_IDENTITY_KEYS
        and v not in (None, "")
        and not _is_cf7_internal_property(str(k))
    }

    return {
        "brand": brand,
        "email": email,
        "gender": gender,
        "birthdate": birthdate,
        "extra_properties": extra_properties,
    }


def customer_identity_payload(parsed: dict[str, Any]) -> dict[str, Any]:
    """Fields persisted on the Customer row."""
    out: dict[str, Any] = {}
    if parsed.get("gender"):
        out["gender"] = parsed["gender"]
    if parsed.get("birthdate") is not None:
        out["birthdate"] = parsed["birthdate"]
    if parsed.get("email"):
        out["email"] = parsed["email"]
    return out
