from __future__ import annotations

import hashlib
import os
from typing import Any

from sqlalchemy.orm import Session

ANONYMIZED_STATUS = "ANONYMIZED"

_PII_KEYS = {
    "email",
    "phone",
    "firstname",
    "lastname",
    "name",
    "address",
    "profileid",
    "billing_email",
    "billingemail",
    "billing_phone",
    "billingphone",
    "billing_first_name",
    "billing_last_name",
    "billingfirstname",
    "billinglastname",
    "billing_address_1",
    "billing_address_2",
    "first_name",
    "last_name",
    "scopeemail",
    "shipping_address_1",
    "shipping_first_name",
    "shipping_last_name",
}


def normalize_email(value: str | None) -> str | None:
    if value is None or not str(value).strip():
        return None
    return str(value).strip().lower()


def email_identity_hash(email: str | None) -> str | None:
    normalized = normalize_email(email)
    if not normalized:
        return None
    secret = (
        os.getenv("IDENTITY_HASH_SECRET")
        or os.getenv("UNOMI_WEBHOOK_SECRET")
        or "loyalty-identity-hash"
    )
    return hashlib.sha256(f"{normalized}{secret}".encode("utf-8")).hexdigest()


def customer_is_anonymized(customer) -> bool:
    return str(getattr(customer, "status", "") or "").upper() == ANONYMIZED_STATUS


def exclude_anonymized(query):
    from app.models.customer import Customer
    from sqlalchemy import or_

    return query.filter(or_(Customer.status.is_(None), Customer.status != ANONYMIZED_STATUS))


def _key_norm(key: str) -> str:
    return str(key or "").strip().lower().replace("-", "")


def scrub_pii(value: Any, *, anon_profile_id: str | None = None) -> Any:
    if isinstance(value, list):
        return [scrub_pii(item, anon_profile_id=anon_profile_id) for item in value]
    if not isinstance(value, dict):
        return value
    out = {}
    for key, raw in value.items():
        kn = _key_norm(key)
        if kn == "profileid" and anon_profile_id:
            out[key] = anon_profile_id
            continue
        if kn in _PII_KEYS:
            continue
        out[key] = scrub_pii(raw, anon_profile_id=anon_profile_id)
    return out


def previously_erased_bypass(db: Session, *, brand: str, email: str | None) -> bool:
    digest = email_identity_hash(email)
    if not digest:
        return False
    from app.models.erased_identity import ErasedIdentity

    row = (
        db.query(ErasedIdentity)
        .filter(ErasedIdentity.brand == brand)
        .filter(ErasedIdentity.email_hash == digest)
        .filter(ErasedIdentity.bypass_used.is_(True))
        .first()
    )
    return row is not None


def previously_erased(db: Session, *, brand: str, email: str | None) -> bool:
    digest = email_identity_hash(email)
    if not digest:
        return False
    from app.models.erased_identity import ErasedIdentity

    row = (
        db.query(ErasedIdentity)
        .filter(ErasedIdentity.brand == brand)
        .filter(ErasedIdentity.email_hash == digest)
        .first()
    )
    return row is not None
