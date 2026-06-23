"""Bidirectional loyalty customer ↔ Apache Unomi profile sync."""

from __future__ import annotations

import logging
import os
from contextvars import ContextVar
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.customer_metrics import CustomerMetrics
from app.services.customer_serialization import _format_birthdate, _tier_name_for_customer
from app.services.unomi_client import UnomiClient, UnomiClientError
from app.services.unomi_settings_service import (
    resolve_unomi_profile_connection,
    resolve_unomi_profile_sync_peer_key,
    unomi_profile_sync_event_type,
    unomi_profile_sync_transport,
)
from app.services.wallet_service import get_status_points_balance

logger = logging.getLogger(__name__)

_profile_sync_source: ContextVar[str | None] = ContextVar("profile_sync_source", default=None)

# Loyalty-owned keys (always refreshed from Customer on sync).
_UNOMI_LOYALTY_MANAGED_KEYS = frozenset(
    {
        "loyaltyStatus",
        "statusPoints",
        "loyaltyPointsBalance",
        "loyaltyCustomerStatus",
        "loyaltyTierName",
        "loyaltyEngineCustomerId",
        "loyaltyEngineSyncedAt",
        "lastActivityAt",
        "loyaltyStatusAssignedAt",
        "loyaltyStatusExpiresAt",
        "pointsExpiresAt",
        "statusPointsResetAt",
        "loyaltyCreatedAt",
        "loyaltyUpdatedAt",
        "birthMonth",
        "birthYear",
    }
)

# CDP / visit / marketing keys — keep existing Unomi values unless provided in upsert.
_UNOMI_CDP_PRESERVE_IF_ABSENT = frozenset(
    {
        "lastVisit",
        "firstVisit",
        "nbOfVisits",
        "pageViewCount",
        "lastEmailSent",
        "lastEmailOpened",
        "lastOpened",
        "openCount",
        "emailId",
        "emailName",
        "recipientEmail",
        "recipientFirstName",
        "recipientLastName",
        "subject",
        "contentHash",
        "fromName",
        "fromAddress",
        "timestamp",
        "dateSent",
        "dateRead",
        "trackingHash",
        "idHash",
    }
)

_UNOMI_UPSERT_SKIP_KEYS = frozenset(
    {
        "profileId",
        "profile_id",
        "birthdate",
    }
)


def set_profile_sync_source(source: str | None):
    return _profile_sync_source.set(source)


def reset_profile_sync_source(token) -> None:
    _profile_sync_source.reset(token)


def should_skip_unomi_profile_push() -> bool:
    return (_profile_sync_source.get() or "").strip().lower() == "unomi"


def _strict_sync_errors() -> bool:
    return str(os.getenv("UNOMI_PROFILE_SYNC_STRICT", "")).strip().lower() in {"1", "true", "yes"}


def _birthdate_to_unomi_value(customer: Customer) -> str | int | None:
    formatted = _format_birthdate(customer)
    if not formatted:
        return None
    if len(formatted) == 10:
        try:
            d = date.fromisoformat(formatted)
            dt = datetime(d.year, d.month, d.day)
            return int(dt.timestamp() * 1000)
        except Exception:
            return formatted
    return formatted


def _iso_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    try:
        return value.isoformat()
    except Exception:
        return str(value)


def _unomi_visit_iso8601_z(value: datetime | None) -> str | None:
    """Unomi CDP visit timestamps: 2026-06-18T11:10:47Z (UTC, no fractional seconds)."""
    if value is None:
        return None
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_unomi_visit_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, (int, float)):
        # Epoch ms (Unomi internal) or seconds
        ts = float(value)
        if ts > 1_000_000_000_000:
            ts /= 1000.0
        return datetime.utcfromtimestamp(ts)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
            return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except ValueError:
            return None
    return None


def _apply_visit_properties(
    merged: dict[str, Any],
    *,
    existing_props: dict[str, Any],
    customer: Customer,
    explicit_visits: dict[str, Any] | None = None,
) -> None:
    """
    firstVisit / lastVisit (ISO 8601 Z):
    - preserve CDP values when already set on Unomi
    - otherwise derive from loyalty timestamps (created_at, last_activity_at, updated_at)
    - explicit values in upsert properties.* override
    """
    explicit = explicit_visits or {}

    if explicit.get("firstVisit") not in (None, ""):
        parsed = _parse_unomi_visit_datetime(explicit["firstVisit"])
        if parsed:
            merged["firstVisit"] = _unomi_visit_iso8601_z(parsed)
    elif existing_props.get("firstVisit") not in (None, ""):
        merged["firstVisit"] = existing_props["firstVisit"]
    elif customer.created_at:
        merged["firstVisit"] = _unomi_visit_iso8601_z(customer.created_at)

    if explicit.get("lastVisit") not in (None, ""):
        parsed = _parse_unomi_visit_datetime(explicit["lastVisit"])
        if parsed:
            merged["lastVisit"] = _unomi_visit_iso8601_z(parsed)
    else:
        candidates: list[datetime] = []
        for raw in (customer.last_activity_at, customer.updated_at, customer.created_at):
            if isinstance(raw, datetime):
                candidates.append(raw.replace(tzinfo=None) if raw.tzinfo else raw)
        existing_last = _parse_unomi_visit_datetime(existing_props.get("lastVisit"))
        if existing_last:
            candidates.append(existing_last)
        if candidates:
            merged["lastVisit"] = _unomi_visit_iso8601_z(max(candidates))


def _derive_scope_email(*, brand: str, email: Any, scope_email: Any = None) -> str | None:
    if scope_email is not None and str(scope_email).strip():
        return str(scope_email).strip()
    if not brand or email is None:
        return None
    em = str(email).strip()
    if not em:
        return None
    return f"{brand.strip()}-{em}"


def _normalize_contact_properties(extra: dict[str, Any] | None) -> dict[str, Any]:
    if not extra:
        return {}
    out: dict[str, Any] = {}
    for key, val in extra.items():
        if not isinstance(key, str) or not key.strip():
            continue
        if key in _UNOMI_UPSERT_SKIP_KEYS:
            continue
        if val is None or val == "":
            continue
        out[key.strip()] = val
    phone = out.get("phone")
    phone_number = out.get("phoneNumber")
    if phone and not phone_number:
        out["phoneNumber"] = phone
    elif phone_number and not phone:
        out["phone"] = phone_number
    return out


def build_customer_identity_unomi_properties(customer: Customer) -> dict[str, Any]:
    """Loyalty profile identity: profileId fields stored on Customer (email, gender, birthdate, brand)."""
    props: dict[str, Any] = {"brand": customer.brand}
    if customer.email:
        props["email"] = str(customer.email).strip()
    if customer.gender:
        props["gender"] = customer.gender
    birth = _birthdate_to_unomi_value(customer)
    if birth is not None:
        props["birthDate"] = birth
    if customer.birth_month is not None:
        props["birthMonth"] = int(customer.birth_month)
    if customer.birth_year is not None:
        props["birthYear"] = int(customer.birth_year)
    scope_email = _derive_scope_email(brand=customer.brand, email=customer.email)
    if scope_email:
        props["scopeEmail"] = scope_email
    return props


def build_loyalty_program_unomi_properties(
    db: Session,
    *,
    customer: Customer,
    include_points_balance: bool = True,
) -> dict[str, Any]:
    """Fidélité enrichie (statut, points, métriques) — sans réinventer l'identité contact."""
    metrics = (
        db.query(CustomerMetrics)
        .filter(CustomerMetrics.brand == customer.brand)
        .filter(CustomerMetrics.customer_id == customer.id)
        .first()
    )
    tier_name = _tier_name_for_customer(db, brand=customer.brand, customer=customer)
    points_balance = (
        int(get_status_points_balance(db, customer.id) or 0) if include_points_balance else None
    )

    properties: dict[str, Any] = {
        "loyaltyStatus": customer.loyalty_status,
        "statusPoints": int(customer.status_points or 0),
        "loyaltyCustomerStatus": customer.status,
        "loyaltyTierName": tier_name,
        "loyaltyEngineCustomerId": str(customer.id),
        "loyaltyEngineSyncedAt": datetime.utcnow().isoformat(),
    }

    if points_balance is not None:
        properties["loyaltyPointsBalance"] = points_balance

    if metrics:
        metrics_props = {
            "transactions_count_30d": int(metrics.transactions_count_30d or 0),
            "transactions_count_90d": int(metrics.transactions_count_90d or 0),
            "last_transaction_at": _iso_or_none(metrics.last_transaction_at),
            "computed_at": _iso_or_none(metrics.computed_at),
        }
        properties["metrics"] = metrics_props

    loyalty_dates = {
        "lastActivityAt": _iso_or_none(customer.last_activity_at),
        "loyaltyStatusAssignedAt": _iso_or_none(customer.loyalty_status_assigned_at),
        "loyaltyStatusExpiresAt": _iso_or_none(customer.loyalty_status_expires_at),
        "pointsExpiresAt": _iso_or_none(customer.points_expires_at),
        "statusPointsResetAt": _iso_or_none(customer.status_points_reset_at),
        "loyaltyCreatedAt": _iso_or_none(customer.created_at),
        "loyaltyUpdatedAt": _iso_or_none(customer.updated_at),
    }
    for key, val in loyalty_dates.items():
        if val is not None:
            properties[key] = val

    return properties


def _compact_unomi_properties(properties: dict[str, Any]) -> dict[str, Any]:
    """Drop null/empty values before Unomi save/event (no invented placeholders)."""
    out: dict[str, Any] = {}
    for key, val in properties.items():
        if val is None or val == "":
            continue
        if key == "metrics" and isinstance(val, dict) and not val:
            continue
        out[key] = val
    return out


# Backward-compatible alias
build_loyalty_unomi_properties = build_loyalty_program_unomi_properties


def merge_unomi_profile_properties(
    *,
    existing_profile: dict[str, Any] | None,
    customer: Customer,
    loyalty_program_properties: dict[str, Any],
    extra_properties: dict[str, Any] | None,
    profile_id: str,
) -> dict[str, Any]:
    """Unomi profile = existing CDP data + loyalty identity + optional extras + fidélité."""
    existing_props = (
        existing_profile.get("properties")
        if isinstance(existing_profile, dict) and isinstance(existing_profile.get("properties"), dict)
        else {}
    )
    merged: dict[str, Any] = dict(existing_props)

    extras = _normalize_contact_properties(extra_properties)
    for key, val in extras.items():
        if key in _UNOMI_LOYALTY_MANAGED_KEYS:
            continue
        if key in {"email", "gender", "birthDate", "birthdate", "brand", "scopeEmail"}:
            continue
        merged[key] = val

    merged.update(build_customer_identity_unomi_properties(customer))

    for key, val in loyalty_program_properties.items():
        if val is None:
            continue
        if key == "metrics" and isinstance(val, dict):
            if not val:
                continue
            prev = merged.get("metrics") if isinstance(merged.get("metrics"), dict) else {}
            merged["metrics"] = {**prev, **val}
            continue
        merged[key] = val

    merged["unomiProfileId"] = profile_id
    merged["unomi_profile_id"] = profile_id
    _apply_visit_properties(
        merged,
        existing_props=existing_props,
        customer=customer,
        explicit_visits=extra_properties,
    )
    return _compact_unomi_properties(merged)


def merge_unomi_system_properties(
    *,
    existing_profile: dict[str, Any] | None,
    scope: str,
    scope_email: str | None,
) -> dict[str, Any]:
    existing_sys = (
        existing_profile.get("systemProperties")
        if isinstance(existing_profile, dict) and isinstance(existing_profile.get("systemProperties"), dict)
        else {}
    )
    system: dict[str, Any] = dict(existing_sys)
    system["scope"] = scope
    if scope_email:
        system["mergeIdentifier"] = scope_email
    return system


def build_unomi_profile_payload(
    db: Session,
    *,
    customer: Customer,
    scope: str,
    include_points_balance: bool = True,
    extra_properties: dict[str, Any] | None = None,
    existing_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    loyalty_program = build_loyalty_program_unomi_properties(
        db,
        customer=customer,
        include_points_balance=include_points_balance,
    )
    properties = merge_unomi_profile_properties(
        existing_profile=existing_profile,
        customer=customer,
        loyalty_program_properties=loyalty_program,
        extra_properties=extra_properties,
        profile_id=customer.profile_id,
    )
    scope_email = properties.get("scopeEmail")
    if isinstance(scope_email, str):
        scope_email = scope_email.strip() or None
    else:
        scope_email = None

    segments = []
    scores: dict[str, Any] = {}
    consents: dict[str, Any] = {}
    if isinstance(existing_profile, dict):
        if isinstance(existing_profile.get("segments"), list):
            segments = list(existing_profile["segments"])
        if isinstance(existing_profile.get("scores"), dict):
            scores = dict(existing_profile["scores"])
        if isinstance(existing_profile.get("consents"), dict):
            consents = dict(existing_profile["consents"])

    return {
        "itemId": customer.profile_id,
        "itemType": "profile",
        "properties": properties,
        "systemProperties": merge_unomi_system_properties(
            existing_profile=existing_profile,
            scope=scope,
            scope_email=scope_email,
        ),
        "segments": segments,
        "scores": scores,
        "consents": consents,
    }


def build_unomi_eventcollector_payload(
    *,
    profile_id: str,
    scope: str,
    properties: dict[str, Any],
    event_type: str | None = None,
) -> dict[str, Any]:
    """Build POST /cxs/eventcollector body (Unomi 2.x recommended for profile upsert)."""
    evt_type = (event_type or unomi_profile_sync_event_type() or "contactInfoSubmitted").strip()
    session_id = f"loyalty-{profile_id}"

    event: dict[str, Any] = {
        "eventType": evt_type,
        "scope": scope,
        "profileId": profile_id,
        "source": {
            "itemId": "loyalty-engine",
            "itemType": "system",
            "scope": scope,
        },
    }

    if evt_type.lower() == "updateproperties":
        add_map: dict[str, Any] = {}
        for key, val in properties.items():
            if val is None:
                continue
            prop_key = key if key.startswith("properties.") else f"properties.{key}"
            add_map[prop_key] = val
        event["properties"] = {
            "targetId": profile_id,
            "targetType": "profile",
            "add": add_map,
        }
        event["target"] = None
    else:
        event["target"] = {
            "itemId": "loyalty-profile-sync",
            "itemType": "form",
            "scope": scope,
        }
        event["properties"] = _compact_unomi_properties(dict(properties))

    return {
        "sessionId": session_id,
        "profileId": profile_id,
        "events": [event],
    }


def _push_profile_via_eventcollector(
    client: UnomiClient,
    *,
    profile_id: str,
    scope: str,
    properties: dict[str, Any],
    brand: str,
) -> None:
    """Send a single event via /cxs/eventcollector (default: contactInfoSubmitted)."""
    event_type = (unomi_profile_sync_event_type() or "contactInfoSubmitted").strip()
    norm = event_type.lower()
    peer_key = resolve_unomi_profile_sync_peer_key(brand=brand)

    if norm == "updateproperties":
        if not peer_key:
            logger.warning(
                "updateProperties requested but UNOMI_PROFILE_SYNC_PEER_KEY is missing/placeholder; "
                "falling back to contactInfoSubmitted (brand=%s profile_id=%s)",
                brand,
                profile_id,
            )
            norm = "contactinfosubmitted"
        else:
            secured = build_unomi_eventcollector_payload(
                profile_id=profile_id,
                scope=scope,
                properties=properties,
                event_type="updateProperties",
            )
            client.collect_events(secured, peer_key=peer_key)
            return

    resolved_type = "contactInfoSubmitted" if norm in {"", "contactinfosubmitted"} else event_type
    payload = build_unomi_eventcollector_payload(
        profile_id=profile_id,
        scope=scope,
        properties=properties,
        event_type=resolved_type,
    )
    client.collect_events(payload)


def get_unomi_profile_client(*, brand: str) -> UnomiClient | None:
    cfg = resolve_unomi_profile_connection(brand=brand)
    if not cfg:
        return None
    return UnomiClient(cfg)


def sync_customer_profile_to_unomi(
    db: Session,
    *,
    customer: Customer,
    reason: str = "update",
    extra_properties: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Push loyalty customer state to Unomi (best-effort unless UNOMI_PROFILE_SYNC_STRICT=true)."""
    if should_skip_unomi_profile_push():
        return {"skipped": True, "reason": "sync_source_unomi"}

    cfg = resolve_unomi_profile_connection(brand=customer.brand)
    if not cfg:
        logger.warning(
            "unomi profile sync skipped (not configured) brand=%s profile_id=%s reason=%s",
            customer.brand,
            customer.profile_id,
            reason,
        )
        return {
            "synced": False,
            "skipped": True,
            "reason": "profile_sync_not_configured",
            "profileId": customer.profile_id,
        }

    client = UnomiClient(cfg)
    existing_profile = None
    try:
        existing_profile = client.get_profile(customer.profile_id)
    except UnomiClientError as e:
        logger.debug(
            "unomi get_profile before sync brand=%s profile_id=%s: %s",
            customer.brand,
            customer.profile_id,
            e,
        )

    body = build_unomi_profile_payload(
        db,
        customer=customer,
        scope=cfg.scope,
        extra_properties=extra_properties,
        existing_profile=existing_profile,
    )
    transport = unomi_profile_sync_transport()
    try:
        if transport == "eventcollector":
            # Unomi eventcollector alone may assign a random profile UUID.
            # Ensure stable itemId (= loyalty profile_id) then fire contactInfoSubmitted for CDP rules.
            client.save_profile(body)
            _push_profile_via_eventcollector(
                client,
                profile_id=customer.profile_id,
                scope=cfg.scope,
                properties=body.get("properties") or {},
                brand=customer.brand,
            )
        else:
            client.save_profile(body)
        logger.info(
            "unomi profile sync ok brand=%s profile_id=%s reason=%s transport=%s",
            customer.brand,
            customer.profile_id,
            reason,
            transport,
        )
        return {
            "synced": True,
            "profileId": customer.profile_id,
            "reason": reason,
            "transport": transport,
        }
    except UnomiClientError as e:
        logger.warning(
            "unomi profile sync failed brand=%s profile_id=%s reason=%s transport=%s error=%s body=%s",
            customer.brand,
            customer.profile_id,
            reason,
            transport,
            e,
            (e.body or "")[:500],
        )
        if _strict_sync_errors():
            raise
        return {"synced": False, "profileId": customer.profile_id, "error": str(e)}


def delete_profile_from_unomi(*, brand: str, profile_id: str) -> dict[str, Any] | None:
    if should_skip_unomi_profile_push():
        return {"skipped": True, "reason": "sync_source_unomi"}

    cfg = resolve_unomi_profile_connection(brand=brand)
    if not cfg:
        return None

    client = UnomiClient(cfg)
    try:
        client.delete_profile(profile_id, with_data=True)
        logger.info("unomi profile deleted brand=%s profile_id=%s", brand, profile_id)
        return {"deleted": True, "profileId": profile_id}
    except UnomiClientError as e:
        logger.warning("unomi profile delete failed brand=%s profile_id=%s error=%s", brand, profile_id, e)
        if _strict_sync_errors():
            raise
        return {"deleted": False, "profileId": profile_id, "error": str(e)}
