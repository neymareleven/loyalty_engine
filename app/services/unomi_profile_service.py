"""Bidirectional loyalty customer ↔ Apache Unomi profile sync."""

from __future__ import annotations

import logging
import os
from contextvars import ContextVar
from datetime import date, datetime
from typing import Any

from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.customer_metrics import CustomerMetrics
from app.services.customer_serialization import _format_birthdate, _tier_name_for_customer
from app.services.unomi_client import UnomiClient, UnomiClientError
from app.services.unomi_settings_service import resolve_unomi_profile_connection
from app.services.wallet_service import get_status_points_balance

logger = logging.getLogger(__name__)

_profile_sync_source: ContextVar[str | None] = ContextVar("profile_sync_source", default=None)


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


def build_unomi_profile_payload(
    db: Session,
    *,
    customer: Customer,
    scope: str,
    include_points_balance: bool = True,
) -> dict[str, Any]:
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
        "brand": customer.brand,
        "loyaltyStatus": customer.loyalty_status,
        "statusPoints": int(customer.status_points or 0),
        "loyaltyCustomerStatus": customer.status,
        "loyaltyTierName": tier_name,
        "loyaltyEngineCustomerId": str(customer.id),
        "loyaltyEngineSyncedAt": datetime.utcnow().isoformat(),
    }

    if customer.gender:
        properties["gender"] = customer.gender
    birth = _birthdate_to_unomi_value(customer)
    if birth is not None:
        properties["birthDate"] = birth
    if customer.birth_month is not None:
        properties["birthMonth"] = int(customer.birth_month)
    if customer.birth_year is not None:
        properties["birthYear"] = int(customer.birth_year)

    if points_balance is not None:
        properties["loyaltyPointsBalance"] = points_balance

    metrics_props: dict[str, Any] = {}
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

    return {
        "itemId": customer.profile_id,
        "itemType": "profile",
        "properties": properties,
        "systemProperties": {"scope": scope},
        "segments": [],
        "scores": {},
        "consents": {},
    }


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
) -> dict[str, Any] | None:
    """Push loyalty customer state to Unomi (best-effort unless UNOMI_PROFILE_SYNC_STRICT=true)."""
    if should_skip_unomi_profile_push():
        return {"skipped": True, "reason": "sync_source_unomi"}

    cfg = resolve_unomi_profile_connection(brand=customer.brand)
    if not cfg:
        return None

    client = UnomiClient(cfg)
    body = build_unomi_profile_payload(db, customer=customer, scope=cfg.scope)
    try:
        client.save_profile(body)
        logger.info(
            "unomi profile sync ok brand=%s profile_id=%s reason=%s",
            customer.brand,
            customer.profile_id,
            reason,
        )
        return {"synced": True, "profileId": customer.profile_id, "reason": reason}
    except UnomiClientError as e:
        logger.warning(
            "unomi profile sync failed brand=%s profile_id=%s reason=%s error=%s",
            customer.brand,
            customer.profile_id,
            reason,
            e,
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
