"""Unomi segment definitions and manual membership via profileId OR conditions."""

from __future__ import annotations

import re
from typing import Any
from uuid import UUID

from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.segment import Segment
from app.services.segment_condition_unomi import resolve_unomi_condition_for_segment
from app.services.unomi_client import UnomiClient, UnomiClientError
from app.services.unomi_settings_service import UnomiConnectionConfig, resolve_unomi_connection


def _slug_segment_id(brand: str, name: str) -> str:
    base = re.sub(r"[^a-zA-Z0-9]+", "-", (name or "").strip().lower()).strip("-")
    brand_part = re.sub(r"[^a-zA-Z0-9]+", "-", (brand or "").strip().lower()).strip("-")
    return f"loyalty-{brand_part}-{base}"[:200] or f"loyalty-{brand_part}-segment"


def profile_ids_or_condition(profile_ids: list[str]) -> dict[str, Any]:
    """Unomi condition: profile itemId equals any of the given profile IDs."""
    cleaned = sorted({str(p).strip() for p in profile_ids if str(p).strip()})
    if not cleaned:
        return {
            "type": "profilePropertyCondition",
            "parameterValues": {
                "propertyName": "itemId",
                "comparisonOperator": "equals",
                "propertyValue": "__no_profiles__",
            },
        }
    if len(cleaned) == 1:
        return {
            "type": "profilePropertyCondition",
            "parameterValues": {
                "propertyName": "itemId",
                "comparisonOperator": "equals",
                "propertyValue": cleaned[0],
            },
        }
    return {
        "type": "booleanCondition",
        "parameterValues": {
            "operator": "or",
            "subConditions": [
                {
                    "type": "profilePropertyCondition",
                    "parameterValues": {
                        "propertyName": "itemId",
                        "comparisonOperator": "equals",
                        "propertyValue": pid,
                    },
                }
                for pid in cleaned
            ],
        },
    }


def build_unomi_segment_definition(
    *,
    segment_id: str,
    name: str,
    scope: str,
    description: str | None,
    condition: dict[str, Any],
    read_only: bool = False,
) -> dict[str, Any]:
    return {
        "metadata": {
            "id": segment_id,
            "name": name,
            "scope": scope,
            "description": description or "",
            "readOnly": read_only,
        },
        "condition": condition,
    }


def get_unomi_client(db: Session, *, brand: str) -> UnomiClient | None:
    cfg = resolve_unomi_connection(brand=brand)
    if not cfg:
        return None
    return UnomiClient(cfg)


def resolve_segment_scope(seg: Segment, cfg: UnomiConnectionConfig) -> str:
    return (seg.unomi_scope or cfg.scope or seg.brand).strip()


def manual_profile_ids_list(seg: Segment) -> list[str]:
    raw = seg.manual_profile_ids
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    return []


def set_manual_profile_ids(seg: Segment, profile_ids: list[str]) -> None:
    seg.manual_profile_ids = sorted({str(p).strip() for p in profile_ids if str(p).strip()})


def sync_manual_list_segment_to_unomi(db: Session, *, seg: Segment) -> dict[str, Any]:
    """Push manual_profile_ids to Unomi as an OR of itemId equals conditions."""
    if seg.provider != "UNOMI" or not seg.unomi_segment_id:
        raise ValueError("Segment is not backed by Unomi")

    client = get_unomi_client(db, brand=seg.brand)
    if not client:
        raise ValueError("Unomi is not configured for this brand")

    cfg = resolve_unomi_connection(brand=seg.brand)
    assert cfg is not None

    profile_ids = manual_profile_ids_list(seg)
    condition = profile_ids_or_condition(profile_ids)
    seg.unomi_condition = condition

    definition = build_unomi_segment_definition(
        segment_id=seg.unomi_segment_id,
        name=seg.name,
        scope=resolve_segment_scope(seg, cfg),
        description=seg.description,
        condition=condition,
        read_only=False,
    )
    client.save_segment(definition)
    db.flush()
    return {"unomiSegmentId": seg.unomi_segment_id, "profileCount": len(profile_ids), "synced": True}


def create_unomi_segment_mirror(
    db: Session,
    *,
    brand: str,
    name: str,
    description: str | None,
    is_dynamic: bool,
    conditions: dict | None,
    manual_profile_ids: list[str] | None,
    active: bool,
    unomi_segment_id: str | None = None,
    unomi_condition_override: dict | None = None,
) -> Segment:
    """Create segment in Unomi then persist registry row in loyalty DB."""
    client = get_unomi_client(db, brand=brand)
    cfg = resolve_unomi_connection(brand=brand)
    if not client or not cfg:
        raise ValueError("Unomi is not configured for this brand")

    ext_id = (unomi_segment_id or _slug_segment_id(brand, name)).strip()
    scope = cfg.scope

    if is_dynamic:
        if not conditions and not unomi_condition_override:
            raise ValueError("Dynamic Unomi segments require conditions or unomi_condition")
        if unomi_condition_override:
            unomi_condition = unomi_condition_override
        else:
            unomi_condition = resolve_unomi_condition_for_segment(
                is_dynamic=True,
                conditions=conditions,
                manual_profile_ids=None,
            )
    else:
        unomi_condition = resolve_unomi_condition_for_segment(
            is_dynamic=False,
            conditions=None,
            manual_profile_ids=manual_profile_ids or [],
        )

    definition = build_unomi_segment_definition(
        segment_id=ext_id,
        name=name,
        scope=scope,
        description=description,
        condition=unomi_condition,
    )
    try:
        client.save_segment(definition)
    except UnomiClientError as e:
        if e.status_code not in (409, 400):
            raise
        client.save_segment(definition)

    seg = Segment(
        brand=brand,
        name=name,
        description=description,
        is_dynamic=is_dynamic,
        conditions=None if not is_dynamic else conditions,
        active=active,
        provider="UNOMI",
        unomi_segment_id=ext_id,
        unomi_scope=scope,
        manual_profile_ids=sorted({str(p).strip() for p in (manual_profile_ids or []) if str(p).strip()}) or None,
        unomi_condition=unomi_condition,
    )
    db.add(seg)
    db.flush()
    return seg


def add_customers_to_unomi_manual_segment(
    db: Session,
    *,
    seg: Segment,
    customer_ids: list[UUID],
) -> dict[str, int]:
    if seg.provider != "UNOMI":
        raise ValueError("Not a Unomi segment")
    if seg.is_dynamic:
        raise ValueError("Cannot manually add members to a dynamic Unomi segment from the engine UI")

    existing = set(manual_profile_ids_list(seg))
    created = 0
    skipped = 0
    missing = 0

    for cid in customer_ids:
        cust = db.query(Customer).filter(Customer.id == cid, Customer.brand == seg.brand).first()
        if not cust or not cust.profile_id:
            missing += 1
            continue
        pid = cust.profile_id.strip()
        if pid in existing:
            skipped += 1
            continue
        existing.add(pid)
        created += 1

    set_manual_profile_ids(seg, list(existing))
    sync_manual_list_segment_to_unomi(db, seg=seg)
    return {"created": created, "skipped_existing": skipped, "missing": missing}


def remove_customers_from_unomi_manual_segment(
    db: Session,
    *,
    seg: Segment,
    customer_ids: list[UUID],
) -> dict[str, int]:
    if seg.provider != "UNOMI":
        raise ValueError("Not a Unomi segment")
    if seg.is_dynamic:
        raise ValueError("Cannot manually remove members from a dynamic Unomi segment from the engine UI")

    existing = set(manual_profile_ids_list(seg))
    deleted = 0
    missing = 0

    for cid in customer_ids:
        cust = db.query(Customer).filter(Customer.id == cid, Customer.brand == seg.brand).first()
        if not cust or not cust.profile_id:
            missing += 1
            continue
        pid = cust.profile_id.strip()
        if pid not in existing:
            missing += 1
            continue
        existing.remove(pid)
        deleted += 1

    set_manual_profile_ids(seg, list(existing))
    sync_manual_list_segment_to_unomi(db, seg=seg)
    return {"deleted": deleted, "missing": missing}


def delete_unomi_segment(db: Session, *, seg: Segment) -> None:
    if seg.provider != "UNOMI" or not seg.unomi_segment_id:
        return
    client = get_unomi_client(db, brand=seg.brand)
    if not client:
        return
    try:
        client.delete_segment(seg.unomi_segment_id)
    except UnomiClientError as e:
        if e.status_code != 404:
            raise
