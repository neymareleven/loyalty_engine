"""List segment members for INTERNAL and Unomi-backed segments."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.segment import Segment
from app.models.segment_member import SegmentMember
from app.services.segment_membership_service import resolve_unomi_segment_profile_ids
from app.services.unomi_segment_service import manual_profile_ids_list


def list_segment_members(
    db: Session,
    *,
    seg: Segment,
    limit: int = 500,
    offset: int = 0,
    source: str | None = None,
) -> dict:
    if getattr(seg, "provider", "INTERNAL") == "UNOMI":
        return _list_unomi_members(db, seg=seg, limit=limit, offset=offset, source=source)
    return _list_internal_members(db, seg=seg, limit=limit, offset=offset, source=source)


def _list_internal_members(
    db: Session,
    *,
    seg: Segment,
    limit: int,
    offset: int,
    source: str | None,
) -> dict:
    q = (
        db.query(SegmentMember, Customer)
        .join(Customer, Customer.id == SegmentMember.customer_id)
        .filter(SegmentMember.segment_id == seg.id)
        .filter(Customer.brand == seg.brand)
    )
    if source:
        q = q.filter(SegmentMember.source == source)

    total = q.count()
    rows = q.order_by(SegmentMember.created_at.desc()).offset(offset).limit(limit).all()

    items = [
        {
            "segment_id": seg.id,
            "customer_id": m.customer_id,
            "profile_id": c.profile_id,
            "source": m.source,
            "computed_at": m.computed_at,
            "created_at": m.created_at,
            "membership_origin": "segment_members",
        }
        for m, c in rows
    ]
    return {
        "segment_id": seg.id,
        "provider": seg.provider or "INTERNAL",
        "is_dynamic": seg.is_dynamic,
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": items,
    }


def _list_unomi_members(
    db: Session,
    *,
    seg: Segment,
    limit: int,
    offset: int,
    source: str | None,
) -> dict:
    if seg.is_dynamic:
        profile_ids = resolve_unomi_segment_profile_ids(db, segment=seg)
        origin = "unomi_impacted"
    else:
        profile_ids = manual_profile_ids_list(seg)
        origin = "manual_profile_ids"

    if source and source.upper() not in ("UNOMI", "STATIC", "DYNAMIC"):
        profile_ids = []

    total = len(profile_ids)
    page_ids = profile_ids[offset : offset + limit]

    customers_by_profile: dict[str, Customer] = {}
    if page_ids:
        rows = (
            db.query(Customer)
            .filter(Customer.brand == seg.brand)
            .filter(Customer.profile_id.in_(page_ids))
            .all()
        )
        customers_by_profile = {(c.profile_id or "").strip(): c for c in rows if c.profile_id}

    items = []
    for pid in page_ids:
        c = customers_by_profile.get(pid)
        items.append(
            {
                "segment_id": seg.id,
                "customer_id": c.id if c else None,
                "profile_id": pid,
                "source": "UNOMI",
                "computed_at": None,
                "created_at": None,
                "membership_origin": origin,
                "customer_found_in_engine": c is not None,
            }
        )

    return {
        "segment_id": seg.id,
        "provider": "UNOMI",
        "is_dynamic": seg.is_dynamic,
        "unomi_segment_id": seg.unomi_segment_id,
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": items,
        "note": (
            "Dynamic: members from Unomi impacted/match API. "
            "Static: manual_profile_ids synced as OR(itemId) condition in Unomi."
        ),
    }
