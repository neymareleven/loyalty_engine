"""List segment members for INTERNAL and Unomi-backed segments."""

from __future__ import annotations

from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.segment import Segment
from app.models.segment_member import SegmentMember
from app.services.segment_admin_service import segment_needs_recompute
from app.services.segment_membership_service import unomi_dynamic_uses_engine_membership
from app.services.segment_service import recompute_dynamic_segment
from app.services.unomi_segment_service import manual_profile_ids_list


def _customer_matches_segment_conditions(db: Session, *, customer: Customer, seg: Segment) -> bool:
    if not seg.is_dynamic or not seg.conditions:
        return True
    from app.services.rule_engine import _evaluate_ast_condition  # noqa: PLC0415

    tx = type("SegTx", (), {"payload": {}, "brand": seg.brand})()
    try:
        return bool(
            _evaluate_ast_condition(db=db, customer=customer, transaction=tx, node=seg.conditions)
        )
    except Exception:
        return False


def _enrich_members_payload(
    db: Session,
    *,
    seg: Segment,
    payload: dict,
    verify: bool,
) -> dict:
    payload["last_computed_at"] = seg.last_computed_at
    payload["membership_stale"] = segment_needs_recompute(seg)
    payload["verified"] = bool(verify)
    payload["page_mismatch_count"] = 0

    if not verify or not seg.is_dynamic or not seg.conditions:
        return payload

    mismatch = 0
    for item in payload.get("items") or []:
        cid = item.get("customer_id")
        if not cid:
            item["matches_conditions"] = None
            continue
        cust = db.query(Customer).filter(Customer.id == cid, Customer.brand == seg.brand).first()
        if not cust:
            item["matches_conditions"] = None
            continue
        ok = _customer_matches_segment_conditions(db, customer=cust, seg=seg)
        item["matches_conditions"] = ok
        if not ok:
            mismatch += 1
    payload["page_mismatch_count"] = mismatch
    if mismatch:
        note = payload.get("note") or ""
        payload["note"] = (
            f"{note} " if note else ""
        ) + f"{mismatch} member(s) on this page no longer match conditions (live verify)."
    return payload


def list_segment_members(
    db: Session,
    *,
    seg: Segment,
    limit: int = 500,
    offset: int = 0,
    source: str | None = None,
    refresh: bool = False,
    verify: bool = False,
) -> dict:
    refreshed = False
    if refresh and seg.is_dynamic and seg.active and seg.conditions is not None:
        recompute_dynamic_segment(db, segment=seg)
        refreshed = True

    if unomi_dynamic_uses_engine_membership(seg):
        effective_source = source
        if effective_source and effective_source.upper() == "UNOMI":
            effective_source = "DYNAMIC"
        payload = _list_internal_members(
            db, seg=seg, limit=limit, offset=offset, source=effective_source or "DYNAMIC"
        )
        payload["provider"] = "UNOMI"
        payload["unomi_segment_id"] = getattr(seg, "unomi_segment_id", None)
        payload["note"] = (
            "Dynamic UNOMI: membership from engine segment_members (AST on customers). "
            "Use ?refresh=true before listing after condition changes; ?verify=true to audit the page."
        )
    elif getattr(seg, "provider", "INTERNAL") == "UNOMI":
        payload = _list_unomi_members(db, seg=seg, limit=limit, offset=offset, source=source)
    else:
        payload = _list_internal_members(db, seg=seg, limit=limit, offset=offset, source=source)

    payload["refreshed"] = refreshed
    return _enrich_members_payload(db, seg=seg, payload=payload, verify=verify)


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
        "note": "Static UNOMI: manual_profile_ids synced as OR(itemId) condition in Unomi.",
    }
