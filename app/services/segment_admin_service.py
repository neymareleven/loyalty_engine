"""Segment admin helpers: metadata, deletion guards, recompute orchestration."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.internal_job import InternalJob
from app.models.rule import Rule
from app.models.segment import Segment
from app.models.segment_member import SegmentMember
from app.services.segment_service import recompute_dynamic_segment, recompute_dynamic_segments_for_brand
from app.services.segment_membership_service import unomi_dynamic_uses_engine_membership
from app.services.unomi_segment_service import manual_profile_ids_list


def segment_member_counts(db: Session, *, segment_id: UUID, seg: Segment | None = None) -> dict[str, int]:
    if seg is None:
        seg = db.query(Segment).filter(Segment.id == segment_id).first()

    if seg and getattr(seg, "provider", "INTERNAL") == "UNOMI" and not unomi_dynamic_uses_engine_membership(seg):
        manual = len(manual_profile_ids_list(seg))
        return {
            "member_count": manual,
            "member_count_dynamic": 0,
            "member_count_static": manual,
        }

    rows = (
        db.query(SegmentMember.source, func.count())
        .filter(SegmentMember.segment_id == segment_id)
        .group_by(SegmentMember.source)
        .all()
    )
    by_source = {str(source): int(count) for source, count in rows}
    dynamic = by_source.get("DYNAMIC", 0)
    static = by_source.get("STATIC", 0)
    return {
        "member_count": dynamic + static,
        "member_count_dynamic": dynamic,
        "member_count_static": static,
    }


def segment_needs_recompute(seg: Segment) -> bool:
    if not seg.is_dynamic or not seg.active:
        return False
    if seg.last_computed_at is None:
        return True
    updated = getattr(seg, "updated_at", None)
    if updated is not None and updated > seg.last_computed_at:
        return True
    return False


def segment_referencing_rules(db: Session, *, brand: str, segment_id: UUID) -> list[dict]:
    rules = (
        db.query(Rule.id, Rule.name, Rule.active)
        .filter(Rule.brand == brand)
        .filter(Rule.segment_ids.isnot(None))
        .filter(Rule.segment_ids.contains([segment_id]))
        .order_by(Rule.name.asc())
        .all()
    )
    return [
        {"id": str(rid), "name": name, "active": bool(active)}
        for rid, name, active in rules
    ]


def segment_referencing_internal_jobs(db: Session, *, brand: str, segment_id: UUID) -> list[dict]:
    jobs = (
        db.query(InternalJob.id, InternalJob.name, InternalJob.job_key, InternalJob.active)
        .filter(InternalJob.brand == brand)
        .filter(InternalJob.segment_id == segment_id)
        .order_by(InternalJob.name.asc())
        .all()
    )
    return [
        {
            "id": str(jid),
            "name": name,
            "jobKey": job_key,
            "active": bool(active),
        }
        for jid, name, job_key, active in jobs
    ]


def segment_deletion_meta(db: Session, *, seg: Segment) -> dict:
    rules = segment_referencing_rules(db, brand=seg.brand, segment_id=seg.id)
    jobs = segment_referencing_internal_jobs(db, brand=seg.brand, segment_id=seg.id)
    blocked = bool(rules or jobs)
    return {
        "referencing_rules": rules,
        "referencing_internal_jobs": jobs,
        "referencing_rules_count": len(rules),
        "referencing_internal_jobs_count": len(jobs),
        "can_delete": not blocked,
        "recommended_action": None if not blocked else "detach_references",
    }


def assert_segment_deletable(db: Session, *, seg: Segment) -> None:
    meta = segment_deletion_meta(db, seg=seg)
    if meta["can_delete"]:
        return
    raise HTTPException(
        status_code=409,
        detail={
            "message": (
                "Impossible de supprimer ce segment : il est encore référencé par des règles ou des jobs internes. "
                "Retirez-le de segment_ids / segment_id avant suppression."
            ),
            "referencingRules": meta["referencing_rules"],
            "referencingInternalJobs": meta["referencing_internal_jobs"],
            "recommendedAction": meta["recommended_action"],
        },
    )


def serialize_segment_out(db: Session, *, seg: Segment) -> dict:
    counts = segment_member_counts(db, segment_id=seg.id, seg=seg)
    refs = segment_deletion_meta(db, seg=seg)
    needs = segment_needs_recompute(seg)
    has_loyalty_ast = seg.conditions is not None
    has_unomi = getattr(seg, "unomi_condition", None) is not None
    if has_loyalty_ast:
        conditions_format = "loyalty_ast"
    elif has_unomi:
        conditions_format = "unomi_only"
    else:
        conditions_format = None
    return {
        "id": seg.id,
        "brand": seg.brand,
        "name": seg.name,
        "description": seg.description,
        "is_dynamic": seg.is_dynamic,
        "conditions": seg.conditions,
        "provider": getattr(seg, "provider", None) or "INTERNAL",
        "unomi_segment_id": getattr(seg, "unomi_segment_id", None),
        "unomi_scope": getattr(seg, "unomi_scope", None),
        "manual_profile_ids": manual_profile_ids_list(seg),
        "unomi_condition": getattr(seg, "unomi_condition", None),
        "active": seg.active,
        "last_computed_at": seg.last_computed_at,
        "created_at": seg.created_at,
        "updated_at": seg.updated_at,
        **counts,
        "referencing_rules_count": refs["referencing_rules_count"],
        "referencing_internal_jobs_count": refs["referencing_internal_jobs_count"],
        "can_delete": refs["can_delete"],
        "recommended_action": refs["recommended_action"],
        "needs_recompute": needs,
        "conditions_format": conditions_format,
    }


def assert_no_static_members_on_dynamic(db: Session, *, seg: Segment, action: str) -> None:
    if getattr(seg, "provider", "INTERNAL") == "UNOMI":
        return
    if not seg.is_dynamic:
        return
    static_count = (
        db.query(SegmentMember)
        .filter(SegmentMember.segment_id == seg.id)
        .filter(SegmentMember.source == "STATIC")
        .count()
    )
    if static_count > 0:
        raise HTTPException(
            status_code=409,
            detail={
                "message": (
                    f"Cannot {action}: dynamic segment has manual (STATIC) members. "
                    "Remove them via bulk-delete or switch segment to static first."
                ),
                "memberCountStatic": static_count,
            },
        )


def apply_segment_type_transition(
    db: Session,
    *,
    seg: Segment,
    next_is_dynamic: bool,
    clear_static_on_dynamic: bool = False,
) -> None:
    """Handle is_dynamic changes and keep STATIC/DYNAMIC membership consistent."""
    if seg.is_dynamic == next_is_dynamic:
        return

    if next_is_dynamic:
        if clear_static_on_dynamic:
            db.query(SegmentMember).filter(SegmentMember.segment_id == seg.id).filter(
                SegmentMember.source == "STATIC"
            ).delete(synchronize_session=False)
        else:
            assert_no_static_members_on_dynamic(db, seg=seg, action="switch to dynamic")
        seg.conditions = seg.conditions or {}
    else:
        db.query(SegmentMember).filter(SegmentMember.segment_id == seg.id).filter(
            SegmentMember.source == "DYNAMIC"
        ).delete(synchronize_session=False)
        seg.conditions = None
        seg.last_computed_at = None

    seg.is_dynamic = next_is_dynamic
    db.flush()


def purge_dynamic_members(db: Session, *, segment_id: UUID) -> int:
    return (
        db.query(SegmentMember)
        .filter(SegmentMember.segment_id == segment_id)
        .filter(SegmentMember.source == "DYNAMIC")
        .delete(synchronize_session=False)
    )


def trigger_recompute_if_needed(
    db: Session,
    *,
    seg: Segment,
    recompute: bool,
    batch_size: int = 500,
) -> dict | None:
    if not recompute or not seg.is_dynamic or not seg.active:
        return None
    if seg.conditions is None:
        return None
    return recompute_dynamic_segment(db, segment=seg, batch_size=batch_size)


def recompute_brand_dynamic_segments(
    db: Session,
    *,
    brand: str,
    batch_size: int = 500,
    now_utc: datetime | None = None,
) -> dict:
    return recompute_dynamic_segments_for_brand(db, brand=brand, now_utc=now_utc, batch_size=batch_size)
