"""Unified segment membership checks (INTERNAL table vs Unomi live)."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.segment import Segment
from app.models.segment_member import SegmentMember
from app.services.unomi_segment_service import manual_profile_ids_list


def unomi_dynamic_uses_engine_membership(segment: Segment) -> bool:
    """UNOMI dynamic segments: membership computed in the engine (same as INTERNAL)."""
    return (getattr(segment, "provider", None) or "INTERNAL") == "UNOMI" and bool(segment.is_dynamic)


def _dynamic_segment_profile_ids(db: Session, *, segment: Segment) -> list[str]:
    rows = (
        db.query(Customer.profile_id)
        .join(SegmentMember, SegmentMember.customer_id == Customer.id)
        .filter(SegmentMember.segment_id == segment.id)
        .filter(SegmentMember.source == "DYNAMIC")
        .filter(Customer.brand == segment.brand)
        .all()
    )
    return sorted({str(pid).strip() for (pid,) in rows if pid and str(pid).strip()})


def is_customer_in_segment(db: Session, *, customer: Customer, segment: Segment) -> bool:
    if segment.brand != customer.brand:
        return False

    if getattr(segment, "provider", "INTERNAL") == "UNOMI":
        return _is_customer_in_unomi_segment(db, customer=customer, segment=segment)

    return (
        db.query(SegmentMember.customer_id)
        .filter(SegmentMember.segment_id == segment.id)
        .filter(SegmentMember.customer_id == customer.id)
        .first()
        is not None
    )


def is_customer_in_any_segment(db: Session, *, customer: Customer, segment_ids: list[UUID]) -> bool:
    if not segment_ids:
        return True

    segments = (
        db.query(Segment)
        .filter(Segment.id.in_(segment_ids))
        .filter(Segment.brand == customer.brand)
        .filter(Segment.active.is_(True))
        .all()
    )
    if not segments:
        return False

    for seg in segments:
        if is_customer_in_segment(db, customer=customer, segment=seg):
            return True
    return False


def filter_customers_by_segment(
    db: Session,
    *,
    brand: str,
    segment: Segment,
    customer_query,
):
    """Apply segment filter to a Customer query (for internal jobs)."""
    if unomi_dynamic_uses_engine_membership(segment):
        return customer_query.join(SegmentMember, SegmentMember.customer_id == Customer.id).filter(
            SegmentMember.segment_id == segment.id,
            SegmentMember.source == "DYNAMIC",
        )

    if getattr(segment, "provider", "INTERNAL") == "UNOMI":
        profile_ids = resolve_unomi_segment_profile_ids(db, segment=segment)
        if not profile_ids:
            return customer_query.filter(False)
        return customer_query.filter(Customer.profile_id.in_(profile_ids))

    return customer_query.join(SegmentMember, SegmentMember.customer_id == Customer.id).filter(
        SegmentMember.segment_id == segment.id
    )


def resolve_unomi_segment_profile_ids(db: Session, *, segment: Segment) -> list[str]:
    """Profile IDs for UNOMI static segments (manual_profile_ids only)."""
    if unomi_dynamic_uses_engine_membership(segment):
        return _dynamic_segment_profile_ids(db, segment=segment)
    return manual_profile_ids_list(segment)


def _is_customer_in_unomi_segment(db: Session, *, customer: Customer, segment: Segment) -> bool:
    if unomi_dynamic_uses_engine_membership(segment):
        return (
            db.query(SegmentMember.customer_id)
            .filter(SegmentMember.segment_id == segment.id)
            .filter(SegmentMember.customer_id == customer.id)
            .filter(SegmentMember.source == "DYNAMIC")
            .first()
            is not None
        )

    pid = (customer.profile_id or "").strip()
    if not pid:
        return False
    return pid in manual_profile_ids_list(segment)
