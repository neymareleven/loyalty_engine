"""Unified segment membership checks (INTERNAL table vs Unomi live)."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.segment import Segment
from app.models.segment_member import SegmentMember
from app.services.unomi_segment_service import get_unomi_client, manual_profile_ids_list


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
    if getattr(segment, "provider", "INTERNAL") == "UNOMI":
        profile_ids = resolve_unomi_segment_profile_ids(db, segment=segment)
        if not profile_ids:
            return customer_query.filter(False)
        return customer_query.filter(Customer.profile_id.in_(profile_ids))

    return customer_query.join(SegmentMember, SegmentMember.customer_id == Customer.id).filter(
        SegmentMember.segment_id == segment.id
    )


def resolve_unomi_segment_profile_ids(db: Session, *, segment: Segment) -> list[str]:
    """Profile IDs for targeting: manual list + Unomi impacted (union)."""
    ids = set(manual_profile_ids_list(segment))

    if segment.unomi_segment_id:
        client = get_unomi_client(db, brand=segment.brand)
        if client:
            try:
                ids.update(client.get_impacted_profile_ids(segment.unomi_segment_id))
            except Exception:
                pass
            if not ids and segment.unomi_segment_id:
                for cust in db.query(Customer.profile_id).filter(Customer.brand == segment.brand).all():
                    pid = (cust.profile_id or "").strip()
                    if not pid:
                        continue
                    try:
                        if client.is_profile_in_segment(profile_id=pid, segment_id=segment.unomi_segment_id):
                            ids.add(pid)
                    except Exception:
                        continue
                    if len(ids) >= 500:
                        break

    return sorted(ids)


def _is_customer_in_unomi_segment(db: Session, *, customer: Customer, segment: Segment) -> bool:
    pid = (customer.profile_id or "").strip()
    if not pid:
        return False

    manual = manual_profile_ids_list(segment)
    if pid in manual:
        return True

    if not segment.unomi_segment_id:
        return False

    client = get_unomi_client(db, brand=segment.brand)
    if not client:
        return (
            db.query(SegmentMember.customer_id)
            .filter(SegmentMember.segment_id == segment.id)
            .filter(SegmentMember.customer_id == customer.id)
            .first()
            is not None
        )

    try:
        return client.is_profile_in_segment(profile_id=pid, segment_id=segment.unomi_segment_id)
    except Exception:
        return pid in manual
