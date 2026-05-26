from datetime import datetime

from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.segment import Segment
from app.models.segment_member import SegmentMember


def recompute_dynamic_segment(
    db: Session,
    *,
    segment: Segment,
    now_utc: datetime | None = None,
    batch_size: int = 500,
) -> dict:
    """Recompute membership for one active dynamic segment."""
    if now_utc is None:
        now_utc = datetime.utcnow()

    if not segment.is_dynamic:
        raise ValueError("Segment is not dynamic")
    if not segment.active:
        raise ValueError("Segment is not active")
    if segment.conditions is None:
        raise ValueError("Dynamic segment requires conditions")

    brand = segment.brand
    from app.services.rule_engine import _evaluate_ast_condition  # noqa

    # Dynamic membership is DYNAMIC-only; drop legacy STATIC rows if any.
    db.query(SegmentMember).filter(SegmentMember.segment_id == segment.id).filter(
        SegmentMember.source == "STATIC"
    ).delete(synchronize_session=False)
    db.query(SegmentMember).filter(SegmentMember.segment_id == segment.id).filter(
        SegmentMember.source == "DYNAMIC"
    ).delete(synchronize_session=False)

    touched_members = 0
    cursor = None
    while True:
        q = db.query(Customer).filter(Customer.brand == brand).order_by(Customer.id.asc())
        if cursor is not None:
            q = q.filter(Customer.id > cursor)
        customers = q.limit(batch_size).all()
        if not customers:
            break

        for c in customers:
            cursor = c.id
            tx = type("SegTx", (), {"payload": {}, "brand": brand})()
            try:
                matched = _evaluate_ast_condition(db=db, customer=c, transaction=tx, node=segment.conditions)
            except Exception:
                matched = False

            if not matched:
                continue

            db.add(
                SegmentMember(
                    segment_id=segment.id,
                    customer_id=c.id,
                    source="DYNAMIC",
                    computed_at=now_utc,
                )
            )
            touched_members += 1

        if len(customers) < batch_size:
            break

    segment.last_computed_at = now_utc
    db.flush()

    return {
        "brand": brand,
        "segments": 1,
        "members": int(touched_members),
        "computed_at": now_utc,
    }


def recompute_dynamic_segments_for_brand(
    db: Session,
    *,
    brand: str,
    now_utc: datetime | None = None,
    batch_size: int = 500,
) -> dict:
    if now_utc is None:
        now_utc = datetime.utcnow()

    segs = (
        db.query(Segment)
        .filter(Segment.brand == brand)
        .filter(Segment.active.is_(True))
        .filter(Segment.is_dynamic.is_(True))
        .order_by(Segment.created_at.asc())
        .all()
    )

    processed_segments = 0
    touched_members = 0

    for seg in segs:
        if seg.conditions is None:
            continue
        stats = recompute_dynamic_segment(db, segment=seg, now_utc=now_utc, batch_size=batch_size)
        processed_segments += 1
        touched_members += int(stats["members"])

    return {
        "brand": brand,
        "segments": int(processed_segments),
        "members": int(touched_members),
        "computed_at": now_utc,
    }
