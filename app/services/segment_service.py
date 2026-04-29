from datetime import datetime

from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.segment import Segment
from app.models.segment_member import SegmentMember


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

    from app.services.rule_engine import _evaluate_ast_condition  # noqa

    processed_segments = 0
    touched_members = 0

    for seg in segs:
        processed_segments += 1

        # Replace all dynamic members for this segment (keep STATIC ones if any)
        db.query(SegmentMember).filter(SegmentMember.segment_id == seg.id).filter(SegmentMember.source == "DYNAMIC").delete(
            synchronize_session=False
        )

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

                # Segment conditions are customer-only; use dummy transaction.
                tx = type("SegTx", (), {"payload": {}, "brand": brand})()
                try:
                    matched = _evaluate_ast_condition(db=db, customer=c, transaction=tx, node=seg.conditions)
                except Exception:
                    matched = False

                if not matched:
                    continue

                db.add(
                    SegmentMember(
                        segment_id=seg.id,
                        customer_id=c.id,
                        source="DYNAMIC",
                        computed_at=now_utc,
                    )
                )
                touched_members += 1

            if len(customers) < batch_size:
                break

        seg.last_computed_at = now_utc
        db.flush()

    return {
        "brand": brand,
        "segments": int(processed_segments),
        "members": int(touched_members),
        "computed_at": now_utc,
    }
