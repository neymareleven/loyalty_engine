from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.customer import Customer
from app.models.segment import Segment
from app.models.segment_member import SegmentMember
from app.schemas.segment import (
    SegmentCreate,
    SegmentMemberCreate,
    SegmentMemberOut,
    SegmentOut,
    SegmentUpdate,
)


router = APIRouter(prefix="/admin/segments", tags=["admin-segments"])


def _pgcode(err: IntegrityError) -> str | None:
    orig = getattr(err, "orig", None)
    code = getattr(orig, "pgcode", None)
    if code:
        return str(code)
    return None


@router.get("", response_model=list[SegmentOut])
def list_segments(
    active_brand: str = Depends(get_active_brand),
    active: bool | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(Segment).filter(Segment.brand == active_brand)
    if active is not None:
        q = q.filter(Segment.active.is_(active))
    return q.order_by(Segment.created_at.desc()).all()


@router.post("", response_model=SegmentOut)
def create_segment(
    payload: SegmentCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if payload.brand is not None and payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    if payload.is_dynamic:
        if payload.conditions is None:
            raise HTTPException(status_code=400, detail="Dynamic segments require conditions")
    else:
        if payload.conditions is not None:
            raise HTTPException(status_code=400, detail="Static segments cannot have conditions")

    obj = Segment(
        brand=active_brand,
        name=payload.name,
        description=payload.description,
        is_dynamic=payload.is_dynamic,
        conditions=payload.conditions,
        active=payload.active,
    )
    db.add(obj)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Segment could not be saved")
    db.refresh(obj)
    return obj


@router.get("/{segment_id}", response_model=SegmentOut)
def get_segment(
    segment_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(Segment).filter(Segment.id == segment_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Segment not found")
    return obj


@router.patch("/{segment_id}", response_model=SegmentOut)
def update_segment(
    segment_id: UUID,
    payload: SegmentUpdate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(Segment).filter(Segment.id == segment_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Segment not found")

    data = payload.model_dump(exclude_unset=True)

    next_is_dynamic = data.get("is_dynamic", obj.is_dynamic)
    next_conditions = data.get("conditions", obj.conditions)

    if next_is_dynamic:
        if next_conditions is None:
            raise HTTPException(status_code=400, detail="Dynamic segments require conditions")
    else:
        if next_conditions is not None:
            raise HTTPException(status_code=400, detail="Static segments cannot have conditions")

    for k, v in data.items():
        setattr(obj, k, v)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Segment could not be saved")

    db.refresh(obj)
    return obj


@router.delete("/{segment_id}")
def delete_segment(
    segment_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(Segment).filter(Segment.id == segment_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Segment not found")

    db.delete(obj)
    db.commit()
    return {"deleted": True}


@router.get("/{segment_id}/members", response_model=list[SegmentMemberOut])
def list_segment_members(
    segment_id: UUID,
    active_brand: str = Depends(get_active_brand),
    source: str | None = None,
    db: Session = Depends(get_db),
):
    seg = db.query(Segment).filter(Segment.id == segment_id).first()
    if not seg or seg.brand != active_brand:
        raise HTTPException(status_code=404, detail="Segment not found")

    q = db.query(SegmentMember).filter(SegmentMember.segment_id == segment_id)
    if source:
        q = q.filter(SegmentMember.source == str(source))
    return q.order_by(SegmentMember.created_at.desc()).limit(500).all()


@router.post("/{segment_id}/members", response_model=SegmentMemberOut)
def add_segment_member(
    segment_id: UUID,
    payload: SegmentMemberCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    seg = db.query(Segment).filter(Segment.id == segment_id).first()
    if not seg or seg.brand != active_brand:
        raise HTTPException(status_code=404, detail="Segment not found")
    if seg.is_dynamic:
        raise HTTPException(status_code=400, detail="Cannot manually edit members of a dynamic segment")

    cust = db.query(Customer).filter(Customer.id == payload.customer_id).first()
    if not cust or cust.brand != active_brand:
        raise HTTPException(status_code=400, detail="Customer not found for this brand")

    m = SegmentMember(segment_id=segment_id, customer_id=payload.customer_id, source="STATIC")
    db.add(m)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Customer already in segment")
    db.refresh(m)
    return m


@router.delete("/{segment_id}/members/{customer_id}")
def remove_segment_member(
    segment_id: UUID,
    customer_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    seg = db.query(Segment).filter(Segment.id == segment_id).first()
    if not seg or seg.brand != active_brand:
        raise HTTPException(status_code=404, detail="Segment not found")
    if seg.is_dynamic:
        raise HTTPException(status_code=400, detail="Cannot manually edit members of a dynamic segment")

    m = (
        db.query(SegmentMember)
        .filter(SegmentMember.segment_id == segment_id)
        .filter(SegmentMember.customer_id == customer_id)
        .first()
    )
    if not m:
        raise HTTPException(status_code=404, detail="Segment member not found")

    db.delete(m)
    db.commit()
    return {"deleted": True}
