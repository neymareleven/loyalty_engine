from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.event_type import EventType
from app.schemas.event_type import EventTypeCreate, EventTypeOut, EventTypeUpdate


router = APIRouter(prefix="/admin/event-types", tags=["admin-event-types"])


@router.get("", response_model=list[EventTypeOut])
def list_event_types(
    active_brand: str = Depends(get_active_brand),
    brand: str | None = None,
    include_global: bool = False,
    origin: str | None = None,
    active: bool | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(EventType)
    if brand is not None and brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    if include_global:
        q = q.filter((EventType.brand == active_brand) | (EventType.brand.is_(None)))
    else:
        q = q.filter(EventType.brand == active_brand)
    if origin:
        q = q.filter(EventType.origin == origin)
    if active is not None:
        q = q.filter(EventType.active.is_(active))
    return q.order_by(EventType.origin.asc(), EventType.brand.asc().nullsfirst(), EventType.key.asc()).all()


@router.post("", response_model=EventTypeOut)
def create_event_type(
    payload: EventTypeCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if payload.brand is not None and payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")
    existing = (
        db.query(EventType.id)
        .filter(EventType.key == payload.key)
        .filter(EventType.brand == active_brand)
        .filter(EventType.origin == payload.origin)
        .first()
    )
    if existing:
        raise HTTPException(status_code=400, detail="Event type key already exists")

    obj = EventType(
        brand=active_brand,
        key=payload.key,
        origin=payload.origin,
        name=payload.name,
        description=payload.description,
        payload_schema=payload.payload_schema,
        active=payload.active,
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/{event_type_id}", response_model=EventTypeOut)
def get_event_type(
    event_type_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(EventType).filter(EventType.id == event_type_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Event type not found")
    return obj


@router.patch("/{event_type_id}", response_model=EventTypeOut)
def update_event_type(
    event_type_id: UUID,
    payload: EventTypeUpdate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(EventType).filter(EventType.id == event_type_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Event type not found")
    if obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Event type not found")

    data = payload.model_dump(exclude_unset=True)
    if "brand" in data and data["brand"] is not None and data["brand"] != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")
    next_brand = active_brand
    next_key = data.get("key", obj.key)
    next_origin = data.get("origin", obj.origin)
    if next_key != obj.key or next_brand != obj.brand or next_origin != obj.origin:
        existing = (
            db.query(EventType.id)
            .filter(EventType.key == next_key)
            .filter(EventType.brand == next_brand)
            .filter(EventType.origin == next_origin)
            .filter(EventType.id != obj.id)
            .first()
        )
        if existing:
            raise HTTPException(status_code=400, detail="Event type key already exists")

    for k, v in data.items():
        if k == "brand":
            continue
        setattr(obj, k, v)

    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{event_type_id}")
def delete_event_type(
    event_type_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(EventType).filter(EventType.id == event_type_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Event type not found")
    if obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Event type not found")

    db.delete(obj)
    db.commit()
    return {"deleted": True}
