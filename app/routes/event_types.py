from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.event_type import TransactionType
from app.schemas.event_type import TransactionTypeCreate, TransactionTypeOut, TransactionTypeUpdate


router = APIRouter(prefix="/admin/transaction-types", tags=["admin-transaction-types"])


def _slug_key(value: str) -> str:
    s = (value or "").strip().lower()
    out = []
    prev_us = False
    for ch in s:
        ok = ("a" <= ch <= "z") or ("0" <= ch <= "9")
        if ok:
            out.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    key = "".join(out).strip("_")
    return key or "transaction"


@router.get("", response_model=list[TransactionTypeOut])
def list_transaction_types(
    active_brand: str = Depends(get_active_brand),
    brand: str | None = None,
    include_global: bool = False,
    origin: str | None = None,
    active: bool | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(TransactionType)
    if brand is not None and brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    if include_global:
        q = q.filter((TransactionType.brand == active_brand) | (TransactionType.brand.is_(None)))
    else:
        q = q.filter(TransactionType.brand == active_brand)
    if origin:
        q = q.filter(TransactionType.origin == origin)
    if active is not None:
        q = q.filter(TransactionType.active.is_(active))
    return q.order_by(
        TransactionType.origin.asc(),
        TransactionType.brand.asc().nullsfirst(),
        TransactionType.key.asc(),
    ).all()


@router.post("", response_model=TransactionTypeOut)
def create_transaction_type(
    payload: TransactionTypeCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if payload.brand is not None and payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")
    key_in = (payload.key or "").strip() or None
    key = key_in or _slug_key(payload.name)
    base = key
    i = 1
    while (
        db.query(TransactionType.id)
        .filter(TransactionType.key == key)
        .filter(TransactionType.brand == active_brand)
        .filter(TransactionType.origin == payload.origin)
        .first()
    ):
        i += 1
        key = f"{base}_{i}"

    obj = TransactionType(
        brand=active_brand,
        key=key,
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


@router.get("/{transaction_type_id}", response_model=TransactionTypeOut)
def get_transaction_type(
    transaction_type_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(TransactionType).filter(TransactionType.id == transaction_type_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Transaction type not found")
    return obj


@router.patch("/{transaction_type_id}", response_model=TransactionTypeOut)
def update_transaction_type(
    transaction_type_id: UUID,
    payload: TransactionTypeUpdate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(TransactionType).filter(TransactionType.id == transaction_type_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Transaction type not found")
    if obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Transaction type not found")

    data = payload.model_dump(exclude_unset=True)
    if "brand" in data and data["brand"] is not None and data["brand"] != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")
    next_brand = active_brand
    next_key = data.get("key", obj.key)
    next_origin = data.get("origin", obj.origin)
    if next_key != obj.key or next_brand != obj.brand or next_origin != obj.origin:
        existing = (
            db.query(TransactionType.id)
            .filter(TransactionType.key == next_key)
            .filter(TransactionType.brand == next_brand)
            .filter(TransactionType.origin == next_origin)
            .filter(TransactionType.id != obj.id)
            .first()
        )
        if existing:
            raise HTTPException(status_code=400, detail="Transaction type key already exists")

    for k, v in data.items():
        if k == "brand":
            continue
        setattr(obj, k, v)

    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{transaction_type_id}")
def delete_transaction_type(
    transaction_type_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(TransactionType).filter(TransactionType.id == transaction_type_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Transaction type not found")
    if obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Transaction type not found")

    db.delete(obj)
    db.commit()
    return {"deleted": True}
