from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.coupon_type import CouponType
from app.schemas.coupon_type import CouponTypeCreate, CouponTypeOut, CouponTypeUpdate


router = APIRouter(prefix="/admin/coupon-types", tags=["admin-coupon-types"])


@router.get("", response_model=list[CouponTypeOut])
def list_coupon_types(
    active_brand: str = Depends(get_active_brand),
    brand: str | None = None,
    active: bool | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(CouponType)
    if brand is not None and brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    q = q.filter(CouponType.brand == active_brand)
    if active is not None:
        q = q.filter(CouponType.active.is_(active))
    return q.order_by(CouponType.created_at.desc()).all()


@router.post("", response_model=CouponTypeOut)
def create_coupon_type(
    payload: CouponTypeCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if payload.brand is not None and payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    obj = CouponType(
        brand=active_brand,
        name=payload.name,
        description=payload.description,
        active=payload.active,
    )
    db.add(obj)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Coupon type could not be saved")
    db.refresh(obj)
    return obj


@router.get("/{coupon_type_id}", response_model=CouponTypeOut)
def get_coupon_type(
    coupon_type_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(CouponType).filter(CouponType.id == coupon_type_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Coupon type not found")
    return obj


@router.patch("/{coupon_type_id}", response_model=CouponTypeOut)
def update_coupon_type(
    coupon_type_id: UUID,
    payload: CouponTypeUpdate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(CouponType).filter(CouponType.id == coupon_type_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Coupon type not found")

    data = payload.model_dump(exclude_unset=True)

    for k, v in data.items():
        setattr(obj, k, v)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Coupon type could not be saved")
    db.refresh(obj)
    return obj


@router.delete("/{coupon_type_id}")
def delete_coupon_type(
    coupon_type_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(CouponType).filter(CouponType.id == coupon_type_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Coupon type not found")

    db.delete(obj)
    db.commit()
    return {"deleted": True}
