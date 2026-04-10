from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.coupon_type import CouponType
from app.models.customer_coupon import CustomerCoupon
from app.models.reward_category import RewardCategory
from app.schemas.coupon_type import CouponTypeCreate, CouponTypeOut, CouponTypeUpdate


router = APIRouter(prefix="/admin/coupon-types", tags=["admin-coupon-types"])

def _pgcode(err: IntegrityError) -> str | None:
    orig = getattr(err, "orig", None)
    code = getattr(orig, "pgcode", None)
    if code:
        return str(code)
    return None


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

    linked_category = (
        db.query(RewardCategory.id)
        .filter(RewardCategory.brand == active_brand)
        .filter(RewardCategory.coupon_type_id == obj.id)
        .first()
    )
    if linked_category:
        raise HTTPException(
            status_code=409,
            detail=(
                "Impossible de supprimer ce type de coupon car il est lié à au moins une catégorie de récompense. "
                "Supprimez ou modifiez d'abord les catégories associées."
            ),
        )

    linked_customer_coupon = (
        db.query(CustomerCoupon.id)
        .filter(CustomerCoupon.coupon_type_id == obj.id)
        .first()
    )
    if linked_customer_coupon:
        raise HTTPException(
            status_code=409,
            detail=(
                "Impossible de supprimer ce type de coupon car des coupons clients existent déjà. "
                "Action recommandée: désactivez le type de coupon au lieu de le supprimer."
            ),
        )

    db.delete(obj)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        code = _pgcode(e)
        if code == "23503":
            raise HTTPException(
                status_code=409,
                detail=(
                    "Impossible de supprimer ce type de coupon car il est encore référencé par d'autres données. "
                    "Supprimez d'abord les dépendances ou désactivez le type."
                ),
            )
        raise HTTPException(
            status_code=409,
            detail="Impossible de supprimer ce type de coupon (conflit de données).",
        )
    return {"deleted": True}
