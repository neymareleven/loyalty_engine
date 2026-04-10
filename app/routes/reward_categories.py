from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.coupon_type import CouponType
from app.models.reward import Reward
from app.models.reward_category import RewardCategory
from app.schemas.reward_category import RewardCategoryCreate, RewardCategoryOut, RewardCategoryUpdate


router = APIRouter(prefix="/admin/reward-categories", tags=["admin-reward-categories"])

def _pgcode(err: IntegrityError) -> str | None:
    orig = getattr(err, "orig", None)
    code = getattr(orig, "pgcode", None)
    if code:
        return str(code)
    return None


@router.get("", response_model=list[RewardCategoryOut])
def list_reward_categories(
    active_brand: str = Depends(get_active_brand),
    brand: str | None = None,
    active: bool | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(RewardCategory)
    if brand is not None and brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    q = q.filter(RewardCategory.brand == active_brand)
    if active is not None:
        q = q.filter(RewardCategory.active.is_(active))
    return q.order_by(RewardCategory.created_at.desc()).all()


@router.post("", response_model=RewardCategoryOut)
def create_reward_category(
    payload: RewardCategoryCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if payload.brand is not None and payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    ct = db.query(CouponType).filter(CouponType.id == payload.coupon_type_id).first()
    if not ct or ct.brand != active_brand:
        raise HTTPException(status_code=400, detail="coupon_type_id not found")

    obj = RewardCategory(
        brand=active_brand,
        coupon_type_id=payload.coupon_type_id,
        name=payload.name,
        description=payload.description,
        active=payload.active,
    )
    db.add(obj)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Reward category could not be saved")
    db.refresh(obj)
    return obj


@router.get("/{reward_category_id}", response_model=RewardCategoryOut)
def get_reward_category(
    reward_category_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(RewardCategory).filter(RewardCategory.id == reward_category_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Reward category not found")
    return obj


@router.patch("/{reward_category_id}", response_model=RewardCategoryOut)
def update_reward_category(
    reward_category_id: UUID,
    payload: RewardCategoryUpdate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(RewardCategory).filter(RewardCategory.id == reward_category_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Reward category not found")

    data = payload.model_dump(exclude_unset=True)

    if "coupon_type_id" in data and data["coupon_type_id"] is not None:
        ct = db.query(CouponType).filter(CouponType.id == data["coupon_type_id"]).first()
        if not ct or ct.brand != active_brand:
            raise HTTPException(status_code=400, detail="coupon_type_id not found")

    for k, v in data.items():
        setattr(obj, k, v)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail="Reward category could not be saved")
    db.refresh(obj)
    return obj


@router.delete("/{reward_category_id}")
def delete_reward_category(
    reward_category_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(RewardCategory).filter(RewardCategory.id == reward_category_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Reward category not found")

    linked_reward = (
        db.query(Reward.id)
        .filter(Reward.brand == active_brand)
        .filter(Reward.reward_category_id == obj.id)
        .first()
    )
    if linked_reward:
        raise HTTPException(
            status_code=409,
            detail=(
                "Impossible de supprimer cette catégorie car elle est liée à au moins une récompense. "
                "Supprimez ou réaffectez d'abord les récompenses associées."
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
                    "Impossible de supprimer cette catégorie car elle est encore référencée par d'autres données. "
                    "Supprimez d'abord les dépendances."
                ),
            )
        raise HTTPException(
            status_code=409,
            detail="Impossible de supprimer cette catégorie (conflit de données).",
        )
    return {"deleted": True}
