from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.coupon_type import CouponType
from app.schemas.coupon_type import (
    CouponTypeCreate,
    CouponTypeOut,
    CouponTypeRewardSummary,
    CouponTypeRewardsReplace,
    CouponTypeUpdate,
)
from app.schemas.catalog_delete import CatalogDeletePreviewOut
from app.services.catalog_admin_service import coupon_type_deletion_meta
from app.services.catalog_invalidation_service import (
    apply_coupon_type_catalog_delete,
    preview_coupon_type_delete,
)
from app.services.coupon_rewards_service import (
    list_coupon_type_reward_ids,
    replace_coupon_type_rewards,
    resolve_rewards_catalog,
)


router = APIRouter(prefix="/admin/coupon-types", tags=["admin-coupon-types"])


def _serialize_coupon_type_out(*, db: Session, obj: CouponType) -> dict:
    rewards = resolve_rewards_catalog(db, coupon_type=obj, active_only=None)
    reward_ids = list_coupon_type_reward_ids(db, coupon_type_id=obj.id)
    deletion = coupon_type_deletion_meta(db, coupon_type_id=obj.id)
    return {
        "id": obj.id,
        "brand": obj.brand,
        "name": obj.name,
        "description": obj.description,
        "validity_days": obj.validity_days,
        "reward_ids": reward_ids,
        "rewards": [
            CouponTypeRewardSummary(
                id=r.id,
                name=r.name,
                active=bool(r.active),
            )
            for r in rewards
        ],
        "active": obj.active,
        "customer_coupon_count": deletion["customer_coupon_count"],
        "customer_coupons_issued": deletion.get("customer_coupons_issued", 0),
        "can_delete": deletion["can_delete"],
        "recommended_action": deletion["recommended_action"],
        "created_at": obj.created_at,
        "updated_at": obj.updated_at,
    }


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
    items = q.order_by(CouponType.created_at.desc()).all()
    return [_serialize_coupon_type_out(db=db, obj=obj) for obj in items]


@router.post("", response_model=CouponTypeOut)
def create_coupon_type(
    payload: CouponTypeCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if payload.brand is not None and payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    validity_days = payload.validity_days
    if validity_days is not None:
        try:
            validity_days = int(validity_days)
        except Exception:
            raise HTTPException(status_code=400, detail="validity_days must be an integer")
        if validity_days < 0:
            raise HTTPException(status_code=400, detail="validity_days must be >= 0")

    obj = CouponType(
        brand=active_brand,
        name=payload.name,
        description=payload.description,
        validity_days=validity_days,
        active=payload.active,
    )
    db.add(obj)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=400,
            detail=(
                "Coupon type could not be saved. "
                "Causes possibles: un autre type de coupon existe déjà avec des informations similaires, ou des données invalides."
            ),
        )
    db.refresh(obj)
    return _serialize_coupon_type_out(db=db, obj=obj)


@router.get("/{coupon_type_id}", response_model=CouponTypeOut)
def get_coupon_type(
    coupon_type_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(CouponType).filter(CouponType.id == coupon_type_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Coupon type not found")
    return _serialize_coupon_type_out(db=db, obj=obj)


@router.get("/{coupon_type_id}/rewards", response_model=list[CouponTypeRewardSummary])
def list_coupon_type_rewards_endpoint(
    coupon_type_id: UUID,
    active_brand: str = Depends(get_active_brand),
    active: bool | None = True,
    db: Session = Depends(get_db),
):
    obj = db.query(CouponType).filter(CouponType.id == coupon_type_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Coupon type not found")
    rewards = resolve_rewards_catalog(db, coupon_type=obj, active_only=active)
    return [
        CouponTypeRewardSummary(
            id=r.id,
            name=r.name,
            active=bool(r.active),
        )
        for r in rewards
    ]


@router.put("/{coupon_type_id}/rewards", response_model=CouponTypeOut)
def replace_coupon_type_rewards_endpoint(
    coupon_type_id: UUID,
    payload: CouponTypeRewardsReplace,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(CouponType).filter(CouponType.id == coupon_type_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Coupon type not found")
    replace_coupon_type_rewards(db, coupon_type=obj, reward_ids=payload.reward_ids, brand=active_brand)
    db.commit()
    db.refresh(obj)
    return _serialize_coupon_type_out(db=db, obj=obj)


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
    reward_ids = data.pop("reward_ids", None)

    if "validity_days" in data and data["validity_days"] is not None:
        try:
            v = int(data["validity_days"])
        except Exception:
            raise HTTPException(status_code=400, detail="validity_days must be an integer")
        if v < 0:
            raise HTTPException(status_code=400, detail="validity_days must be >= 0")
        data["validity_days"] = v

    for k, v in data.items():
        setattr(obj, k, v)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=400,
            detail=(
                "Coupon type could not be saved. "
                "Causes possibles: un autre type de coupon existe déjà avec des informations similaires, ou des données invalides."
            ),
        )
    db.refresh(obj)
    if reward_ids is not None:
        replace_coupon_type_rewards(db, coupon_type=obj, reward_ids=reward_ids, brand=active_brand)
        db.commit()
        db.refresh(obj)
    return _serialize_coupon_type_out(db=db, obj=obj)


@router.get("/{coupon_type_id}/delete-preview", response_model=CatalogDeletePreviewOut)
def preview_delete_coupon_type(
    coupon_type_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(CouponType).filter(CouponType.id == coupon_type_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Coupon type not found")
    data = preview_coupon_type_delete(db, coupon_type=obj)
    return CatalogDeletePreviewOut(**data)


@router.delete("/{coupon_type_id}")
def delete_coupon_type(
    coupon_type_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(CouponType).filter(CouponType.id == coupon_type_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Coupon type not found")

    invalidation = apply_coupon_type_catalog_delete(db, coupon_type=obj)
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
    return {"deleted": True, **invalidation}
