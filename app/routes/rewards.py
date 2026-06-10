from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.coupon_type import CouponType
from app.models.product import Product
from app.models.reward import Reward
from app.models.reward_product import RewardProduct
from app.schemas.reward import CouponTypeLinkSummary, RewardCreate, RewardUpdate, RewardOut
from app.schemas.catalog_delete import CatalogDeletePreviewOut
from app.services.catalog_invalidation_service import (
    apply_reward_catalog_delete,
    preview_reward_delete,
)
from app.services.coupon_rewards_service import link_reward_to_coupon_types, list_reward_coupon_type_ids


router = APIRouter(prefix="/rewards", tags=["rewards"])


def _pgcode(err: IntegrityError) -> str | None:
    orig = getattr(err, "orig", None)
    code = getattr(orig, "pgcode", None)
    if code:
        return str(code)
    return None


def _serialize_reward_out(*, db: Session, reward: Reward) -> dict:
    links = db.query(RewardProduct).filter(RewardProduct.reward_id == reward.id).all()
    product_ids = [l.product_id for l in links]
    products = []
    if product_ids:
        products = db.query(Product).filter(Product.id.in_(product_ids)).all()
    prod_map = {p.id: p for p in products}

    coupon_type_ids = list_reward_coupon_type_ids(db, reward_id=reward.id)
    coupon_types = []
    if coupon_type_ids:
        coupon_types = (
            db.query(CouponType)
            .filter(CouponType.id.in_(coupon_type_ids))
            .order_by(CouponType.name.asc())
            .all()
        )

    return {
        "id": reward.id,
        "brand": reward.brand,
        "name": reward.name,
        "description": reward.description,
        "active": reward.active,
        "coupon_type_ids": coupon_type_ids,
        "coupon_types": [
            CouponTypeLinkSummary(id=ct.id, name=ct.name, active=bool(ct.active))
            for ct in coupon_types
        ],
        "created_at": getattr(reward, "created_at", None),
        "products": [
            {
                "product_id": l.product_id,
                "quantity": l.quantity,
                "name": getattr(prod_map.get(l.product_id), "name", None),
                "match_key": getattr(prod_map.get(l.product_id), "match_key", None),
                "points_value": getattr(prod_map.get(l.product_id), "points_value", None),
            }
            for l in links
        ],
    }


def _replace_reward_products(*, db: Session, reward: Reward, active_brand: str, items) -> None:
    if items is None:
        return

    if not isinstance(items, list):
        raise HTTPException(status_code=400, detail="products must be a list")

    dedup: dict = {}
    for it in items:
        if isinstance(it, dict):
            pid = it.get("product_id")
            qty = it.get("quantity", 1)
        else:
            pid = getattr(it, "product_id", None)
            qty = getattr(it, "quantity", 1)
        if pid is None:
            raise HTTPException(status_code=400, detail="products items must be objects")
        if not pid:
            raise HTTPException(status_code=400, detail="products[].product_id is required")
        try:
            qty_int = int(qty)
        except Exception:
            raise HTTPException(status_code=400, detail="products[].quantity must be an integer")
        if qty_int <= 0:
            raise HTTPException(status_code=400, detail="products[].quantity must be >= 1")
        dedup[pid] = qty_int

    product_ids = list(dedup.keys())
    if product_ids:
        found = db.query(Product.id, Product.brand).filter(Product.id.in_(product_ids)).all()
        found_map = {pid: brand for pid, brand in found}
        missing = [str(pid) for pid in product_ids if pid not in found_map]
        if missing:
            raise HTTPException(status_code=400, detail="Unknown product_id(s): " + ", ".join(missing))
        wrong_brand = [str(pid) for pid, b in found_map.items() if b != active_brand]
        if wrong_brand:
            raise HTTPException(status_code=400, detail="Product(s) not in active brand: " + ", ".join(wrong_brand))

    db.query(RewardProduct).filter(RewardProduct.reward_id == reward.id).delete(synchronize_session=False)
    for pid, qty in dedup.items():
        db.add(RewardProduct(reward_id=reward.id, product_id=pid, quantity=qty))


@router.get("", response_model=list[RewardOut])
def list_rewards(
    active_brand: str = Depends(get_active_brand),
    brand: str | None = None,
    active: bool | None = None,
    coupon_type_id: str | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(Reward)
    if brand and brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    q = q.filter(Reward.brand == active_brand)
    if active is not None:
        q = q.filter(Reward.active.is_(active))
    if coupon_type_id:
        from app.models.coupon_type_reward import CouponTypeReward

        q = q.join(CouponTypeReward, CouponTypeReward.reward_id == Reward.id).filter(
            CouponTypeReward.coupon_type_id == coupon_type_id
        )
    rewards = q.order_by(Reward.created_at.desc()).all()
    return [_serialize_reward_out(db=db, reward=r) for r in rewards]


@router.post("", response_model=RewardOut)
def create_reward(
    payload: RewardCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if payload.brand is not None and payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    reward = Reward(
        brand=active_brand,
        name=payload.name,
        description=payload.description,
        active=payload.active,
    )
    db.add(reward)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        code = _pgcode(e)
        if code == "23505":
            raise HTTPException(
                status_code=409,
                detail=(
                    "Une récompense avec des informations identiques existe déjà. "
                    "Veuillez modifier le nom (ou la description) puis réessayer."
                ),
            )
        raise HTTPException(
            status_code=409,
            detail=(
                "Impossible d'enregistrer la récompense (conflit de données). "
                "Veuillez vérifier les champs saisis et réessayer."
            ),
        )
    db.refresh(reward)

    link_reward_to_coupon_types(
        db,
        reward=reward,
        coupon_type_ids=payload.coupon_type_ids,
        brand=active_brand,
    )

    _replace_reward_products(
        db=db,
        reward=reward,
        active_brand=active_brand,
        items=(payload.model_dump().get("products") if payload.products is not None else None),
    )
    db.commit()
    db.refresh(reward)
    return _serialize_reward_out(db=db, reward=reward)


@router.get("/{reward_id}", response_model=RewardOut)
def get_reward(
    reward_id: str,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    reward = db.query(Reward).filter(Reward.id == reward_id).first()
    if not reward or reward.brand != active_brand:
        raise HTTPException(status_code=404, detail="Reward not found")
    return _serialize_reward_out(db=db, reward=reward)


@router.patch("/{reward_id}", response_model=RewardOut)
def update_reward(
    reward_id: str,
    payload: RewardUpdate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    reward = db.query(Reward).filter(Reward.id == reward_id).first()
    if not reward or reward.brand != active_brand:
        raise HTTPException(status_code=404, detail="Reward not found")

    data = payload.model_dump(exclude_unset=True)
    products_items = data.pop("products", None) if "products" in data else None
    coupon_type_ids = data.pop("coupon_type_ids", None)
    if "brand" in data and data["brand"] is not None and data["brand"] != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    for k, v in data.items():
        if k == "brand":
            continue
        setattr(reward, k, v)

    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        code = _pgcode(e)
        if code == "23505":
            raise HTTPException(
                status_code=409,
                detail=(
                    "Impossible de mettre à jour la récompense: une récompense identique existe déjà. "
                    "Veuillez modifier le nom (ou la description) puis réessayer."
                ),
            )
        raise HTTPException(
            status_code=409,
            detail=(
                "Impossible de mettre à jour la récompense (conflit de données). "
                "Veuillez vérifier les champs saisis et réessayer."
            ),
        )
    db.refresh(reward)

    if coupon_type_ids is not None:
        link_reward_to_coupon_types(
            db,
            reward=reward,
            coupon_type_ids=coupon_type_ids,
            brand=active_brand,
            replace=True,
        )

    _replace_reward_products(db=db, reward=reward, active_brand=active_brand, items=products_items)
    db.commit()
    db.refresh(reward)
    return _serialize_reward_out(db=db, reward=reward)


@router.get("/{reward_id}/delete-preview", response_model=CatalogDeletePreviewOut)
def preview_delete_reward(
    reward_id: str,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    reward = db.query(Reward).filter(Reward.id == reward_id).first()
    if not reward or reward.brand != active_brand:
        raise HTTPException(status_code=404, detail="Reward not found")
    data = preview_reward_delete(db, reward=reward)
    return CatalogDeletePreviewOut(**data)


@router.delete("/{reward_id}")
def delete_reward(
    reward_id: str,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    reward = db.query(Reward).filter(Reward.id == reward_id).first()
    if not reward or reward.brand != active_brand:
        raise HTTPException(status_code=404, detail="Reward not found")

    invalidation = apply_reward_catalog_delete(db, reward=reward)
    db.delete(reward)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        code = _pgcode(e)
        if code == "23503":
            raise HTTPException(
                status_code=409,
                detail=(
                    "Impossible de supprimer cette récompense car elle a déjà été attribuée à au moins un client "
                    "(ou est référencée par des coupons/récompenses clients). "
                    "Action recommandée: désactivez la récompense (active=false) au lieu de la supprimer."
                ),
            )
        raise HTTPException(
            status_code=409,
            detail=(
                "Impossible de supprimer cette récompense (conflit de données). "
                "Action recommandée: désactivez la récompense (active=false) au lieu de la supprimer."
            ),
        )
    return {
        "deleted": True,
        "invalidated_count": invalidation.get("rewards_invalidated", 0),
        **invalidation,
    }
