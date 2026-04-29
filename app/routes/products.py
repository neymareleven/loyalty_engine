from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.product import Product
from app.models.product_category import ProductCategory
from app.models.reward import Reward
from app.models.reward_product import RewardProduct
from app.schemas.product import ProductCreate, ProductOut, ProductUpdate


router = APIRouter(prefix="/admin/products", tags=["admin-products"])


def _pgcode(err: IntegrityError) -> str | None:
    orig = getattr(err, "orig", None)
    code = getattr(orig, "pgcode", None)
    if code:
        return str(code)
    return None


@router.get("/by-reward/{reward_id}")
def list_products_by_reward(
    reward_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    reward = db.query(Reward).filter(Reward.id == reward_id).first()
    if not reward or reward.brand != active_brand:
        raise HTTPException(status_code=404, detail="Reward not found")

    links = db.query(RewardProduct).filter(RewardProduct.reward_id == reward_id).all()
    if not links:
        return {"reward_id": str(reward_id), "items": []}

    product_ids = [l.product_id for l in links]
    products = db.query(Product).filter(Product.id.in_(product_ids)).all()
    prod_map = {p.id: p for p in products}

    items = []
    for l in links:
        p = prod_map.get(l.product_id)
        if not p:
            continue
        items.append(
            {
                "product_id": str(p.id),
                "name": p.name,
                "match_key": p.match_key,
                "points_value": p.points_value,
                "active": p.active,
                "category_id": str(p.category_id) if p.category_id else None,
                "quantity": l.quantity,
            }
        )

    items.sort(key=lambda x: (x.get("name") or "").lower())
    return {"reward_id": str(reward_id), "items": items}


@router.get("", response_model=list[ProductOut])
def list_products(
    active_brand: str = Depends(get_active_brand),
    brand: str | None = None,
    active: bool | None = None,
    category_id: UUID | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(Product)
    if brand is not None and brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    q = q.filter(Product.brand == active_brand)
    if active is not None:
        q = q.filter(Product.active.is_(active))
    if category_id is not None:
        q = q.filter(Product.category_id == category_id)
    return q.order_by(Product.created_at.desc()).all()


@router.post("", response_model=ProductOut)
def create_product(
    payload: ProductCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if payload.brand is not None and payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    if payload.points_value is not None and payload.points_value < 0:
        raise HTTPException(status_code=400, detail="points_value must be >= 0")

    if payload.category_id is not None:
        cat = db.query(ProductCategory).filter(ProductCategory.id == payload.category_id).first()
        if not cat or cat.brand != active_brand:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"category_id not found: '{str(payload.category_id)}'. "
                    "Veuillez sélectionner une catégorie de produits existante pour cette marque."
                ),
            )

    obj = Product(
        brand=active_brand,
        category_id=payload.category_id,
        name=payload.name,
        match_key=payload.match_key,
        points_value=payload.points_value,
        active=payload.active,
    )
    db.add(obj)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        code = _pgcode(e)
        if code == "23505":
            raise HTTPException(
                status_code=409,
                detail=(
                    "Un produit avec ce match_key existe déjà pour cette marque. "
                    "Veuillez modifier le match_key puis réessayer."
                ),
            )
        raise HTTPException(
            status_code=409,
            detail=(
                "Impossible d'enregistrer le produit (conflit de données). "
                "Veuillez vérifier les champs saisis et réessayer."
            ),
        )

    db.refresh(obj)
    return obj


@router.get("/{product_id}", response_model=ProductOut)
def get_product(
    product_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(Product).filter(Product.id == product_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Product not found")
    return obj


@router.patch("/{product_id}", response_model=ProductOut)
def update_product(
    product_id: UUID,
    payload: ProductUpdate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(Product).filter(Product.id == product_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Product not found")

    data = payload.model_dump(exclude_unset=True)

    if "points_value" in data and data["points_value"] is not None and data["points_value"] < 0:
        raise HTTPException(status_code=400, detail="points_value must be >= 0")

    if "category_id" in data:
        if data["category_id"] is not None:
            cat = db.query(ProductCategory).filter(ProductCategory.id == data["category_id"]).first()
            if not cat or cat.brand != active_brand:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"category_id not found: '{str(data['category_id'])}'. "
                        "Veuillez sélectionner une catégorie de produits existante pour cette marque."
                    ),
                )

    for k, v in data.items():
        setattr(obj, k, v)

    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        code = _pgcode(e)
        if code == "23505":
            raise HTTPException(
                status_code=409,
                detail=(
                    "Impossible de mettre à jour le produit: un produit avec ce match_key existe déjà pour cette marque. "
                    "Veuillez modifier le match_key puis réessayer."
                ),
            )
        raise HTTPException(
            status_code=409,
            detail="Impossible de mettre à jour le produit (conflit de données).",
        )

    db.refresh(obj)
    return obj


@router.delete("/{product_id}")
def delete_product(
    product_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(Product).filter(Product.id == product_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Product not found")

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
                    "Impossible de supprimer ce produit car il est encore référencé par d'autres données (ex: récompenses). "
                    "Supprimez d'abord les dépendances ou détachez-le."
                ),
            )
        raise HTTPException(
            status_code=409,
            detail="Impossible de supprimer ce produit (conflit de données).",
        )

    return {"deleted": True}
