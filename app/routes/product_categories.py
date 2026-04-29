from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.product_category import ProductCategory
from app.models.product import Product
from app.schemas.product_category import ProductCategoryCreate, ProductCategoryOut, ProductCategoryUpdate


router = APIRouter(prefix="/admin/product-categories", tags=["admin-product-categories"])


def _pgcode(err: IntegrityError) -> str | None:
    orig = getattr(err, "orig", None)
    code = getattr(orig, "pgcode", None)
    if code:
        return str(code)
    return None


@router.get("", response_model=list[ProductCategoryOut])
def list_product_categories(
    active_brand: str = Depends(get_active_brand),
    brand: str | None = None,
    active: bool | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(ProductCategory)
    if brand is not None and brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    q = q.filter(ProductCategory.brand == active_brand)
    if active is not None:
        q = q.filter(ProductCategory.active.is_(active))
    return q.order_by(ProductCategory.created_at.desc()).all()


@router.post("", response_model=ProductCategoryOut)
def create_product_category(
    payload: ProductCategoryCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if payload.brand is not None and payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    obj = ProductCategory(
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
        raise HTTPException(
            status_code=400,
            detail=(
                "Product category could not be saved. "
                "Causes possibles: une catégorie existe déjà avec ce nom pour cette marque, ou des données invalides."
            ),
        )
    db.refresh(obj)
    return obj


@router.get("/{product_category_id}", response_model=ProductCategoryOut)
def get_product_category(
    product_category_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(ProductCategory).filter(ProductCategory.id == product_category_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Product category not found")
    return obj


@router.patch("/{product_category_id}", response_model=ProductCategoryOut)
def update_product_category(
    product_category_id: UUID,
    payload: ProductCategoryUpdate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(ProductCategory).filter(ProductCategory.id == product_category_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Product category not found")

    data = payload.model_dump(exclude_unset=True)
    for k, v in data.items():
        setattr(obj, k, v)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=400,
            detail=(
                "Product category could not be saved. "
                "Causes possibles: une catégorie existe déjà avec ce nom pour cette marque, ou des données invalides."
            ),
        )
    db.refresh(obj)
    return obj


@router.delete("/{product_category_id}")
def delete_product_category(
    product_category_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(ProductCategory).filter(ProductCategory.id == product_category_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Product category not found")

    linked_products = (
        db.query(Product.id, Product.name)
        .filter(Product.brand == active_brand)
        .filter(Product.category_id == obj.id)
        .order_by(Product.created_at.asc())
        .limit(10)
        .all()
    )
    if linked_products:
        linked_label = ", ".join([f"{str(pid)} ({pname})" for pid, pname in linked_products])
        raise HTTPException(
            status_code=409,
            detail=(
                f"Impossible de supprimer cette catégorie car elle est liée à un ou plusieurs produits: {linked_label}. "
                "Action requise: supprimez ces produits ou réaffectez-les à une autre catégorie, puis réessayez."
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
