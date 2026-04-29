from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.customer_reward import CustomerReward
from app.models.product import Product
from app.models.reward import Reward
from app.models.reward_category import RewardCategory
from app.models.reward_product import RewardProduct
from app.schemas.reward import RewardCreate, RewardUpdate, RewardOut


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

    return {
        "id": reward.id,
        "brand": reward.brand,
        "reward_category_id": reward.reward_category_id,
        "name": reward.name,
        "description": reward.description,
        "active": reward.active,
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
    db: Session = Depends(get_db),
):
    q = db.query(Reward)
    if brand and brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    q = q.filter(Reward.brand == active_brand)
    if active is not None:
        q = q.filter(Reward.active.is_(active))
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

    reward_category_id = payload.reward_category_id
    if reward_category_id is not None:
        cat = db.query(RewardCategory).filter(RewardCategory.id == reward_category_id).first()
        if not cat or cat.brand != active_brand:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"reward_category_id not found: '{str(reward_category_id)}'. "
                    "Veuillez sélectionner une catégorie de récompense existante pour cette marque."
                ),
            )

    reward = Reward(
        brand=active_brand,
        reward_category_id=reward_category_id,
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
        if code == "23503":
            raise HTTPException(
                status_code=409,
                detail=(
                    "Impossible d'enregistrer la récompense car une référence est invalide (catégorie). "
                    "Veuillez sélectionner une catégorie valide pour cette marque."
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
    if "brand" in data and data["brand"] is not None and data["brand"] != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    if "reward_category_id" in data and data["reward_category_id"] is not None:
        cat = db.query(RewardCategory).filter(RewardCategory.id == data["reward_category_id"]).first()
        if not cat or cat.brand != active_brand:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"reward_category_id not found: '{str(data['reward_category_id'])}'. "
                    "Veuillez sélectionner une catégorie de récompense existante pour cette marque."
                ),
            )

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
        if code == "23503":
            raise HTTPException(
                status_code=409,
                detail=(
                    "Impossible de mettre à jour la récompense car la catégorie sélectionnée est invalide. "
                    "Veuillez sélectionner une catégorie valide pour cette marque."
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

    _replace_reward_products(db=db, reward=reward, active_brand=active_brand, items=products_items)
    db.commit()
    db.refresh(reward)
    return _serialize_reward_out(db=db, reward=reward)


@router.delete("/{reward_id}")
def delete_reward(
    reward_id: str,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    reward = db.query(Reward).filter(Reward.id == reward_id).first()
    if not reward or reward.brand != active_brand:
        raise HTTPException(status_code=404, detail="Reward not found")

    # Prevent hard delete when reward has already been issued to customers.
    in_use = (
        db.query(CustomerReward.id)
        .filter(CustomerReward.reward_id == reward.id)
        .first()
    )
    if in_use:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Impossible de supprimer la récompense '{reward.name}' ({str(reward.id)}) car elle est déjà attribuée à au moins un client. "
                "Action recommandée: désactivez la récompense (active=false) au lieu de la supprimer."
            ),
        )

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
    return {"deleted": True}
