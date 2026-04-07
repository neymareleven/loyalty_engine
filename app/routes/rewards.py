from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.reward import Reward
from app.models.reward_category import RewardCategory
from app.schemas.reward import RewardCreate, RewardUpdate, RewardOut


router = APIRouter(prefix="/rewards", tags=["rewards"])


def _pgcode(err: IntegrityError) -> str | None:
    orig = getattr(err, "orig", None)
    code = getattr(orig, "pgcode", None)
    if code:
        return str(code)
    return None


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
    return q.order_by(Reward.created_at.desc()).all()


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
            raise HTTPException(status_code=400, detail="reward_category_id not found")

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
    return reward


@router.get("/{reward_id}", response_model=RewardOut)
def get_reward(
    reward_id: str,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    reward = db.query(Reward).filter(Reward.id == reward_id).first()
    if not reward or reward.brand != active_brand:
        raise HTTPException(status_code=404, detail="Reward not found")
    return reward


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
    if "brand" in data and data["brand"] is not None and data["brand"] != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    if "reward_category_id" in data and data["reward_category_id"] is not None:
        cat = db.query(RewardCategory).filter(RewardCategory.id == data["reward_category_id"]).first()
        if not cat or cat.brand != active_brand:
            raise HTTPException(status_code=400, detail="reward_category_id not found")

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
    return reward


@router.delete("/{reward_id}")
def delete_reward(
    reward_id: str,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    reward = db.query(Reward).filter(Reward.id == reward_id).first()
    if not reward or reward.brand != active_brand:
        raise HTTPException(status_code=404, detail="Reward not found")

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
