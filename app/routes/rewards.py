from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.reward import Reward
from app.schemas.reward import RewardCreate, RewardUpdate, RewardOut


router = APIRouter(prefix="/rewards", tags=["rewards"])


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
    reward = Reward(
        brand=active_brand,
        name=payload.name,
        description=payload.description,
        cost_points=payload.cost_points,
        type=payload.type,
        validity_days=payload.validity_days,
        active=payload.active,
    )
    db.add(reward)
    db.commit()
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
    for k, v in data.items():
        if k == "brand":
            continue
        setattr(reward, k, v)

    db.commit()
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
    db.commit()
    return {"deleted": True}
