from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.reward import Reward
from app.schemas.reward import RewardCreate, RewardUpdate, RewardOut


router = APIRouter(prefix="/rewards", tags=["rewards"])


def _validate_reward_by_type(
    *,
    reward_type: str | None,
    currency: str | None,
    value_amount: int | None,
    value_percent: int | None,
    params,
):
    rt = (reward_type or "POINTS").strip().upper()

    if currency is not None and (not isinstance(currency, str) or len(currency.strip()) != 3):
        raise HTTPException(status_code=400, detail="currency must be a 3-letter ISO code")

    if value_amount is not None:
        try:
            if int(value_amount) < 0:
                raise HTTPException(status_code=400, detail="value_amount must be >= 0")
        except Exception:
            raise HTTPException(status_code=400, detail="value_amount must be an integer")

    if value_percent is not None:
        try:
            vp = int(value_percent)
        except Exception:
            raise HTTPException(status_code=400, detail="value_percent must be an integer")
        if vp <= 0 or vp > 100:
            raise HTTPException(status_code=400, detail="value_percent must be between 1 and 100")

    if rt == "DISCOUNT":
        has_percent = value_percent is not None
        has_amount = value_amount is not None
        if not has_percent and not has_amount:
            raise HTTPException(status_code=400, detail="DISCOUNT requires value_percent or value_amount")
        if has_amount and not currency:
            raise HTTPException(status_code=400, detail="DISCOUNT with value_amount requires currency")

    if rt == "CASHBACK":
        if value_amount is None:
            raise HTTPException(status_code=400, detail="CASHBACK requires value_amount")
        if not currency:
            raise HTTPException(status_code=400, detail="CASHBACK requires currency")

    if rt == "VOUCHER":
        if params is None:
            raise HTTPException(status_code=400, detail="VOUCHER requires params (can be empty object)")
        if not isinstance(params, dict):
            raise HTTPException(status_code=400, detail="params must be an object")


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

    _validate_reward_by_type(
        reward_type=payload.type,
        currency=payload.currency,
        value_amount=payload.value_amount,
        value_percent=payload.value_percent,
        params=payload.params,
    )

    reward = Reward(
        brand=active_brand,
        name=payload.name,
        description=payload.description,
        cost_points=payload.cost_points,
        type=payload.type,
        validity_days=payload.validity_days,
        currency=payload.currency,
        value_amount=payload.value_amount,
        value_percent=payload.value_percent,
        params=payload.params,
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

    _validate_reward_by_type(
        reward_type=reward.type,
        currency=reward.currency,
        value_amount=reward.value_amount,
        value_percent=reward.value_percent,
        params=reward.params,
    )

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
