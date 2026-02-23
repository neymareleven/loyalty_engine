from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.bonus_award import BonusAward
from app.schemas.bonus_award import BonusAwardOut


router = APIRouter(prefix="/admin/bonus-awards", tags=["admin-bonus-awards"])


@router.get("", response_model=list[BonusAwardOut])
def list_bonus_awards(
    active_brand: str = Depends(get_active_brand),
    brand: str | None = None,
    profileId: str | None = None,
    bonusKey: str | None = None,
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    q = db.query(BonusAward)
    if brand and brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    q = q.filter(BonusAward.brand == active_brand)
    if profileId:
        q = q.filter(BonusAward.profile_id == profileId)
    if bonusKey:
        q = q.filter(BonusAward.bonus_key == bonusKey)

    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    return (
        q.order_by(BonusAward.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )


@router.get("/{bonus_award_id}", response_model=BonusAwardOut)
def get_bonus_award(
    bonus_award_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(BonusAward).filter(BonusAward.id == bonus_award_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Bonus award not found")
    return obj
