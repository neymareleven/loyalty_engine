from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.models.bonus_award import BonusAward
from app.schemas.bonus_award import BonusAwardOut


router = APIRouter(prefix="/admin/bonus-awards", tags=["admin-bonus-awards"])


@router.get("", response_model=list[BonusAwardOut])
def list_bonus_awards(
    brand: str | None = None,
    profileId: str | None = None,
    bonusKey: str | None = None,
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    q = db.query(BonusAward)
    if brand:
        q = q.filter(BonusAward.brand == brand)
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
def get_bonus_award(bonus_award_id: UUID, db: Session = Depends(get_db)):
    obj = db.query(BonusAward).filter(BonusAward.id == bonus_award_id).first()
    if not obj:
        raise HTTPException(status_code=404, detail="Bonus award not found")
    return obj
