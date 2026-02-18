from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.models.event_type import EventType
from app.models.rule import Rule
from app.schemas.rule import RuleCreate, RuleUpdate, RuleOut


router = APIRouter(prefix="/rules", tags=["rules"])


@router.get("", response_model=list[RuleOut])
def list_rules(brand: str | None = None, event_type: str | None = None, db: Session = Depends(get_db)):
    q = db.query(Rule)
    if brand:
        q = q.filter(Rule.brand == brand)
    if event_type:
        q = q.filter(Rule.event_type == event_type)
    return q.order_by(Rule.brand.asc(), Rule.event_type.asc(), Rule.priority.asc()).all()


@router.post("", response_model=RuleOut)
def create_rule(payload: RuleCreate, db: Session = Depends(get_db)):
    exists = (
        db.query(EventType.id)
        .filter(EventType.key == payload.event_type)
        .filter(EventType.active.is_(True))
        .filter(EventType.brand == payload.brand)
        .first()
    )
    if not exists:
        raise HTTPException(status_code=400, detail="Unknown or inactive event_type. Create it in /admin/event-types first.")

    rule = Rule(
        brand=payload.brand,
        event_type=payload.event_type,
        priority=payload.priority,
        conditions=payload.conditions,
        actions=payload.actions,
        active=payload.active,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return rule


@router.get("/{rule_id}", response_model=RuleOut)
def get_rule(rule_id: UUID, db: Session = Depends(get_db)):
    rule = db.query(Rule).filter(Rule.id == rule_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    return rule


@router.patch("/{rule_id}", response_model=RuleOut)
def update_rule(rule_id: UUID, payload: RuleUpdate, db: Session = Depends(get_db)):
    rule = db.query(Rule).filter(Rule.id == rule_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    data = payload.model_dump(exclude_unset=True)
    for k, v in data.items():
        setattr(rule, k, v)

    db.commit()
    db.refresh(rule)
    return rule


@router.delete("/{rule_id}")
def delete_rule(rule_id: UUID, db: Session = Depends(get_db)):
    rule = db.query(Rule).filter(Rule.id == rule_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    db.delete(rule)
    db.commit()
    return {"deleted": True}
