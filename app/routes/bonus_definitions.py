from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.bonus_definition import BonusDefinition
from app.schemas.bonus_definition import (
    BonusDefinitionCreate,
    BonusDefinitionOut,
    BonusDefinitionUpdate,
)


router = APIRouter(prefix="/admin/bonus-definitions", tags=["admin-bonus-definitions"])


@router.get("/ui-catalog")
def get_bonus_definitions_ui_catalog():

    def _model_json_schema(model_cls):
        fn = getattr(model_cls, "model_json_schema", None)
        if callable(fn):
            return fn()
        return model_cls.schema()

    return {
        "jsonSchema": _model_json_schema(BonusDefinitionCreate),
        "uiHints": {
            "bonus_key": {"widget": "text", "placeholder": "ex: BIRTHDAY_200"},
            "name": {"widget": "text"},
            "description": {"widget": "textarea"},
            "award_policy": {
                "widget": "select",
                "options": ["ONCE_EVER", "ONCE_PER_YEAR", "ONCE_PER_MONTH", "ONCE_PER_WEEK", "ONCE_PER_DAY"],
            },
            "policy_params": {"widget": "json_object"},
            "active": {"widget": "switch"},
        },
        "examples": [
            {
                "bonus_key": "BIRTHDAY_200",
                "name": "Birthday bonus",
                "award_policy": "ONCE_PER_YEAR",
                "policy_params": {"period": "year"},
                "active": True,
            }
        ],
    }


@router.get("", response_model=list[BonusDefinitionOut])
def list_bonus_definitions(
    active_brand: str = Depends(get_active_brand),
    brand: str | None = None,
    active: bool | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(BonusDefinition)
    if brand and brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    q = q.filter(BonusDefinition.brand == active_brand)
    if active is not None:
        q = q.filter(BonusDefinition.active.is_(active))
    return q.order_by(BonusDefinition.created_at.desc()).all()


@router.post("", response_model=BonusDefinitionOut)
def create_bonus_definition(
    payload: BonusDefinitionCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if payload.brand is not None and payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")
    existing = (
        db.query(BonusDefinition.id)
        .filter(BonusDefinition.bonus_key == payload.bonus_key)
        .first()
    )
    if existing:
        raise HTTPException(status_code=400, detail="bonus_key already exists")

    obj = BonusDefinition(
        bonus_key=payload.bonus_key,
        brand=active_brand,
        name=payload.name,
        description=payload.description,
        award_policy=payload.award_policy,
        policy_params=payload.policy_params,
        active=payload.active,
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/{bonus_definition_id}", response_model=BonusDefinitionOut)
def get_bonus_definition(
    bonus_definition_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(BonusDefinition).filter(BonusDefinition.id == bonus_definition_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Bonus definition not found")
    return obj


@router.patch("/{bonus_definition_id}", response_model=BonusDefinitionOut)
def update_bonus_definition(
    bonus_definition_id: UUID,
    payload: BonusDefinitionUpdate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(BonusDefinition).filter(BonusDefinition.id == bonus_definition_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Bonus definition not found")

    data = payload.model_dump(exclude_unset=True)
    if "brand" in data and data["brand"] is not None and data["brand"] != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")
    if "bonus_key" in data and data["bonus_key"] != obj.bonus_key:
        existing = (
            db.query(BonusDefinition.id)
            .filter(BonusDefinition.bonus_key == data["bonus_key"])
            .first()
        )
        if existing:
            raise HTTPException(status_code=400, detail="bonus_key already exists")

    for k, v in data.items():
        if k == "brand":
            continue
        setattr(obj, k, v)

    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{bonus_definition_id}")
def delete_bonus_definition(
    bonus_definition_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(BonusDefinition).filter(BonusDefinition.id == bonus_definition_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Bonus definition not found")

    db.delete(obj)
    db.commit()
    return {"deleted": True}
