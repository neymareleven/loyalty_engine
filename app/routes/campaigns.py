from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.campaign import Campaign
from app.schemas.campaign import CampaignCreate, CampaignUpdate, CampaignOut


router = APIRouter(prefix="/campaigns", tags=["campaigns"])


@router.get("", response_model=list[CampaignOut])
def list_campaigns(
    active_brand: str = Depends(get_active_brand),
    brand: str | None = None,
    event_type: str | None = None,
    active: bool | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(Campaign)
    if brand and brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    q = q.filter(Campaign.brand == active_brand)
    if event_type:
        q = q.filter(Campaign.event_type == event_type)
    if active is not None:
        q = q.filter(Campaign.active.is_(active))
    return q.order_by(Campaign.created_at.desc()).all()


@router.post("", response_model=CampaignOut)
def create_campaign(
    payload: CampaignCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")
    campaign = Campaign(
        brand=active_brand,
        name=payload.name,
        event_type=payload.event_type,
        bonus_points=payload.bonus_points,
        conditions=payload.conditions,
        active=payload.active,
        start_date=payload.start_date,
        end_date=payload.end_date,
    )
    db.add(campaign)
    db.commit()
    db.refresh(campaign)
    return campaign


@router.get("/{campaign_id}", response_model=CampaignOut)
def get_campaign(
    campaign_id: str,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    campaign = (
        db.query(Campaign)
        .filter(Campaign.id == campaign_id)
        .filter(Campaign.brand == active_brand)
        .first()
    )
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return campaign


@router.patch("/{campaign_id}", response_model=CampaignOut)
def update_campaign(
    campaign_id: str,
    payload: CampaignUpdate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    campaign = (
        db.query(Campaign)
        .filter(Campaign.id == campaign_id)
        .filter(Campaign.brand == active_brand)
        .first()
    )
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    data = payload.model_dump(exclude_unset=True)
    if "brand" in data and data["brand"] is not None and data["brand"] != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")
    for k, v in data.items():
        if k == "brand":
            continue
        setattr(campaign, k, v)

    db.commit()
    db.refresh(campaign)
    return campaign


@router.delete("/{campaign_id}")
def delete_campaign(
    campaign_id: str,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    campaign = (
        db.query(Campaign)
        .filter(Campaign.id == campaign_id)
        .filter(Campaign.brand == active_brand)
        .first()
    )
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    db.delete(campaign)
    db.commit()
    return {"deleted": True}
