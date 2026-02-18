from datetime import datetime
from typing import Any, Dict, Optional

from uuid import UUID

from pydantic import BaseModel


class CampaignCreate(BaseModel):
    brand: str
    name: str
    event_type: str
    bonus_points: int

    conditions: Optional[Dict[str, Any]] = None
    active: bool = True

    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


class CampaignUpdate(BaseModel):
    name: Optional[str] = None
    event_type: Optional[str] = None
    bonus_points: Optional[int] = None

    conditions: Optional[Dict[str, Any]] = None
    active: Optional[bool] = None

    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


class CampaignOut(BaseModel):
    id: UUID
    brand: str
    name: str
    event_type: str
    bonus_points: int

    conditions: Optional[Dict[str, Any]] = None
    active: bool

    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True
