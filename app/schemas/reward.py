from datetime import datetime
from typing import Optional

from uuid import UUID

from pydantic import BaseModel


class RewardCreate(BaseModel):
    brand: str
    name: str
    description: Optional[str] = None
    cost_points: Optional[int] = None
    type: str = "POINTS"
    validity_days: Optional[int] = None
    active: bool = True


class RewardUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    cost_points: Optional[int] = None
    type: Optional[str] = None
    validity_days: Optional[int] = None
    active: Optional[bool] = None


class RewardOut(BaseModel):
    id: UUID
    brand: str
    name: str
    description: Optional[str] = None
    cost_points: Optional[int] = None
    type: str
    validity_days: Optional[int] = None
    active: bool
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True
