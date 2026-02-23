from datetime import datetime
from typing import Optional

from uuid import UUID

from pydantic import BaseModel


class LoyaltyTierCreate(BaseModel):
    brand: Optional[str] = None

    key: str
    name: str

    min_status_points: int
    rank: int

    active: bool = True


class LoyaltyTierUpdate(BaseModel):
    key: Optional[str] = None
    name: Optional[str] = None

    min_status_points: Optional[int] = None
    rank: Optional[int] = None

    active: Optional[bool] = None


class LoyaltyTierOut(BaseModel):
    id: UUID

    brand: str
    key: str
    name: str

    min_status_points: int
    rank: int

    active: bool

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
