from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class RewardCategoryCreate(BaseModel):
    brand: Optional[str] = None
    coupon_type_id: UUID
    name: str
    description: Optional[str] = None
    active: bool = True


class RewardCategoryUpdate(BaseModel):
    coupon_type_id: Optional[UUID] = None
    name: Optional[str] = None
    description: Optional[str] = None
    active: Optional[bool] = None


class RewardCategoryOut(BaseModel):
    id: UUID
    brand: str
    coupon_type_id: UUID
    name: str
    description: Optional[str] = None
    active: bool
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
