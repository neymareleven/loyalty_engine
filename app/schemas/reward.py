from datetime import datetime
from typing import Optional

from uuid import UUID

from pydantic import BaseModel


class RewardProductItem(BaseModel):
    product_id: UUID
    quantity: int = 1


class RewardProductItemOut(BaseModel):
    product_id: UUID
    quantity: int
    name: Optional[str] = None
    match_key: Optional[str] = None
    points_value: Optional[int] = None


class RewardCreate(BaseModel):
    brand: Optional[str] = None
    reward_category_id: Optional[UUID] = None
    name: str
    description: Optional[str] = None
    active: bool = True
    products: Optional[list[RewardProductItem]] = None


class RewardUpdate(BaseModel):
    reward_category_id: Optional[UUID] = None
    name: Optional[str] = None
    description: Optional[str] = None
    active: Optional[bool] = None
    products: Optional[list[RewardProductItem]] = None


class RewardOut(BaseModel):
    id: UUID
    brand: str
    reward_category_id: Optional[UUID] = None
    name: str
    description: Optional[str] = None
    active: bool
    created_at: Optional[datetime] = None
    products: list[RewardProductItemOut] = []

    class Config:
        from_attributes = True
