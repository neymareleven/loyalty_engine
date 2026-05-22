from datetime import datetime
from typing import Optional

from uuid import UUID

from pydantic import BaseModel, Field


class RewardProductItem(BaseModel):
    product_id: UUID
    quantity: int = 1


class RewardProductItemOut(BaseModel):
    product_id: UUID
    quantity: int
    name: Optional[str] = None
    match_key: Optional[str] = None
    points_value: Optional[int] = None


class CouponTypeLinkSummary(BaseModel):
    id: UUID
    name: str
    active: bool


class RewardCreate(BaseModel):
    brand: Optional[str] = None
    name: str
    description: Optional[str] = None
    active: bool = True
    products: Optional[list[RewardProductItem]] = None
    coupon_type_ids: list[UUID] = Field(
        ...,
        min_length=1,
        description="Types de coupon existants auxquels rattacher cette récompense.",
    )


class RewardUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    active: Optional[bool] = None
    products: Optional[list[RewardProductItem]] = None
    coupon_type_ids: Optional[list[UUID]] = None


class RewardOut(BaseModel):
    id: UUID
    brand: str
    name: str
    description: Optional[str] = None
    active: bool
    coupon_type_ids: list[UUID] = Field(default_factory=list)
    coupon_types: list[CouponTypeLinkSummary] = Field(default_factory=list)
    created_at: Optional[datetime] = None
    products: list[RewardProductItemOut] = []

    class Config:
        from_attributes = True
