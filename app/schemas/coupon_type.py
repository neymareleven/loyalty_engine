from datetime import datetime
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field

CouponTypeRecommendedAction = Literal["deactivate"]


class CouponTypeCreate(BaseModel):
    """Créer d'abord le type de coupon ; les rewards se rattachent ensuite via coupon_type_ids."""

    brand: Optional[str] = None
    name: str
    description: Optional[str] = None
    validity_days: Optional[int] = None
    active: bool = True


class CouponTypeUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    validity_days: Optional[int] = None
    reward_ids: Optional[list[UUID]] = None
    active: Optional[bool] = None


class CouponTypeRewardSummary(BaseModel):
    id: UUID
    name: str
    active: bool


class CouponTypeOut(BaseModel):
    id: UUID
    brand: str
    name: str
    description: Optional[str] = None
    validity_days: Optional[int] = None
    reward_ids: list[UUID] = Field(default_factory=list)
    rewards: list[CouponTypeRewardSummary] = Field(default_factory=list)
    active: bool
    customer_coupon_count: int = 0
    can_delete: bool = True
    recommended_action: Optional[CouponTypeRecommendedAction] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class CouponTypeRewardsReplace(BaseModel):
    reward_ids: list[UUID] = Field(default_factory=list)
