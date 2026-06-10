from datetime import datetime
from typing import Any, Dict, Optional

from uuid import UUID

from pydantic import BaseModel, Field


class CustomerRewardOut(BaseModel):
    id: UUID
    customer_id: UUID
    reward_id: Optional[UUID] = None

    customer_coupon_id: Optional[UUID] = None

    status: str

    issued_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    used_at: Optional[datetime] = None

    source_transaction_id: Optional[UUID] = None

    rule_id: Optional[UUID] = None
    rule_execution_id: Optional[UUID] = None

    payload: Optional[Dict[str, Any]] = None

    reward_name: Optional[str] = None
    coupon_type_name: Optional[str] = None
    display_label: Optional[str] = None
    catalog_removed: Optional[bool] = None
    status_label: Optional[str] = None
    allowed_admin_transitions: list[str] = Field(default_factory=list)
    admin_actions_enabled: bool = True
    products: list[dict] = Field(default_factory=list)

    class Config:
        from_attributes = True


class RedeemCatalogRewardIn(BaseModel):
    reward_id: UUID
