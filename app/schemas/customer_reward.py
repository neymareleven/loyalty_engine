from datetime import datetime
from typing import Any, Dict, Optional

from uuid import UUID

from pydantic import BaseModel


class CustomerRewardOut(BaseModel):
    id: UUID
    customer_id: UUID
    reward_id: UUID

    status: str

    issued_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    used_at: Optional[datetime] = None

    source_transaction_id: Optional[UUID] = None

    rule_id: Optional[UUID] = None
    rule_execution_id: Optional[UUID] = None

    payload: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True


class RedeemCatalogRewardIn(BaseModel):
    reward_id: UUID
