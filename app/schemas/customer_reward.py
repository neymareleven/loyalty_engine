from datetime import datetime
from typing import Optional

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

    class Config:
        from_attributes = True
