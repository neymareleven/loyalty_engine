from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel


class CustomerCouponOut(BaseModel):
    id: UUID
    customer_id: UUID
    coupon_type_id: UUID

    calendar_year: int

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
