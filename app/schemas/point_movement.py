from datetime import date, datetime
from typing import Optional

from uuid import UUID

from pydantic import BaseModel


class PointMovementOut(BaseModel):
    id: UUID
    customer_id: UUID

    points: int
    type: str

    source_transaction_id: Optional[UUID] = None

    created_at: Optional[datetime] = None
    expires_at: Optional[date] = None
    isExpired: bool = False
    status: str = "active"

    class Config:
        from_attributes = True
