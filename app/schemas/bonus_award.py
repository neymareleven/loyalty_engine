from datetime import datetime
from typing import Any, Dict, Optional

from uuid import UUID

from pydantic import BaseModel, Field


class BonusAwardOut(BaseModel):
    id: UUID

    bonus_key: str
    brand: str
    profile_id: str

    period_key: Optional[str] = None

    event_id: Optional[str] = None
    transaction_id: Optional[UUID] = None

    meta: Dict[str, Any] = Field(default_factory=dict, alias="meta")

    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True
