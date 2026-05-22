from datetime import datetime
from typing import Any, Dict, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class CustomerCouponStatusUpdate(BaseModel):
    status: Literal["ISSUED", "USED", "EXPIRED"] = Field(
        description="Statut cible du coupon client (et synchronisation des rewards liées).",
    )


class CustomerCouponOut(BaseModel):
    id: UUID
    customer_id: UUID
    coupon_type_id: UUID

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
