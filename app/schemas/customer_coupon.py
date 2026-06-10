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
    coupon_type_id: Optional[UUID] = None

    status: str

    issued_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    used_at: Optional[datetime] = None

    source_transaction_id: Optional[UUID] = None

    rule_id: Optional[UUID] = None
    rule_execution_id: Optional[UUID] = None

    payload: Optional[Dict[str, Any]] = None

    coupon_type_name: Optional[str] = None
    coupon_type_description: Optional[str] = None
    """Libellé principal pour l'UI (nom du type de coupon)."""
    display_label: Optional[str] = None
    catalog_removed: Optional[bool] = None
    status_label: Optional[str] = None
    allowed_admin_transitions: list[str] = Field(default_factory=list)
    admin_actions_enabled: bool = True

    class Config:
        from_attributes = True
