from datetime import datetime
from typing import Any, Dict, Optional

from uuid import UUID

from pydantic import BaseModel, model_validator


class RuleCreate(BaseModel):
    brand: Optional[str] = None
    name: str
    description: Optional[str] = None
    transaction_type: Optional[str] = None
    transaction_types: Optional[list[str]] = None
    priority: int = 0

    segment_ids: Optional[list[UUID]] = None

    conditions: Optional[Dict[str, Any]] = None
    actions: Optional[list[Dict[str, Any]]] = None

    active: bool = True
    valid_from: Optional[datetime] = None
    valid_until: Optional[datetime] = None

    @model_validator(mode="after")
    def _check_validity_window(self):
        if self.valid_from and self.valid_until and self.valid_from > self.valid_until:
            raise ValueError("valid_from must be before or equal to valid_until")
        return self


class RuleUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    priority: Optional[int] = None

    transaction_type: Optional[str] = None
    transaction_types: Optional[list[str]] = None

    segment_ids: Optional[list[UUID]] = None

    conditions: Optional[Dict[str, Any]] = None
    actions: Optional[list[Dict[str, Any]]] = None

    active: Optional[bool] = None
    valid_from: Optional[datetime] = None
    valid_until: Optional[datetime] = None


class RuleOut(BaseModel):
    id: UUID
    brand: str
    name: str
    description: Optional[str] = None
    transaction_type: str
    transaction_types: Optional[list[str]] = None
    priority: int

    segment_ids: Optional[list[UUID]] = None

    conditions: Optional[Dict[str, Any]] = None
    actions: Optional[list[Dict[str, Any]]] = None

    active: bool
    valid_from: Optional[datetime] = None
    valid_until: Optional[datetime] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class RuleReorderRequest(BaseModel):
    rule_ids: list[UUID]
