from datetime import datetime
from typing import Any, Dict, Optional

from uuid import UUID

from pydantic import BaseModel


class RuleCreate(BaseModel):
    brand: str
    event_type: str
    priority: int = 0

    conditions: Optional[Dict[str, Any]] = None
    actions: Optional[list[Dict[str, Any]]] = None

    active: bool = True


class RuleUpdate(BaseModel):
    priority: Optional[int] = None

    conditions: Optional[Dict[str, Any]] = None
    actions: Optional[list[Dict[str, Any]]] = None

    active: Optional[bool] = None


class RuleOut(BaseModel):
    id: UUID
    brand: str
    event_type: str
    priority: int

    conditions: Optional[Dict[str, Any]] = None
    actions: Optional[list[Dict[str, Any]]] = None

    active: bool
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True
