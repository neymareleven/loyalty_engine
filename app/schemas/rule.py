from datetime import datetime
from typing import Any, Dict, Optional

from uuid import UUID

from pydantic import BaseModel


class RuleCreate(BaseModel):
    brand: Optional[str] = None
    name: str
    description: Optional[str] = None
    transaction_type: str
    priority: int = 0

    conditions: Optional[Dict[str, Any]] = None
    actions: Optional[list[Dict[str, Any]]] = None

    active: bool = True


class RuleUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    priority: Optional[int] = None

    conditions: Optional[Dict[str, Any]] = None
    actions: Optional[list[Dict[str, Any]]] = None

    active: Optional[bool] = None


class RuleOut(BaseModel):
    id: UUID
    brand: str
    name: str
    description: Optional[str] = None
    transaction_type: str
    priority: int

    conditions: Optional[Dict[str, Any]] = None
    actions: Optional[list[Dict[str, Any]]] = None

    active: bool
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True
