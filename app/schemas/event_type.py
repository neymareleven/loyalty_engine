from datetime import datetime
from typing import Any, Dict, Optional

from uuid import UUID

from pydantic import BaseModel, Field


class EventTypeCreate(BaseModel):
    brand: Optional[str] = None
    key: str
    origin: str

    name: str
    description: Optional[str] = None

    payload_schema: Optional[Dict[str, Any]] = None

    active: bool = True


class EventTypeUpdate(BaseModel):
    brand: Optional[str] = None
    key: Optional[str] = None
    origin: Optional[str] = None

    name: Optional[str] = None
    description: Optional[str] = None

    payload_schema: Optional[Dict[str, Any]] = None

    active: Optional[bool] = None


class EventTypeOut(BaseModel):
    id: UUID
    brand: Optional[str] = None
    key: str
    origin: str

    name: str
    description: Optional[str] = None

    payload_schema: Optional[Dict[str, Any]] = None

    active: bool

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
