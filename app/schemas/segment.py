from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel


class SegmentCreate(BaseModel):
    brand: Optional[str] = None

    name: str
    description: Optional[str] = None

    is_dynamic: bool = True
    conditions: Optional[Dict[str, Any]] = None

    active: bool = True


class SegmentUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None

    is_dynamic: Optional[bool] = None
    conditions: Optional[Dict[str, Any]] = None

    active: Optional[bool] = None


class SegmentOut(BaseModel):
    id: UUID
    brand: str

    name: str
    description: Optional[str] = None

    is_dynamic: bool
    conditions: Optional[Dict[str, Any]] = None

    active: bool
    last_computed_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class SegmentMemberCreate(BaseModel):
    customer_id: UUID


class SegmentMemberOut(BaseModel):
    segment_id: UUID
    customer_id: UUID
    source: str
    computed_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True
