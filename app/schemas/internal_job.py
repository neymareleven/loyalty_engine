from datetime import datetime
from typing import Any, Dict, Optional

from uuid import UUID

from pydantic import BaseModel, Field


class InternalJobCreate(BaseModel):
    job_key: str
    brand: Optional[str] = None
    event_type: str

    selector: Dict[str, Any] = Field(default_factory=dict)
    payload_template: Optional[Dict[str, Any]] = None

    active: bool = True
    schedule: Optional[str] = None


class InternalJobUpdate(BaseModel):
    job_key: Optional[str] = None
    brand: Optional[str] = None
    event_type: Optional[str] = None

    selector: Optional[Dict[str, Any]] = None
    payload_template: Optional[Dict[str, Any]] = None

    active: Optional[bool] = None
    schedule: Optional[str] = None


class InternalJobOut(BaseModel):
    id: UUID
    job_key: str
    brand: Optional[str] = None
    event_type: str

    selector: Dict[str, Any] = Field(default_factory=dict)
    payload_template: Optional[Dict[str, Any]] = None

    active: bool
    schedule: Optional[str] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
