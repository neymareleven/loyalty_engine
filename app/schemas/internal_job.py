from datetime import datetime
from typing import Any, Dict, Literal, Optional

from uuid import UUID

from pydantic import BaseModel, Field


class InternalJobScheduleCron(BaseModel):
    type: Literal["cron"] = "cron"
    cron: str
    timezone: str = "UTC"


class InternalJobCreate(BaseModel):
    job_key: str
    brand: Optional[str] = None
    event_type: str

    selector: Dict[str, Any] = Field(default_factory=dict)
    payload_template: Optional[Dict[str, Any]] = None

    active: bool = True
    schedule: Optional[InternalJobScheduleCron] = None

    first_run_at: Optional[datetime] = None
    start_in_seconds: Optional[int] = None


class InternalJobUpdate(BaseModel):
    job_key: Optional[str] = None
    brand: Optional[str] = None
    event_type: Optional[str] = None

    selector: Optional[Dict[str, Any]] = None
    payload_template: Optional[Dict[str, Any]] = None

    active: Optional[bool] = None
    schedule: Optional[InternalJobScheduleCron] = None

    first_run_at: Optional[datetime] = None
    start_in_seconds: Optional[int] = None


class InternalJobOut(BaseModel):
    id: UUID
    job_key: str
    brand: Optional[str] = None
    event_type: str

    selector: Dict[str, Any] = Field(default_factory=dict)
    payload_template: Optional[Dict[str, Any]] = None

    active: bool
    schedule: Optional[Dict[str, Any]] = None

    next_run_at: Optional[datetime] = None
    last_run_at: Optional[datetime] = None

    locked_at: Optional[datetime] = None
    locked_by: Optional[str] = None

    last_status: Optional[str] = None
    last_error: Optional[str] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
