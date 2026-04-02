from datetime import date, datetime
from typing import Optional

from uuid import UUID

from pydantic import BaseModel

from typing import Any, Dict


class CustomerCreate(BaseModel):
    gender: Optional[str] = None
    birthdate: Optional[str] = None


class CustomerSetTierMinOnly(BaseModel):
    tierKey: str


class CustomerUpsert(BaseModel):
    brand: Optional[str] = None
    profileId: str

    properties: Optional[Dict[str, Any]] = None

    gender: Optional[str] = None
    birthdate: Optional[str] = None


class CustomerOut(BaseModel):
    id: UUID
    brand: str
    profile_id: str

    gender: Optional[str] = None
    birthdate: Optional[date | str] = None
    
    status: str
    loyalty_status: Optional[str] = None
    loyalty_status_name: Optional[str] = None

    status_points: int
    status_points_reset_at: Optional[datetime] = None
    points_balance: Optional[int] = None

    points_expires_at: Optional[datetime] = None
    loyalty_status_assigned_at: Optional[datetime] = None
    loyalty_status_expires_at: Optional[datetime] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
