from datetime import date, datetime
from typing import Optional

from uuid import UUID

from pydantic import BaseModel


class CustomerCreate(BaseModel):
    gender: Optional[str] = None
    birthdate: Optional[date] = None


class CustomerUpsert(BaseModel):
    profileId: str

    gender: Optional[str] = None
    birthdate: Optional[date] = None


class CustomerOut(BaseModel):
    id: UUID
    brand: str
    profile_id: str

    gender: Optional[str] = None
    birthdate: Optional[date] = None
    
    status: str
    loyalty_status: Optional[str] = None
    lifetime_points: int

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
