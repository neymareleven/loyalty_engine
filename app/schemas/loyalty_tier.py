from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class LoyaltyTierCreate(BaseModel):
    brand: Optional[str] = None

    key: Optional[str] = None
    name: str

    min_status_points: int = Field(description="Seuil points (mode POINTS).")
    min_purchase_count: int = Field(0, description="Seuil nombre d'achats (mode PURCHASES).")
    min_purchase_amount: int = Field(0, description="Seuil montant total d'achats (mode PURCHASES).")
    rank: Optional[int] = None

    active: bool = True


class LoyaltyTierUpdate(BaseModel):
    key: Optional[str] = None
    name: Optional[str] = None

    min_status_points: Optional[int] = None
    min_purchase_count: Optional[int] = None
    min_purchase_amount: Optional[int] = None
    rank: Optional[int] = None

    active: Optional[bool] = None


class LoyaltyTierOut(BaseModel):
    id: UUID

    brand: str
    key: str
    name: str

    min_status_points: int
    min_purchase_count: int = 0
    min_purchase_amount: int = 0
    rank: int

    active: bool

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
