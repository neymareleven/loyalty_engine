from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class ProductCreate(BaseModel):
    brand: Optional[str] = None

    category_id: Optional[UUID] = None

    name: str
    match_key: str

    points_value: Optional[int] = None

    active: bool = True


class ProductUpdate(BaseModel):
    category_id: Optional[UUID] = None

    name: Optional[str] = None
    match_key: Optional[str] = None

    points_value: Optional[int] = None

    active: Optional[bool] = None


class ProductOut(BaseModel):
    id: UUID
    brand: str

    category_id: Optional[UUID] = None

    name: str
    match_key: str

    points_value: Optional[int] = None

    active: bool
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True
