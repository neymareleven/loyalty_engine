from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class ProductCategoryCreate(BaseModel):
    brand: Optional[str] = None
    name: str
    description: Optional[str] = None
    active: bool = True


class ProductCategoryUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    active: Optional[bool] = None


class ProductCategoryOut(BaseModel):
    id: UUID
    brand: str
    name: str
    description: Optional[str] = None
    active: bool
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True
