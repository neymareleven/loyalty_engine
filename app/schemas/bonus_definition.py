from datetime import datetime
from typing import Any, Dict, Optional

from uuid import UUID

from pydantic import BaseModel, Field


class BonusDefinitionCreate(BaseModel):
    bonus_key: str
    brand: Optional[str] = None

    name: str
    description: Optional[str] = None

    award_policy: str
    policy_params: Optional[Dict[str, Any]] = None

    active: bool = True


class BonusDefinitionUpdate(BaseModel):
    bonus_key: Optional[str] = None
    brand: Optional[str] = None

    name: Optional[str] = None
    description: Optional[str] = None

    award_policy: Optional[str] = None
    policy_params: Optional[Dict[str, Any]] = None

    active: Optional[bool] = None


class BonusDefinitionOut(BaseModel):
    id: UUID
    bonus_key: str
    brand: Optional[str] = None

    name: str
    description: Optional[str] = None

    award_policy: str
    policy_params: Optional[Dict[str, Any]] = None

    active: bool

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
