from datetime import datetime
from typing import Any, Dict, Optional

from uuid import UUID

from pydantic import BaseModel, Field


class BonusDefinitionCreate(BaseModel):
    bonus_key: Optional[str] = None
    brand: Optional[str] = None

    name: str
    description: Optional[str] = None

    award_policy: str
    policy_params: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional. Policy parameters for award_policy. "
            "For the currently supported policies (ONCE_EVER/ONCE_PER_YEAR/ONCE_PER_MONTH/ONCE_PER_WEEK/ONCE_PER_DAY), "
            "this object is not used by the engine and can be omitted or set to {}. "
            "The frontend should render a guided form based on /admin/bonus-award-policies."
        ),
        examples=[{}, None],
    )

    active: bool = True


class BonusDefinitionUpdate(BaseModel):
    bonus_key: Optional[str] = None
    brand: Optional[str] = None

    name: Optional[str] = None
    description: Optional[str] = None

    award_policy: Optional[str] = None
    policy_params: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional. Same as on create. Usually omit or set to {} for current policies. "
            "See /admin/bonus-award-policies for the guided schema/examples."
        ),
        examples=[{}],
    )

    active: Optional[bool] = None


class BonusDefinitionOut(BaseModel):
    id: UUID
    bonus_key: str
    brand: Optional[str] = None

    name: str
    description: Optional[str] = None

    award_policy: str
    policy_params: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional policy parameters. For current policies this is usually {} or null. "
            "The authoritative catalog is /admin/bonus-award-policies."
        ),
        examples=[{}, None],
    )

    active: bool

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
