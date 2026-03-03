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
            "Optional. Parameters for bonus behavior. "
            "Award frequency is controlled by award_policy (see /admin/bonus-award-policies). "
            "Additionally, when a rule uses the action grant_bonus, the engine will use policy_params.points "
            "as the default number of points if the action does not provide an explicit points override."
        ),
        examples=[{"points": 200}, {}, None],
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
            "Optional. Same as on create. "
            "Award frequency is controlled by award_policy (see /admin/bonus-award-policies). "
            "When using the grant_bonus action, policy_params.points can define the default number of points "
            "if the action does not provide points."
        ),
        examples=[{"points": 200}, {}],
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
            "Optional parameters for bonus behavior. "
            "award_policy controls frequency (see /admin/bonus-award-policies). "
            "policy_params.points can be used as the default points value for the grant_bonus rule action."
        ),
        examples=[{"points": 200}, {}, None],
    )

    active: bool

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
