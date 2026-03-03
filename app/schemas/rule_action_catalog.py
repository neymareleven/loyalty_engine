from typing import Literal, Optional

from pydantic import BaseModel, Field


class EarnPointsAction(BaseModel):
    type: Literal["earn_points"] = "earn_points"
    points: int


class BurnPointsAction(BaseModel):
    type: Literal["burn_points"] = "burn_points"
    points: int


class BurnStatusPointsAction(BaseModel):
    type: Literal["burn_status_points"] = "burn_status_points"
    points: int


class RedeemRewardAction(BaseModel):
    type: Literal["redeem_reward"] = "redeem_reward"
    reward_id: str


class IssueRewardAction(BaseModel):
    type: Literal["issue_reward"] = "issue_reward"
    reward_id: str


class RecordBonusAwardAction(BaseModel):
    type: Literal["record_bonus_award"] = "record_bonus_award"
    bonusKey: str


class GrantBonusAction(BaseModel):
    type: Literal["grant_bonus"] = "grant_bonus"
    bonusKey: str
    points: Optional[int] = None


class GrantBonusRewardAction(BaseModel):
    type: Literal["grant_bonus_reward"] = "grant_bonus_reward"
    bonusKey: str
    reward_id: Optional[str] = Field(
        default=None,
        description="Optional override. If omitted, the engine will use BonusDefinition.policy_params.reward_id.",
    )


class GrantBonusStatusAction(BaseModel):
    type: Literal["grant_bonus_status"] = "grant_bonus_status"
    bonusKey: str
    status: Optional[str] = Field(
        default=None,
        description="Optional override. If omitted, the engine will use BonusDefinition.policy_params.status.",
    )


class ResetStatusPointsAction(BaseModel):
    type: Literal["reset_status_points"] = "reset_status_points"


class DowngradeOneTierAction(BaseModel):
    type: Literal["downgrade_one_tier"] = "downgrade_one_tier"


class SetCustomerStatusAction(BaseModel):
    type: Literal["set_customer_status"] = "set_customer_status"
    status: str


class AddCustomerTagAction(BaseModel):
    type: Literal["add_customer_tag"] = "add_customer_tag"
    tag: str
