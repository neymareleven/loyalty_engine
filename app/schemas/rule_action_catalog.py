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
