from typing import Literal, Optional

from pydantic import BaseModel


class EarnPointsAction(BaseModel):
    type: Literal["earn_points"] = "earn_points"
    points: int
    multiplier: Optional[int] = None


class BurnPointsAction(BaseModel):
    type: Literal["burn_points"] = "burn_points"
    points: int


class IssueRewardAction(BaseModel):
    type: Literal["issue_reward"] = "issue_reward"
    reward_id: str


class SetRankAction(BaseModel):
    type: Literal["set_rank"] = "set_rank"
    tier_key: str


class ResetStatusPointsAction(BaseModel):
    type: Literal["reset_status_points"] = "reset_status_points"
