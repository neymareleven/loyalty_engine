from typing import Any, Literal, Optional

from pydantic import BaseModel


class EarnPointsAction(BaseModel):
    type: Literal["earn_points"] = "earn_points"
    points: int | dict[str, Any]
    multiplier: Optional[int] = None


class IssueCouponAction(BaseModel):
    type: Literal["issue_coupon"] = "issue_coupon"
    coupon_type_id: str
    frequency: Literal["ALWAYS", "ONCE_PER_CALENDAR_YEAR", "ONCE_PER_CUSTOMER"] = "ONCE_PER_CALENDAR_YEAR"


class ResetStatusPointsAction(BaseModel):
    type: Literal["reset_status_points"] = "reset_status_points"
