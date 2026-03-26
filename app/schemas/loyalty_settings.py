from typing import Optional

from pydantic import BaseModel


class LoyaltySettingsOut(BaseModel):
    brand: str
    points_validity_days: Optional[int] = None
    loyalty_status_validity_days: Optional[int] = None


class LoyaltySettingsUpdate(BaseModel):
    points_validity_days: Optional[int] = None
    loyalty_status_validity_days: Optional[int] = None
