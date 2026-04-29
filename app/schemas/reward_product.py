from uuid import UUID

from pydantic import BaseModel


class RewardProductLinkCreate(BaseModel):
    product_id: UUID
    quantity: int = 1


class RewardProductLinkOut(BaseModel):
    reward_id: UUID
    product_id: UUID
    quantity: int

    class Config:
        from_attributes = True
