from sqlalchemy import Column, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from app.db import Base


class CouponTypeReward(Base):
    """Explicit many-to-many: default rewards bundled when a coupon type is issued."""

    __tablename__ = "coupon_type_rewards"

    __table_args__ = (
        UniqueConstraint("coupon_type_id", "reward_id", name="uq_coupon_type_rewards_coupon_reward"),
    )

    coupon_type_id = Column(
        UUID(as_uuid=True),
        ForeignKey("coupon_types.id", ondelete="CASCADE"),
        primary_key=True,
    )
    reward_id = Column(
        UUID(as_uuid=True),
        ForeignKey("rewards.id", ondelete="CASCADE"),
        primary_key=True,
    )
