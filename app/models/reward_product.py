from sqlalchemy import Column, ForeignKey, Integer, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from app.db import Base


class RewardProduct(Base):
    __tablename__ = "reward_products"

    __table_args__ = (UniqueConstraint("reward_id", "product_id", name="uq_reward_products_reward_product"),)

    reward_id = Column(UUID(as_uuid=True), ForeignKey("rewards.id", ondelete="CASCADE"), primary_key=True)
    product_id = Column(UUID(as_uuid=True), ForeignKey("products.id", ondelete="RESTRICT"), primary_key=True)

    quantity = Column(Integer, nullable=False, default=1)
