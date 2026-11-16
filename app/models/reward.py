import uuid

from sqlalchemy import Boolean, Column, Integer, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class Reward(Base):
    __tablename__ = "rewards"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    brand = Column(String(50), nullable=False)

    name = Column(String(100), nullable=False)
    description = Column(String(255))

    active = Column(Boolean, default=True)

    benefit_type = Column(String(30), nullable=False, default="CUSTOM")
    benefit_value = Column(Integer, nullable=True)
    max_discount_amount = Column(Integer, nullable=True)
    min_cart_amount = Column(Integer, nullable=True)
    applies_to = Column(String(20), nullable=False, default="CART")
    free_product_quantity = Column(Integer, nullable=False, default=1)
    stackable = Column(Boolean, nullable=False, default=False)

    created_at = Column(TIMESTAMP, server_default=func.now())
