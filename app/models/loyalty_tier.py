import uuid

from sqlalchemy import Boolean, Column, Integer, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class LoyaltyTier(Base):
    __tablename__ = "loyalty_tiers"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    brand = Column(String(50), nullable=False)

    key = Column(String(50), nullable=False)
    name = Column(String(200), nullable=False)

    min_status_points = Column(Integer, nullable=False)
    min_purchase_count = Column(Integer, nullable=False, default=0)
    min_purchase_amount = Column(Integer, nullable=False, default=0)
    rank = Column(Integer, nullable=False)

    active = Column(Boolean, default=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
