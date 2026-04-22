import uuid

from sqlalchemy import Column, Integer, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class BrandLoyaltySettings(Base):
    __tablename__ = "brand_loyalty_settings"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    brand = Column(String(50), nullable=False, unique=True)

    points_validity_days = Column(Integer, nullable=True)
    loyalty_status_validity_days = Column(Integer, nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
