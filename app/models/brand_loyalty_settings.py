import uuid

from sqlalchemy import Boolean, Column, Integer, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class BrandLoyaltySettings(Base):
    __tablename__ = "brand_loyalty_settings"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    brand = Column(String(50), nullable=False, unique=True)

    # POINTS = status_points thresholds; PURCHASES = purchase_count + purchase_amount thresholds.
    qualification_mode = Column(String(20), nullable=False, default="POINTS")

    points_validity_days = Column(Integer, nullable=True)
    loyalty_status_validity_days = Column(Integer, nullable=True)
    purchase_validity_days = Column(Integer, nullable=True)

    # First sale of at least min_amount grants the configured palier (if enabled).
    first_purchase_bypass_enabled = Column(Boolean, nullable=False, default=False)
    first_purchase_bypass_min_amount = Column(Integer, nullable=True)
    first_purchase_bypass_tier_key = Column(String(50), nullable=True)

    # Legacy columns (unused): Unomi is configured via .env — see UNOMI_* variables.
    segmentation_mode = Column(String(20), nullable=False, default="INTERNAL")
    unomi_base_url = Column(String(500), nullable=True)
    unomi_username = Column(String(255), nullable=True)
    unomi_password = Column(String(255), nullable=True)
    unomi_scope = Column(String(100), nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
