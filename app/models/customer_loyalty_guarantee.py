import uuid

from sqlalchemy import Column, String, TIMESTAMP, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


GUARANTEE_BYPASS = "FIRST_PURCHASE_BYPASS"
GUARANTEE_ADMIN = "ADMIN_SET_TIER"
GUARANTEE_MODE_HOLD = "MODE_SWITCH_HOLD"


class CustomerLoyaltyGuarantee(Base):
    """Floor palier granted outside earned thresholds (bypass, admin, mode-switch hold)."""

    __tablename__ = "customer_loyalty_guarantees"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_id = Column(UUID(as_uuid=True), ForeignKey("customers.id", ondelete="CASCADE"), nullable=False)
    brand = Column(String(50), nullable=False)
    source = Column(String(50), nullable=False)
    floor_tier_key = Column(String(50), nullable=False)
    expires_at = Column(TIMESTAMP, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    revoked_at = Column(TIMESTAMP, nullable=True)
