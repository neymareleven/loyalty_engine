import uuid

from sqlalchemy import Column, ForeignKey, Index, JSON, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class Redemption(Base):
    __tablename__ = "redemptions"

    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_redemptions_idempotency_key"),
        Index("ix_redemptions_brand_status", "brand", "status"),
        Index("ix_redemptions_coupon", "customer_coupon_id"),
        Index("ix_redemptions_reserved_until", "reserved_until"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    brand = Column(String(50), nullable=False)
    customer_id = Column(UUID(as_uuid=True), ForeignKey("customers.id"), nullable=False)
    customer_coupon_id = Column(
        UUID(as_uuid=True), ForeignKey("customer_coupons.id"), nullable=True
    )
    customer_reward_id = Column(
        UUID(as_uuid=True), ForeignKey("customer_rewards.id"), nullable=True
    )
    site_id = Column(String(100), nullable=True)
    cart_ref = Column(String(150), nullable=True)
    order_ref = Column(String(150), nullable=True)
    status = Column(String(20), nullable=False, default="RESERVED")
    benefit = Column(JSON, nullable=True)
    currency = Column(String(10), nullable=True)
    cart_snapshot = Column(JSON, nullable=True)
    idempotency_key = Column(String(255), nullable=True)
    reserved_until = Column(TIMESTAMP, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
