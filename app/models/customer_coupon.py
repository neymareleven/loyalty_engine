import uuid

from sqlalchemy import Column, ForeignKey, Integer, JSON, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class CustomerCoupon(Base):
    __tablename__ = "customer_coupons"

    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_customer_coupons_idempotency_key"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    customer_id = Column(UUID(as_uuid=True), ForeignKey("customers.id"), nullable=False)
    coupon_type_id = Column(UUID(as_uuid=True), ForeignKey("coupon_types.id"), nullable=False)

    status = Column(String(20), nullable=False, default="ISSUED")
    # ISSUED | USED | EXPIRED

    issued_at = Column(TIMESTAMP, server_default=func.now())
    expires_at = Column(TIMESTAMP, nullable=True)
    used_at = Column(TIMESTAMP, nullable=True)

    source_transaction_id = Column(UUID(as_uuid=True), ForeignKey("transactions.id"), nullable=True)

    rule_id = Column(UUID(as_uuid=True), ForeignKey("rules.id", ondelete="SET NULL"), nullable=True)
    rule_execution_id = Column(
        UUID(as_uuid=True),
        ForeignKey("transaction_rule_execution.id", ondelete="SET NULL"),
        nullable=True,
    )

    idempotency_key = Column(String(255), nullable=True)

    payload = Column(JSON, nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
