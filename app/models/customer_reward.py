import uuid
from sqlalchemy import Column, String, TIMESTAMP, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.db import Base


class CustomerReward(Base):
    __tablename__ = "customer_rewards"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    customer_id = Column(UUID(as_uuid=True), ForeignKey("customers.id"), nullable=False)
    reward_id = Column(UUID(as_uuid=True), ForeignKey("rewards.id"), nullable=False)

    status = Column(String(20), nullable=False, default="ISSUED")
    # ISSUED | USED | EXPIRED | CANCELLED

    issued_at = Column(TIMESTAMP, server_default=func.now())
    expires_at = Column(TIMESTAMP, nullable=True)
    used_at = Column(TIMESTAMP, nullable=True)

    source_transaction_id = Column(
        UUID(as_uuid=True),
        ForeignKey("transactions.id"),
        nullable=True,
    )
