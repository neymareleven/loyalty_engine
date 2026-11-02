import uuid

from sqlalchemy import Column, Date, Integer, String, TIMESTAMP, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class PurchaseMovement(Base):
    """One sale (or admin adjustment) contributing to purchase-count + purchase-amount qualification."""

    __tablename__ = "purchase_movements"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    customer_id = Column(UUID(as_uuid=True), ForeignKey("customers.id"), nullable=False)

    quantity = Column(Integer, nullable=False)
    amount = Column(Integer, nullable=False)
    type = Column(String(20), nullable=False)  # EARN / DEDUCT / EXPIRE

    source_transaction_id = Column(UUID(as_uuid=True), ForeignKey("transactions.id"))

    created_at = Column(TIMESTAMP, server_default=func.now())
    expires_at = Column(Date, nullable=True)
