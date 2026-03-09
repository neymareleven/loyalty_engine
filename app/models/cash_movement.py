import uuid

from sqlalchemy import Column, Integer, String, TIMESTAMP, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class CashMovement(Base):
    __tablename__ = "cash_movements"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    customer_id = Column(UUID(as_uuid=True), ForeignKey("customers.id"), nullable=False)

    amount = Column(Integer, nullable=False)
    currency = Column(String(3), nullable=False)
    type = Column(String(20), nullable=False)  # CREDIT / DEBIT / ADJUST

    source_transaction_id = Column(UUID(as_uuid=True), ForeignKey("transactions.id"), nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
