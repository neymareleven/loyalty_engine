import uuid
from sqlalchemy import Column, JSON, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.db import Base


class Transaction(Base):
    __tablename__ = "transactions"

    __table_args__ = (UniqueConstraint("brand", "event_id", name="uq_transactions_brand_event_id"),)

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    brand = Column(String(50), nullable=False)
    profile_id = Column(String(100), nullable=False)
    event_type = Column(String(50), nullable=False)
    event_id = Column(String(100), nullable=False)

    source = Column(String(20))
    payload = Column(JSON)

    status = Column(String(20), nullable=False, default="PENDING")

    idempotency_key = Column(String(150))
    error_code = Column(String(50))
    error_message = Column(String)

    created_at = Column(TIMESTAMP, server_default=func.now())
    processed_at = Column(TIMESTAMP)
