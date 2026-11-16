import uuid

from sqlalchemy import Column, ForeignKey, Index, Integer, JSON, String, TIMESTAMP, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class WebhookOutbox(Base):
    __tablename__ = "webhook_outbox"

    __table_args__ = (
        Index("ix_webhook_outbox_status_next", "status", "next_attempt_at"),
        Index("ix_webhook_outbox_brand", "brand"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    brand = Column(String(50), nullable=False)
    endpoint_id = Column(UUID(as_uuid=True), ForeignKey("webhook_endpoints.id"), nullable=False)
    event = Column(String(80), nullable=False)
    payload = Column(JSON, nullable=False)
    status = Column(String(20), nullable=False, default="PENDING")
    attempts = Column(Integer, nullable=False, default=0)
    next_attempt_at = Column(TIMESTAMP, nullable=True)
    last_error = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    sent_at = Column(TIMESTAMP, nullable=True)
