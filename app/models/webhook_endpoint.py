import uuid

from sqlalchemy import Boolean, Column, JSON, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class WebhookEndpoint(Base):
    __tablename__ = "webhook_endpoints"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    brand = Column(String(50), nullable=False)
    url = Column(String(500), nullable=False)
    secret = Column(String(255), nullable=False)
    events = Column(JSON, nullable=False, default=list)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
