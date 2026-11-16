import uuid

from sqlalchemy import Column, Index, JSON, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class StorefrontAuditEvent(Base):
    __tablename__ = "storefront_audit_events"

    __table_args__ = (Index("ix_storefront_audit_brand_created", "brand", "created_at"),)

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    brand = Column(String(50), nullable=False)
    site_id = Column(String(100), nullable=True)
    action = Column(String(40), nullable=False)
    profile_id = Column(String(100), nullable=True)
    redemption_id = Column(String(64), nullable=True)
    request = Column(JSON, nullable=True)
    response = Column(JSON, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
