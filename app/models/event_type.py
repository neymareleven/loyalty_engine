import uuid

from sqlalchemy import Boolean, Column, JSON, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class EventType(Base):
    __tablename__ = "event_types"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    brand = Column(String(50), nullable=True)

    key = Column(String(100), nullable=False)
    origin = Column(String(20), nullable=False)  # EXTERNAL / INTERNAL

    name = Column(String(200), nullable=False)
    description = Column(String(1000), nullable=True)

    payload_schema = Column(JSON, nullable=True)

    active = Column(Boolean, default=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
