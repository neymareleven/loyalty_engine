import uuid
from sqlalchemy import Column, String, Integer, Boolean, JSON, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.db import Base


class Rule(Base):
    __tablename__ = "rules"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    brand = Column(String(50), nullable=False)
    event_type = Column(String(50), nullable=False)
    priority = Column(Integer, default=0)

    conditions = Column(JSON)
    actions = Column(JSON)

    active = Column(Boolean, default=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
