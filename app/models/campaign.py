import uuid
from sqlalchemy import Column, String, Integer, Boolean, TIMESTAMP, JSON
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.db import Base


class Campaign(Base):
    __tablename__ = "campaigns"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    brand = Column(String(50), nullable=False)

    name = Column(String(100), nullable=False)

    event_type = Column(String(50), nullable=False)

    bonus_points = Column(Integer, nullable=False)

    conditions = Column(JSON, default={})  
    # ex: {"weekend": true} ou {"first_purchase": true}

    active = Column(Boolean, default=True)

    start_date = Column(TIMESTAMP)
    end_date = Column(TIMESTAMP)

    created_at = Column(TIMESTAMP, server_default=func.now())
