import uuid

from sqlalchemy import Column, JSON, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class BonusAward(Base):
    __tablename__ = "bonus_awards"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    bonus_key = Column(String(100), nullable=False)

    brand = Column(String(50), nullable=False)
    profile_id = Column(String(100), nullable=False)

    period_key = Column(String(50), nullable=True)

    event_id = Column(String(150), nullable=True)
    transaction_id = Column(UUID(as_uuid=True), nullable=True)

    meta = Column("metadata", JSON, nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
