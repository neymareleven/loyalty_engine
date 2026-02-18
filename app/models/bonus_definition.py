import uuid

from sqlalchemy import Boolean, Column, JSON, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class BonusDefinition(Base):
    __tablename__ = "bonus_definitions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    bonus_key = Column(String(100), nullable=False, unique=True)
    brand = Column(String(50), nullable=True)

    name = Column(String(200), nullable=False)
    description = Column(String(1000), nullable=True)

    award_policy = Column(String(50), nullable=False)  # ONCE_EVER / ONCE_PER_YEAR / ...
    policy_params = Column(JSON, nullable=True)

    active = Column(Boolean, default=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
