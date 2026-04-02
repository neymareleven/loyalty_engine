import uuid
from sqlalchemy import Column, ForeignKey, String, Boolean, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.db import Base


class Reward(Base):
    __tablename__ = "rewards"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    brand = Column(String(50), nullable=False)

    reward_category_id = Column(
        UUID(as_uuid=True),
        ForeignKey("reward_categories.id", ondelete="RESTRICT"),
        nullable=True,
    )

    name = Column(String(100), nullable=False)
    description = Column(String(255))

    active = Column(Boolean, default=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
