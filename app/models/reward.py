import uuid
from sqlalchemy import Column, String, Integer, Boolean, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.db import Base


class Reward(Base):
    __tablename__ = "rewards"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    brand = Column(String(50), nullable=False)

    name = Column(String(100), nullable=False)
    description = Column(String(255))

    # NULL = reward gratuite (marketing)
    cost_points = Column(Integer, nullable=True)

    # type fonctionnel de reward
    type = Column(String(50), nullable=False, default="POINTS")

    # durée de validité après attribution (en jours)
    validity_days = Column(Integer, nullable=True)

    active = Column(Boolean, default=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
