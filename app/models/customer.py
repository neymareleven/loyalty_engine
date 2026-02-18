import uuid
from sqlalchemy import Column, String, TIMESTAMP, Date, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.db import Base


class Customer(Base):
    __tablename__ = "customers"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    brand = Column(String(50), nullable=False)
    profile_id = Column(String(100), nullable=False)

    gender = Column(String(10))      # M / F / OTHER / UNKNOWN
    birthdate = Column(Date)

    status = Column(String(20), default="ACTIVE")
    loyalty_status = Column(String(20), nullable=False, default="BRONZE")
    lifetime_points = Column(Integer, nullable=False, default=0)

    status_points = Column(Integer, nullable=False, default=0)
    last_activity_at = Column(TIMESTAMP)
    status_points_reset_at = Column(TIMESTAMP)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
