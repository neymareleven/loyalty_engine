import uuid
from sqlalchemy import Column, Integer, String, TIMESTAMP, ForeignKey
from sqlalchemy import Date
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.db import Base


class PointMovement(Base):
    __tablename__ = "point_movements"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    customer_id = Column(UUID(as_uuid=True), ForeignKey("customers.id"), nullable=False)

    points = Column(Integer, nullable=False)
    type = Column(String(20), nullable=False)  # EARN / BURN / ADJUST

    source_transaction_id = Column(UUID(as_uuid=True), ForeignKey("transactions.id"))

    created_at = Column(TIMESTAMP, server_default=func.now())

    expires_at = Column(Date, nullable=True)
