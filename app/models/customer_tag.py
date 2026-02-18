import uuid

from sqlalchemy import Column, ForeignKey, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class CustomerTag(Base):
    __tablename__ = "customer_tags"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    customer_id = Column(UUID(as_uuid=True), ForeignKey("customers.id"), nullable=False)
    tag = Column(String(100), nullable=False)

    created_at = Column(TIMESTAMP, server_default=func.now())

    __table_args__ = (UniqueConstraint("customer_id", "tag", name="uq_customer_tags_customer_id_tag"),)
