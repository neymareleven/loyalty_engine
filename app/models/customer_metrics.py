import uuid

from sqlalchemy import Column, Integer, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class CustomerMetrics(Base):
    __tablename__ = "customer_metrics"

    __table_args__ = (
        UniqueConstraint("brand", "customer_id", name="uq_customer_metrics_brand_customer"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    brand = Column(String(50), nullable=False)
    customer_id = Column(UUID(as_uuid=True), nullable=False)

    last_transaction_at = Column(TIMESTAMP)
    transactions_count_30d = Column(Integer, nullable=False, default=0)
    transactions_count_90d = Column(Integer, nullable=False, default=0)

    computed_at = Column(TIMESTAMP)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
