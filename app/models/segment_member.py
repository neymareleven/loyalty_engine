from sqlalchemy import Column, ForeignKey, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class SegmentMember(Base):
    __tablename__ = "segment_members"

    __table_args__ = (
        UniqueConstraint("segment_id", "customer_id", name="uq_segment_members_segment_customer"),
    )

    segment_id = Column(UUID(as_uuid=True), ForeignKey("segments.id", ondelete="CASCADE"), primary_key=True)
    customer_id = Column(UUID(as_uuid=True), ForeignKey("customers.id", ondelete="CASCADE"), primary_key=True)

    # STATIC or DYNAMIC
    source = Column(String(20), nullable=False, default="DYNAMIC")

    computed_at = Column(TIMESTAMP, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
