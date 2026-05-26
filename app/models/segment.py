import uuid

from sqlalchemy import Boolean, Column, JSON, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class Segment(Base):
    __tablename__ = "segments"

    __table_args__ = (UniqueConstraint("brand", "name", name="uq_segments_brand_name"),)

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    brand = Column(String(50), nullable=False)

    name = Column(String(255), nullable=False)
    description = Column(String(1000), nullable=True)

    is_dynamic = Column(Boolean, nullable=False, default=True)
    conditions = Column(JSON, nullable=True)

    # INTERNAL | UNOMI — when UNOMI, unomi_segment_id points to Apache Unomi segment metadata.id
    provider = Column(String(20), nullable=False, default="INTERNAL")
    unomi_segment_id = Column(String(255), nullable=True)
    unomi_scope = Column(String(100), nullable=True)
    manual_profile_ids = Column(JSONB, nullable=True)
    unomi_condition = Column(JSONB, nullable=True)

    active = Column(Boolean, default=True)

    last_computed_at = Column(TIMESTAMP, nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
