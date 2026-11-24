import uuid

from sqlalchemy import Boolean, Column, Index, Integer, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class ErasedIdentity(Base):
    __tablename__ = "erased_identities"

    __table_args__ = (
        UniqueConstraint("brand", "email_hash", name="uq_erased_identities_brand_email_hash"),
        Index("ix_erased_identities_brand_hash", "brand", "email_hash"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    brand = Column(String(50), nullable=False)
    email_hash = Column(String(64), nullable=False)
    erased_at = Column(TIMESTAMP, server_default=func.now(), nullable=False)
    bypass_used = Column(Boolean, nullable=False, default=False)
    mode = Column(String(20), nullable=False, default="anonymize")


class UnomiDeleteQueue(Base):
    __tablename__ = "unomi_delete_queue"

    __table_args__ = (Index("ix_unomi_delete_queue_brand_status", "brand", "status"),)

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    brand = Column(String(50), nullable=False)
    profile_id = Column(String(100), nullable=False)
    status = Column(String(20), nullable=False, default="PENDING")
    attempts = Column(Integer, nullable=False, default=0)
    last_error = Column(String(2000), nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
