import uuid

from sqlalchemy import Boolean, Column, JSON, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class InternalJob(Base):
    __tablename__ = "internal_jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    job_key = Column(String(100), nullable=False)
    brand = Column(String(50), nullable=True)

    event_type = Column(String(50), nullable=False)

    selector = Column(JSON, nullable=False, default=dict)
    payload_template = Column(JSON, nullable=True)

    active = Column(Boolean, default=True)

    schedule = Column(String(50), nullable=True)

    next_run_at = Column(TIMESTAMP, nullable=True)
    last_run_at = Column(TIMESTAMP, nullable=True)

    locked_at = Column(TIMESTAMP, nullable=True)
    locked_by = Column(String(100), nullable=True)

    last_status = Column(String(20), nullable=True)
    last_error = Column(String(2000), nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
