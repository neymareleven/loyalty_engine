import uuid

from sqlalchemy import Boolean, Column, JSON, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class ApiClient(Base):
    __tablename__ = "api_clients"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    brand = Column(String(50), nullable=False)
    name = Column(String(150), nullable=False)
    key_hash = Column(String(64), nullable=False, unique=True)
    key_prefix = Column(String(16), nullable=False)
    scopes = Column(JSON, nullable=False, default=list)
    active = Column(Boolean, nullable=False, default=True)
    last_used_at = Column(TIMESTAMP, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
