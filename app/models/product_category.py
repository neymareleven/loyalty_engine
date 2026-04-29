import uuid

from sqlalchemy import Boolean, Column, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class ProductCategory(Base):
    __tablename__ = "product_categories"

    __table_args__ = (UniqueConstraint("brand", "name", name="uq_product_categories_brand_name"),)

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    brand = Column(String(50), nullable=False)

    name = Column(String(200), nullable=False)
    description = Column(String(1000), nullable=True)

    active = Column(Boolean, default=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
