import uuid

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class Product(Base):
    __tablename__ = "products"

    __table_args__ = (
        UniqueConstraint("brand", "match_key", name="uq_products_brand_match_key"),
        UniqueConstraint("brand", "name", name="uq_products_brand_name"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    brand = Column(String(50), nullable=False)

    category_id = Column(
        UUID(as_uuid=True),
        ForeignKey("product_categories.id", ondelete="RESTRICT"),
        nullable=True,
    )

    name = Column(String(255), nullable=False)

    # Key used to match incoming productNames[] from external events.
    # Suggested format: slug/normalized key derived from name.
    match_key = Column(String(255), nullable=False)

    points_value = Column(Integer, nullable=True)

    active = Column(Boolean, default=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
