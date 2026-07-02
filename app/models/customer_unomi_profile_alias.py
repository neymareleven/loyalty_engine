import uuid

from sqlalchemy import Column, ForeignKey, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.db import Base


class CustomerUnomiProfileAlias(Base):
    """Unomi profileId seen for a customer (session cookie, pre-merge duplicate).

    The canonical id for Loyalty remains ``customers.profile_id`` (master).
    Aliases are for lookup and audit only — never overwrite the master or transactions.
    """

    __tablename__ = "customer_unomi_profile_aliases"

    __table_args__ = (
        UniqueConstraint("brand", "profile_id", name="uq_customer_unomi_aliases_brand_profile_id"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    brand = Column(String(50), nullable=False)
    customer_id = Column(UUID(as_uuid=True), ForeignKey("customers.id", ondelete="CASCADE"), nullable=False)
    profile_id = Column(String(100), nullable=False)
    source = Column(String(30), nullable=False, default="session")
    first_seen_at = Column(TIMESTAMP, server_default=func.now(), nullable=False)
    last_seen_at = Column(TIMESTAMP, server_default=func.now(), nullable=False)
