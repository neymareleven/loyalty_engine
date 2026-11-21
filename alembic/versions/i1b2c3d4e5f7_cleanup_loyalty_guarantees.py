"""align loyalty guarantees to a single status window

Revision ID: i1b2c3d4e5f7
Revises: h0a1b2c3d4e6
Create Date: 2026-11-21

Revokes guarantees whose floor is already earned and aligns remaining
expires_at to customer.loyalty_status_expires_at. Safe to re-run.
"""

from typing import Sequence, Union

from alembic import op

revision: str = "i1b2c3d4e5f7"
down_revision: Union[str, Sequence[str], None] = "h0a1b2c3d4e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    from sqlalchemy.orm import sessionmaker

    from app.models.customer import Customer  # noqa: F401
    from app.models.customer_loyalty_guarantee import CustomerLoyaltyGuarantee  # noqa: F401
    from app.models.loyalty_tier import LoyaltyTier  # noqa: F401
    from app.models.brand_loyalty_settings import BrandLoyaltySettings  # noqa: F401
    from app.services.loyalty_guarantee_service import cleanup_loyalty_guarantees

    bind = op.get_bind()
    Session = sessionmaker(bind=bind)
    session = Session()
    try:
        stats = cleanup_loyalty_guarantees(session)
        session.commit()
        print("cleanup_loyalty_guarantees:", stats)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def downgrade() -> None:
    pass
