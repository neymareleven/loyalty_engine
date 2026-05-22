"""backfill customer_rewards catalog snapshots in payload

Revision ID: fe67ab89cd01
Revises: fd56ab78ef90
Create Date: 2026-05-22

"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "fe67ab89cd01"
down_revision: Union[str, Sequence[str], None] = "fd56ab78ef90"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    from sqlalchemy.orm import sessionmaker

    # Register all ORM tables referenced by CustomerReward FKs before flush.
    from app.models.customer import Customer  # noqa: F401
    from app.models.customer_coupon import CustomerCoupon  # noqa: F401
    from app.models.coupon_type import CouponType  # noqa: F401
    from app.models.reward import Reward  # noqa: F401
    from app.models.rule import Rule  # noqa: F401
    from app.models.transaction import Transaction  # noqa: F401
    from app.models.transaction_rule_execution import TransactionRuleExecution  # noqa: F401

    from app.services.catalog_admin_service import backfill_customer_reward_snapshots

    bind = op.get_bind()
    Session = sessionmaker(bind=bind)
    session = Session()
    try:
        stats = backfill_customer_reward_snapshots(session)
        session.commit()
        print(
            "backfill customer_rewards snapshots:",
            stats,
        )
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def downgrade() -> None:
    # Data backfill is not reversed (snapshots are additive).
    pass
