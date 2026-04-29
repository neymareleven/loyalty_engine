"""customer metrics table

Revision ID: a1b2c3d4e5f7
Revises: 9d3a2b1c0f4e
Create Date: 2026-04-22

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "a1b2c3d4e5f7"
down_revision: Union[str, Sequence[str], None] = "9d3a2b1c0f4e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if insp.has_table("customer_metrics"):
        return

    op.create_table(
        "customer_metrics",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("brand", sa.String(length=50), nullable=False),
        sa.Column("customer_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("last_transaction_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("transactions_count_30d", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("transactions_count_90d", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("computed_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
        sa.UniqueConstraint("brand", "customer_id", name="uq_customer_metrics_brand_customer"),
    )

    op.create_index(
        "ix_customer_metrics_brand_customer",
        "customer_metrics",
        ["brand", "customer_id"],
        unique=False,
    )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("customer_metrics"):
        return

    op.drop_index("ix_customer_metrics_brand_customer", table_name="customer_metrics")
    op.drop_table("customer_metrics")
