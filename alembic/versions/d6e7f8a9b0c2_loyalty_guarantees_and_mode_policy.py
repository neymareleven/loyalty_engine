"""customer loyalty guarantees + mode_switch_policy

Revision ID: d6e7f8a9b0c2
Revises: c5d6e7f8a9b0
Create Date: 2026-11-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "d6e7f8a9b0c2"
down_revision: Union[str, Sequence[str], None] = "c5d6e7f8a9b0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    cols = set()
    if inspector.has_table("brand_loyalty_settings"):
        cols = {c["name"] for c in inspector.get_columns("brand_loyalty_settings")}
    if "mode_switch_policy" not in cols and inspector.has_table("brand_loyalty_settings"):
        op.add_column(
            "brand_loyalty_settings",
            sa.Column(
                "mode_switch_policy",
                sa.String(length=30),
                nullable=False,
                server_default="IMMEDIATE",
            ),
        )

    if not inspector.has_table("customer_loyalty_guarantees"):
        op.create_table(
            "customer_loyalty_guarantees",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column("customer_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("brand", sa.String(length=50), nullable=False),
            sa.Column("source", sa.String(length=50), nullable=False),
            sa.Column("floor_tier_key", sa.String(length=50), nullable=False),
            sa.Column("expires_at", sa.TIMESTAMP(), nullable=True),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.func.now(), nullable=True),
            sa.Column("revoked_at", sa.TIMESTAMP(), nullable=True),
            sa.ForeignKeyConstraint(["customer_id"], ["customers.id"], ondelete="CASCADE"),
        )
        op.create_index(
            "ix_customer_loyalty_guarantees_customer",
            "customer_loyalty_guarantees",
            ["customer_id"],
        )
        op.create_index(
            "ix_customer_loyalty_guarantees_brand_active",
            "customer_loyalty_guarantees",
            ["brand", "revoked_at"],
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if inspector.has_table("customer_loyalty_guarantees"):
        op.drop_index("ix_customer_loyalty_guarantees_brand_active", table_name="customer_loyalty_guarantees")
        op.drop_index("ix_customer_loyalty_guarantees_customer", table_name="customer_loyalty_guarantees")
        op.drop_table("customer_loyalty_guarantees")
    if inspector.has_table("brand_loyalty_settings"):
        cols = {c["name"] for c in inspector.get_columns("brand_loyalty_settings")}
        if "mode_switch_policy" in cols:
            op.drop_column("brand_loyalty_settings", "mode_switch_policy")
