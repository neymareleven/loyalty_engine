"""add status_points and loyalty_tiers

Revision ID: 0c057ed5526f
Revises: 
Create Date: 2026-02-17 01:18:56.627405

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '0c057ed5526f'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    customer_cols = {c["name"] for c in inspector.get_columns("customers")}
    if "status_points" not in customer_cols:
        op.add_column(
            "customers",
            sa.Column("status_points", sa.Integer(), server_default="0", nullable=False),
        )
    if "last_activity_at" not in customer_cols:
        op.add_column("customers", sa.Column("last_activity_at", sa.TIMESTAMP(), nullable=True))
    if "status_points_reset_at" not in customer_cols:
        op.add_column(
            "customers",
            sa.Column("status_points_reset_at", sa.TIMESTAMP(), nullable=True),
        )

    if not inspector.has_table("loyalty_tiers"):
        op.create_table(
            "loyalty_tiers",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column("brand", sa.String(length=50), nullable=False),
            sa.Column("key", sa.String(length=50), nullable=False),
            sa.Column("name", sa.String(length=200), nullable=False),
            sa.Column("min_status_points", sa.Integer(), nullable=False),
            sa.Column("rank", sa.Integer(), nullable=False),
            sa.Column("active", sa.Boolean(), nullable=True),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.UniqueConstraint("brand", "key", name="uq_loyalty_tiers_brand_key"),
        )

    existing_indexes = {ix["name"] for ix in inspector.get_indexes("loyalty_tiers")}
    if "ix_loyalty_tiers_brand_rank" not in existing_indexes:
        op.create_index("ix_loyalty_tiers_brand_rank", "loyalty_tiers", ["brand", "rank"])
    if "ix_loyalty_tiers_brand_active" not in existing_indexes:
        op.create_index("ix_loyalty_tiers_brand_active", "loyalty_tiers", ["brand", "active"])


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if inspector.has_table("loyalty_tiers"):
        existing_indexes = {ix["name"] for ix in inspector.get_indexes("loyalty_tiers")}
        if "ix_loyalty_tiers_brand_active" in existing_indexes:
            op.drop_index("ix_loyalty_tiers_brand_active", table_name="loyalty_tiers")
        if "ix_loyalty_tiers_brand_rank" in existing_indexes:
            op.drop_index("ix_loyalty_tiers_brand_rank", table_name="loyalty_tiers")

        op.drop_table("loyalty_tiers")

    customer_cols = {c["name"] for c in inspector.get_columns("customers")}
    if "status_points_reset_at" in customer_cols:
        op.drop_column("customers", "status_points_reset_at")
    if "last_activity_at" in customer_cols:
        op.drop_column("customers", "last_activity_at")
    if "status_points" in customer_cols:
        op.drop_column("customers", "status_points")
