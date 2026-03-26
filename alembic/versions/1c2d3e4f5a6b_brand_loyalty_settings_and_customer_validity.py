"""brand loyalty settings and customer validity

Revision ID: 1c2d3e4f5a6b
Revises: 93229b4c2da4
Create Date: 2026-03-25

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "1c2d3e4f5a6b"
down_revision: Union[str, Sequence[str], None] = "93229b4c2da4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("brand_loyalty_settings"):
        op.create_table(
            "brand_loyalty_settings",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column("brand", sa.String(length=50), nullable=False),
            sa.Column("points_validity_days", sa.Integer(), nullable=True),
            sa.Column("loyalty_status_validity_days", sa.Integer(), nullable=True),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.UniqueConstraint("brand", name="uq_brand_loyalty_settings_brand"),
        )

    if insp.has_table("customers"):
        cols = {c["name"] for c in insp.get_columns("customers")}

        if "points_expires_at" not in cols:
            op.add_column("customers", sa.Column("points_expires_at", sa.TIMESTAMP(), nullable=True))

        if "loyalty_status_assigned_at" not in cols:
            op.add_column("customers", sa.Column("loyalty_status_assigned_at", sa.TIMESTAMP(), nullable=True))

        if "loyalty_status_expires_at" not in cols:
            op.add_column("customers", sa.Column("loyalty_status_expires_at", sa.TIMESTAMP(), nullable=True))

        existing_indexes = {ix["name"] for ix in insp.get_indexes("customers")}
        if "ix_customers_brand_points_expires_at" not in existing_indexes:
            op.create_index(
                "ix_customers_brand_points_expires_at",
                "customers",
                ["brand", "points_expires_at"],
                unique=False,
            )
        if "ix_customers_brand_loyalty_status_expires_at" not in existing_indexes:
            op.create_index(
                "ix_customers_brand_loyalty_status_expires_at",
                "customers",
                ["brand", "loyalty_status_expires_at"],
                unique=False,
            )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if insp.has_table("customers"):
        existing_indexes = {ix["name"] for ix in insp.get_indexes("customers")}
        if "ix_customers_brand_loyalty_status_expires_at" in existing_indexes:
            op.drop_index("ix_customers_brand_loyalty_status_expires_at", table_name="customers")
        if "ix_customers_brand_points_expires_at" in existing_indexes:
            op.drop_index("ix_customers_brand_points_expires_at", table_name="customers")

        cols = {c["name"] for c in insp.get_columns("customers")}
        if "loyalty_status_expires_at" in cols:
            op.drop_column("customers", "loyalty_status_expires_at")
        if "loyalty_status_assigned_at" in cols:
            op.drop_column("customers", "loyalty_status_assigned_at")
        if "points_expires_at" in cols:
            op.drop_column("customers", "points_expires_at")

    if insp.has_table("brand_loyalty_settings"):
        op.drop_table("brand_loyalty_settings")
