"""purchase qualification mode (count + amount) for loyalty tiers

Revision ID: b4c5d6e7f8a9
Revises: a3b4c5d6e7f8
Create Date: 2026-10-30

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "b4c5d6e7f8a9"
down_revision: Union[str, Sequence[str], None] = "a3b4c5d6e7f8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if inspector.has_table("brand_loyalty_settings"):
        cols = {c["name"] for c in inspector.get_columns("brand_loyalty_settings")}
        if "qualification_mode" not in cols:
            op.add_column(
                "brand_loyalty_settings",
                sa.Column("qualification_mode", sa.String(length=20), nullable=False, server_default="POINTS"),
            )
        if "purchase_validity_days" not in cols:
            op.add_column(
                "brand_loyalty_settings",
                sa.Column("purchase_validity_days", sa.Integer(), nullable=True),
            )

    if inspector.has_table("loyalty_tiers"):
        cols = {c["name"] for c in inspector.get_columns("loyalty_tiers")}
        if "min_purchase_count" not in cols:
            op.add_column(
                "loyalty_tiers",
                sa.Column("min_purchase_count", sa.Integer(), nullable=False, server_default="0"),
            )
        if "min_purchase_amount" not in cols:
            op.add_column(
                "loyalty_tiers",
                sa.Column("min_purchase_amount", sa.Integer(), nullable=False, server_default="0"),
            )

    if inspector.has_table("customers"):
        cols = {c["name"] for c in inspector.get_columns("customers")}
        if "purchase_count" not in cols:
            op.add_column(
                "customers",
                sa.Column("purchase_count", sa.Integer(), nullable=False, server_default="0"),
            )
        if "purchase_amount_total" not in cols:
            op.add_column(
                "customers",
                sa.Column("purchase_amount_total", sa.Integer(), nullable=False, server_default="0"),
            )
        if "purchases_expires_at" not in cols:
            op.add_column("customers", sa.Column("purchases_expires_at", sa.TIMESTAMP(), nullable=True))

    if not inspector.has_table("purchase_movements"):
        op.create_table(
            "purchase_movements",
            sa.Column("id", UUID(as_uuid=True), primary_key=True),
            sa.Column("customer_id", UUID(as_uuid=True), sa.ForeignKey("customers.id"), nullable=False),
            sa.Column("quantity", sa.Integer(), nullable=False),
            sa.Column("amount", sa.Integer(), nullable=False),
            sa.Column("type", sa.String(length=20), nullable=False),
            sa.Column("source_transaction_id", UUID(as_uuid=True), sa.ForeignKey("transactions.id"), nullable=True),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.func.now()),
            sa.Column("expires_at", sa.Date(), nullable=True),
        )
        op.create_index("ix_purchase_movements_customer_id", "purchase_movements", ["customer_id"])
        op.create_index(
            "ix_purchase_movements_customer_expires_at",
            "purchase_movements",
            ["customer_id", "expires_at"],
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if inspector.has_table("purchase_movements"):
        op.drop_table("purchase_movements")

    if inspector.has_table("customers"):
        cols = {c["name"] for c in inspector.get_columns("customers")}
        if "purchases_expires_at" in cols:
            op.drop_column("customers", "purchases_expires_at")
        if "purchase_amount_total" in cols:
            op.drop_column("customers", "purchase_amount_total")
        if "purchase_count" in cols:
            op.drop_column("customers", "purchase_count")

    if inspector.has_table("loyalty_tiers"):
        cols = {c["name"] for c in inspector.get_columns("loyalty_tiers")}
        if "min_purchase_amount" in cols:
            op.drop_column("loyalty_tiers", "min_purchase_amount")
        if "min_purchase_count" in cols:
            op.drop_column("loyalty_tiers", "min_purchase_count")

    if inspector.has_table("brand_loyalty_settings"):
        cols = {c["name"] for c in inspector.get_columns("brand_loyalty_settings")}
        if "purchase_validity_days" in cols:
            op.drop_column("brand_loyalty_settings", "purchase_validity_days")
        if "qualification_mode" in cols:
            op.drop_column("brand_loyalty_settings", "qualification_mode")
