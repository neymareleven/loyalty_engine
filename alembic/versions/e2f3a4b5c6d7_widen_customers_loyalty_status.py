"""Widen customers.loyalty_status to match loyalty_tiers.key (varchar 50).

Revision ID: e2f3a4b5c6d7
Revises: d1e2f3a4b5c6
Create Date: 2026-07-09

"""

from alembic import op
import sqlalchemy as sa


revision = "e2f3a4b5c6d7"
down_revision = "d1e2f3a4b5c6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if not insp.has_table("customers"):
        return

    cols = {c["name"]: c for c in insp.get_columns("customers")}
    if "loyalty_status" not in cols:
        return

    current_len = cols["loyalty_status"].get("type").length
    if current_len is not None and current_len >= 50:
        return

    op.alter_column(
        "customers",
        "loyalty_status",
        existing_type=sa.String(length=20),
        type_=sa.String(length=50),
        existing_nullable=True,
    )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if not insp.has_table("customers"):
        return

    cols = {c["name"] for c in insp.get_columns("customers")}
    if "loyalty_status" not in cols:
        return

    op.execute(
        sa.text(
            "UPDATE customers SET loyalty_status = LEFT(loyalty_status, 20) "
            "WHERE loyalty_status IS NOT NULL AND LENGTH(loyalty_status) > 20"
        )
    )
    op.alter_column(
        "customers",
        "loyalty_status",
        existing_type=sa.String(length=50),
        type_=sa.String(length=20),
        existing_nullable=True,
    )
