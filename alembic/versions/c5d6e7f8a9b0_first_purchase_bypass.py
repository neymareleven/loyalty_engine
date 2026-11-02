"""first-purchase bypass (amount + target tier)

Revision ID: c5d6e7f8a9b0
Revises: b4c5d6e7f8a9
Create Date: 2026-10-30

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "c5d6e7f8a9b0"
down_revision: Union[str, Sequence[str], None] = "b4c5d6e7f8a9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table("brand_loyalty_settings"):
        return
    cols = {c["name"] for c in inspector.get_columns("brand_loyalty_settings")}
    if "first_purchase_bypass_enabled" not in cols:
        op.add_column(
            "brand_loyalty_settings",
            sa.Column(
                "first_purchase_bypass_enabled",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            ),
        )
    if "first_purchase_bypass_min_amount" not in cols:
        op.add_column(
            "brand_loyalty_settings",
            sa.Column("first_purchase_bypass_min_amount", sa.Integer(), nullable=True),
        )
    if "first_purchase_bypass_tier_key" not in cols:
        op.add_column(
            "brand_loyalty_settings",
            sa.Column("first_purchase_bypass_tier_key", sa.String(length=50), nullable=True),
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table("brand_loyalty_settings"):
        return
    cols = {c["name"] for c in inspector.get_columns("brand_loyalty_settings")}
    if "first_purchase_bypass_tier_key" in cols:
        op.drop_column("brand_loyalty_settings", "first_purchase_bypass_tier_key")
    if "first_purchase_bypass_min_amount" in cols:
        op.drop_column("brand_loyalty_settings", "first_purchase_bypass_min_amount")
    if "first_purchase_bypass_enabled" in cols:
        op.drop_column("brand_loyalty_settings", "first_purchase_bypass_enabled")
