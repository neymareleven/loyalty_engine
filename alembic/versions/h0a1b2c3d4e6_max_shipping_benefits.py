"""max shipping benefits per cart

Revision ID: h0a1b2c3d4e6
Revises: g9b0c1d2e3f5
Create Date: 2026-11-19

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "h0a1b2c3d4e6"
down_revision: Union[str, Sequence[str], None] = "g9b0c1d2e3f5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table("brand_loyalty_settings"):
        return
    cols = {c["name"] for c in inspector.get_columns("brand_loyalty_settings")}
    if "max_shipping_benefits_per_cart" not in cols:
        op.add_column(
            "brand_loyalty_settings",
            sa.Column(
                "max_shipping_benefits_per_cart",
                sa.Integer(),
                nullable=False,
                server_default="1",
            ),
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table("brand_loyalty_settings"):
        return
    cols = {c["name"] for c in inspector.get_columns("brand_loyalty_settings")}
    if "max_shipping_benefits_per_cart" in cols:
        op.drop_column("brand_loyalty_settings", "max_shipping_benefits_per_cart")
