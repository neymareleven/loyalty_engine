"""drop brand loyalty settings coupon validity days

Revision ID: 8c2a1f0d9e4b
Revises: 2b7c1d9e4f0a
Create Date: 2026-04-21

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "8c2a1f0d9e4b"
down_revision: Union[str, Sequence[str], None] = "2b7c1d9e4f0a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_column(insp, table_name: str, column_name: str) -> bool:
    try:
        cols = insp.get_columns(table_name)
    except Exception:
        return False
    return any(c.get("name") == column_name for c in cols)


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("brand_loyalty_settings"):
        return

    if _has_column(insp, "brand_loyalty_settings", "coupon_validity_days"):
        op.drop_column("brand_loyalty_settings", "coupon_validity_days")


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("brand_loyalty_settings"):
        return

    if not _has_column(insp, "brand_loyalty_settings", "coupon_validity_days"):
        op.add_column(
            "brand_loyalty_settings",
            sa.Column("coupon_validity_days", sa.Integer(), nullable=True),
        )
