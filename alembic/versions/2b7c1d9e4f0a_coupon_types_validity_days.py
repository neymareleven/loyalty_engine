"""coupon types validity days

Revision ID: 2b7c1d9e4f0a
Revises: 00e584102a28
Create Date: 2026-04-21

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "2b7c1d9e4f0a"
down_revision: Union[str, Sequence[str], None] = "00e584102a28"
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

    if not insp.has_table("coupon_types"):
        return

    if not _has_column(insp, "coupon_types", "validity_days"):
        op.add_column("coupon_types", sa.Column("validity_days", sa.Integer(), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("coupon_types"):
        return

    if _has_column(insp, "coupon_types", "validity_days"):
        op.drop_column("coupon_types", "validity_days")
