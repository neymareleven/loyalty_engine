"""rewards usage limits

Revision ID: c4a6f1b2d3e4
Revises: b2c3d4e5f6a7
Create Date: 2026-03-06

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c4a6f1b2d3e4"
down_revision: Union[str, Sequence[str], None] = "b2c3d4e5f6a7"
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

    if not insp.has_table("rewards"):
        return

    if not _has_column(insp, "rewards", "max_attributions"):
        op.add_column("rewards", sa.Column("max_attributions", sa.Integer(), nullable=True))

    if not _has_column(insp, "rewards", "reset_period"):
        op.add_column("rewards", sa.Column("reset_period", sa.String(length=20), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("rewards"):
        return

    if _has_column(insp, "rewards", "reset_period"):
        op.drop_column("rewards", "reset_period")

    if _has_column(insp, "rewards", "max_attributions"):
        op.drop_column("rewards", "max_attributions")
