"""drop rewards.cost_points

Revision ID: 3c9d7a1e2f4b
Revises: 2a3b4c5d6e7f
Create Date: 2026-04-01

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "3c9d7a1e2f4b"
down_revision: Union[str, Sequence[str], None] = "2a3b4c5d6e7f"
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

    if _has_column(insp, "rewards", "cost_points"):
        op.drop_column("rewards", "cost_points")


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("rewards"):
        return

    if not _has_column(insp, "rewards", "cost_points"):
        op.add_column("rewards", sa.Column("cost_points", sa.Integer(), nullable=True))
