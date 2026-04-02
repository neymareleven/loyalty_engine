"""drop customers.lifetime_points

Revision ID: 4a1b2c3d4e5f
Revises: 3c9d7a1e2f4b
Create Date: 2026-04-01

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "4a1b2c3d4e5f"
down_revision: Union[str, Sequence[str], None] = "3c9d7a1e2f4b"
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

    if not insp.has_table("customers"):
        return

    if _has_column(insp, "customers", "lifetime_points"):
        op.drop_column("customers", "lifetime_points")


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("customers"):
        return

    if not _has_column(insp, "customers", "lifetime_points"):
        op.add_column("customers", sa.Column("lifetime_points", sa.Integer(), nullable=True))
