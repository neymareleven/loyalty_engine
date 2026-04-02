"""drop rewards effect fields

Revision ID: 5b6c7d8e9f0a
Revises: 4a1b2c3d4e5f
Create Date: 2026-04-01

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "5b6c7d8e9f0a"
down_revision: Union[str, Sequence[str], None] = "4a1b2c3d4e5f"
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

    for col in [
        "type",
        "validity_days",
        "max_attributions",
        "reset_period",
        "currency",
        "value_amount",
        "value_percent",
        "params",
    ]:
        if _has_column(insp, "rewards", col):
            op.drop_column("rewards", col)


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("rewards"):
        return

    if not _has_column(insp, "rewards", "type"):
        op.add_column(
            "rewards",
            sa.Column("type", sa.String(length=50), nullable=False, server_default="POINTS"),
        )

    if not _has_column(insp, "rewards", "validity_days"):
        op.add_column("rewards", sa.Column("validity_days", sa.Integer(), nullable=True))

    if not _has_column(insp, "rewards", "max_attributions"):
        op.add_column("rewards", sa.Column("max_attributions", sa.Integer(), nullable=True))

    if not _has_column(insp, "rewards", "reset_period"):
        op.add_column("rewards", sa.Column("reset_period", sa.String(length=20), nullable=True))

    if not _has_column(insp, "rewards", "currency"):
        op.add_column("rewards", sa.Column("currency", sa.String(length=3), nullable=True))

    if not _has_column(insp, "rewards", "value_amount"):
        op.add_column("rewards", sa.Column("value_amount", sa.Integer(), nullable=True))

    if not _has_column(insp, "rewards", "value_percent"):
        op.add_column("rewards", sa.Column("value_percent", sa.Integer(), nullable=True))

    if not _has_column(insp, "rewards", "params"):
        op.add_column("rewards", sa.Column("params", sa.JSON(), nullable=True))
