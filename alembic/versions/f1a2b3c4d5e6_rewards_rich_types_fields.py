"""rewards rich types fields

Revision ID: f1a2b3c4d5e6
Revises: e7f8a9b0c1d2
Create Date: 2026-03-04

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f1a2b3c4d5e6"
down_revision: Union[str, Sequence[str], None] = "e7f8a9b0c1d2"
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

    if insp.has_table("rewards"):
        if not _has_column(insp, "rewards", "currency"):
            op.add_column("rewards", sa.Column("currency", sa.String(length=3), nullable=True))
        if not _has_column(insp, "rewards", "value_amount"):
            op.add_column("rewards", sa.Column("value_amount", sa.Integer(), nullable=True))
        if not _has_column(insp, "rewards", "value_percent"):
            op.add_column("rewards", sa.Column("value_percent", sa.Integer(), nullable=True))
        if not _has_column(insp, "rewards", "params"):
            op.add_column("rewards", sa.Column("params", sa.JSON(), nullable=True))

    if insp.has_table("customer_rewards"):
        if not _has_column(insp, "customer_rewards", "payload"):
            op.add_column("customer_rewards", sa.Column("payload", sa.JSON(), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if insp.has_table("customer_rewards"):
        if _has_column(insp, "customer_rewards", "payload"):
            op.drop_column("customer_rewards", "payload")

    if insp.has_table("rewards"):
        if _has_column(insp, "rewards", "params"):
            op.drop_column("rewards", "params")
        if _has_column(insp, "rewards", "value_percent"):
            op.drop_column("rewards", "value_percent")
        if _has_column(insp, "rewards", "value_amount"):
            op.drop_column("rewards", "value_amount")
        if _has_column(insp, "rewards", "currency"):
            op.drop_column("rewards", "currency")
