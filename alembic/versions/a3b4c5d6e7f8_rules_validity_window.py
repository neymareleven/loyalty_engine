"""rules validity window (valid_from / valid_until)

Revision ID: a3b4c5d6e7f8
Revises: e2f3a4b5c6d7
Create Date: 2026-07-26

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "a3b4c5d6e7f8"
down_revision: Union[str, Sequence[str], None] = "e2f3a4b5c6d7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table("rules"):
        return
    cols = {c["name"] for c in inspector.get_columns("rules")}
    if "valid_from" not in cols:
        op.add_column("rules", sa.Column("valid_from", sa.TIMESTAMP(), nullable=True))
    if "valid_until" not in cols:
        op.add_column("rules", sa.Column("valid_until", sa.TIMESTAMP(), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table("rules"):
        return
    cols = {c["name"] for c in inspector.get_columns("rules")}
    if "valid_until" in cols:
        op.drop_column("rules", "valid_until")
    if "valid_from" in cols:
        op.drop_column("rules", "valid_from")
