"""add name and description to rules

Revision ID: e0c8890dae68
Revises: a8c4e2f1b9d0
Create Date: 2026-03-04 13:51:33.172936

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text


# revision identifiers, used by Alembic.
revision: str = 'e0c8890dae68'
down_revision: Union[str, Sequence[str], None] = 'a8c4e2f1b9d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table("rules"):
        return

    cols = {c["name"] for c in inspector.get_columns("rules")}

    if "name" not in cols:
        op.add_column("rules", sa.Column("name", sa.String(length=255), nullable=True))
        bind.execute(text("UPDATE rules SET name = event_type || ' rule' WHERE name IS NULL"))
        op.alter_column("rules", "name", existing_type=sa.String(length=255), nullable=False)

    if "description" not in cols:
        op.add_column("rules", sa.Column("description", sa.String(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table("rules"):
        return

    cols = {c["name"] for c in inspector.get_columns("rules")}

    if "description" in cols:
        op.drop_column("rules", "description")

    if "name" in cols:
        op.drop_column("rules", "name")
