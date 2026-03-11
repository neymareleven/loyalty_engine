"""internal jobs name description

Revision ID: b0e829065bb9
Revises: 77aaff35429b
Create Date: 2026-03-11 15:51:28.575734

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b0e829065bb9'
down_revision: Union[str, Sequence[str], None] = '77aaff35429b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    insp = sa.inspect(bind)

    cols = {c["name"] for c in insp.get_columns("internal_jobs")}

    if "name" not in cols:
        op.add_column("internal_jobs", sa.Column("name", sa.String(length=200), nullable=True))
    if "description" not in cols:
        op.add_column("internal_jobs", sa.Column("description", sa.String(length=1000), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    insp = sa.inspect(bind)

    cols = {c["name"] for c in insp.get_columns("internal_jobs")}

    if "description" in cols:
        op.drop_column("internal_jobs", "description")
    if "name" in cols:
        op.drop_column("internal_jobs", "name")
