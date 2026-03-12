"""merge heads

Revision ID: 77aaff35429b
Revises: d0a1b2c3d4e5, d4e8f2a1c9b1
Create Date: 2026-03-11 14:28:38.926596

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '77aaff35429b'
down_revision: Union[str, Sequence[str], None] = ('d0a1b2c3d4e5', 'd4e8f2a1c9b1')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
