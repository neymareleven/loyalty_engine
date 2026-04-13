"""merge heads (coupons + rules multi transaction types)

Revision ID: 00e584102a28
Revises: 6c7d8e9f0a1b, f8c9d0e1a2b3
Create Date: 2026-04-13 13:00:15.204731

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '00e584102a28'
down_revision: Union[str, Sequence[str], None] = ('6c7d8e9f0a1b', 'f8c9d0e1a2b3')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
