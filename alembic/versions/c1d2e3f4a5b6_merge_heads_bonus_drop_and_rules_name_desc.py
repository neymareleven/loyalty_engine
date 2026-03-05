"""merge heads: bonus/campaign drop + rules name/description

Revision ID: c1d2e3f4a5b6
Revises: b8c3d1f2a9e4, e0c8890dae68
Create Date: 2026-03-04

"""

from typing import Sequence, Union


# revision identifiers, used by Alembic.
revision: str = "c1d2e3f4a5b6"
down_revision: Union[str, Sequence[str], None] = ("b8c3d1f2a9e4", "e0c8890dae68")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
