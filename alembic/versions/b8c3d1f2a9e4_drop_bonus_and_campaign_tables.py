"""drop bonus and campaign tables

Revision ID: b8c3d1f2a9e4
Revises: f7a1c9d2e3b4
Create Date: 2026-03-04

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b8c3d1f2a9e4"
down_revision: Union[str, Sequence[str], None] = "f7a1c9d2e3b4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # Drop in dependency order if FK exists.
    if insp.has_table("bonus_awards"):
        op.drop_table("bonus_awards")

    if insp.has_table("bonus_definitions"):
        op.drop_table("bonus_definitions")

    if insp.has_table("campaigns"):
        op.drop_table("campaigns")


def downgrade() -> None:
    # Irreversible: tables were removed as part of feature deletion.
    raise NotImplementedError("Downgrade not supported for dropped bonus/campaign tables")
