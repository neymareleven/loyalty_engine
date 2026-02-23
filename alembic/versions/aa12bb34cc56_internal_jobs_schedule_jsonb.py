"""internal_jobs schedule jsonb

Revision ID: aa12bb34cc56
Revises: 9c1a2b3d4e5f
Create Date: 2026-02-23

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "aa12bb34cc56"
down_revision: Union[str, Sequence[str], None] = "9c1a2b3d4e5f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table("internal_jobs"):
        return

    cols = {c["name"] for c in inspector.get_columns("internal_jobs")}
    if "schedule" not in cols:
        return

    # Convert schedule from VARCHAR to JSONB.
    # Legacy values like "3600" become JSON string values ("3600").
    op.alter_column(
        "internal_jobs",
        "schedule",
        existing_type=sa.String(length=50),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=True,
        postgresql_using="to_jsonb(schedule)",
    )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table("internal_jobs"):
        return

    cols = {c["name"] for c in inspector.get_columns("internal_jobs")}
    if "schedule" not in cols:
        return

    # Best-effort: convert JSONB back to a string.
    # For JSON strings, schedule::text yields quoted value; we trim quotes.
    op.alter_column(
        "internal_jobs",
        "schedule",
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=sa.String(length=50),
        existing_nullable=True,
        postgresql_using="trim(both '" + "\"" + "' from schedule::text)",
    )
