"""internal jobs segment targeting

Revision ID: d6e7f8a9b0c1
Revises: c4d5e6f7a8b9
Create Date: 2026-04-28

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "d6e7f8a9b0c1"
down_revision: Union[str, Sequence[str], None] = "c4d5e6f7a8b9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    cols = {c["name"] for c in insp.get_columns("internal_jobs")}
    if "segment_id" not in cols:
        op.add_column(
            "internal_jobs",
            sa.Column("segment_id", postgresql.UUID(as_uuid=True), nullable=True),
        )
        op.create_index("ix_internal_jobs_segment_id", "internal_jobs", ["segment_id"], unique=False)
        op.create_foreign_key(
            "fk_internal_jobs_segment_id",
            "internal_jobs",
            "segments",
            ["segment_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    cols = {c["name"] for c in insp.get_columns("internal_jobs")}
    if "segment_id" in cols:
        op.drop_constraint("fk_internal_jobs_segment_id", "internal_jobs", type_="foreignkey")
        op.drop_index("ix_internal_jobs_segment_id", table_name="internal_jobs")
        op.drop_column("internal_jobs", "segment_id")
