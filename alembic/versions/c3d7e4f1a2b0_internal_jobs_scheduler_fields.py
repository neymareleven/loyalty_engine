"""internal jobs scheduler fields

Revision ID: c3d7e4f1a2b0
Revises: b1c4d2e9f0aa
Create Date: 2026-02-18

"""

from alembic import op
import sqlalchemy as sa


revision = "c3d7e4f1a2b0"
down_revision = "b1c4d2e9f0aa"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    cols = {c["name"] for c in insp.get_columns("internal_jobs")}

    if "next_run_at" not in cols:
        op.add_column("internal_jobs", sa.Column("next_run_at", sa.TIMESTAMP(), nullable=True))
    if "last_run_at" not in cols:
        op.add_column("internal_jobs", sa.Column("last_run_at", sa.TIMESTAMP(), nullable=True))
    if "locked_at" not in cols:
        op.add_column("internal_jobs", sa.Column("locked_at", sa.TIMESTAMP(), nullable=True))
    if "locked_by" not in cols:
        op.add_column("internal_jobs", sa.Column("locked_by", sa.String(length=100), nullable=True))
    if "last_status" not in cols:
        op.add_column("internal_jobs", sa.Column("last_status", sa.String(length=20), nullable=True))
    if "last_error" not in cols:
        op.add_column("internal_jobs", sa.Column("last_error", sa.String(length=2000), nullable=True))

    existing_indexes = {ix["name"] for ix in insp.get_indexes("internal_jobs")}
    if "ix_internal_jobs_next_run_at" not in existing_indexes:
        op.create_index("ix_internal_jobs_next_run_at", "internal_jobs", ["next_run_at"], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    existing_indexes = {ix["name"] for ix in insp.get_indexes("internal_jobs")}
    if "ix_internal_jobs_next_run_at" in existing_indexes:
        op.drop_index("ix_internal_jobs_next_run_at", table_name="internal_jobs")

    cols = {c["name"] for c in insp.get_columns("internal_jobs")}

    if "last_error" in cols:
        op.drop_column("internal_jobs", "last_error")
    if "last_status" in cols:
        op.drop_column("internal_jobs", "last_status")
    if "locked_by" in cols:
        op.drop_column("internal_jobs", "locked_by")
    if "locked_at" in cols:
        op.drop_column("internal_jobs", "locked_at")
    if "last_run_at" in cols:
        op.drop_column("internal_jobs", "last_run_at")
    if "next_run_at" in cols:
        op.drop_column("internal_jobs", "next_run_at")
