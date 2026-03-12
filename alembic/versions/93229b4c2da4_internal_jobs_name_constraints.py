"""internal jobs name constraints

Revision ID: 93229b4c2da4
Revises: b0e829065bb9
Create Date: 2026-03-11 15:58:32.121295

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '93229b4c2da4'
down_revision: Union[str, Sequence[str], None] = 'b0e829065bb9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    insp = sa.inspect(bind)

    cols = {c["name"] for c in insp.get_columns("internal_jobs")}
    if "name" in cols:
        # Backfill missing names from job_key
        op.execute(
            sa.text(
                """
                update internal_jobs
                set name = job_key
                where name is null or btrim(name) = ''
                """
            )
        )

        # Deduplicate existing (brand, name) collisions before enforcing uniqueness.
        # Keeps the first row per (brand, name) and appends a suffix to others.
        op.execute(
            sa.text(
                """
                with ranked as (
                    select
                        id,
                        brand,
                        name,
                        row_number() over (
                            partition by brand, name
                            order by created_at asc, id asc
                        ) as rn
                    from internal_jobs
                    where name is not null and btrim(name) <> ''
                )
                update internal_jobs j
                set name = ranked.name || '_' || ranked.rn::text
                from ranked
                where j.id = ranked.id and ranked.rn > 1
                """
            )
        )

        # Enforce NOT NULL
        op.alter_column("internal_jobs", "name", existing_type=sa.String(length=200), nullable=False)

    # Uniqueness (brand, name)
    existing_indexes = {ix["name"] for ix in insp.get_indexes("internal_jobs")}
    if "uq_internal_jobs_brand_name" not in existing_indexes:
        op.create_index(
            "uq_internal_jobs_brand_name",
            "internal_jobs",
            ["brand", "name"],
            unique=True,
        )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    insp = sa.inspect(bind)

    existing_indexes = {ix["name"] for ix in insp.get_indexes("internal_jobs")}
    if "uq_internal_jobs_brand_name" in existing_indexes:
        op.drop_index("uq_internal_jobs_brand_name", table_name="internal_jobs")

    cols = {c["name"] for c in insp.get_columns("internal_jobs")}
    if "name" in cols:
        op.alter_column("internal_jobs", "name", existing_type=sa.String(length=200), nullable=True)
