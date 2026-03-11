"""customer partial birthdate columns

Revision ID: d4e8f2a1c9b1
Revises: c3d7e4f1a2b0
Create Date: 2026-03-11

"""

from alembic import op
import sqlalchemy as sa


revision = "d4e8f2a1c9b1"
down_revision = "c3d7e4f1a2b0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    cols = {c["name"] for c in insp.get_columns("customers")}

    if "birth_month" not in cols:
        op.add_column("customers", sa.Column("birth_month", sa.Integer(), nullable=True))
    if "birth_day" not in cols:
        op.add_column("customers", sa.Column("birth_day", sa.Integer(), nullable=True))
    if "birth_year" not in cols:
        op.add_column("customers", sa.Column("birth_year", sa.Integer(), nullable=True))

    # Backfill from existing full birthdate (DATE) if present
    cols = {c["name"] for c in insp.get_columns("customers")}
    if "birthdate" in cols:
        op.execute(
            sa.text(
                """
                update customers
                set
                    birth_year = extract(year from birthdate)::int,
                    birth_month = extract(month from birthdate)::int,
                    birth_day = extract(day from birthdate)::int
                where birthdate is not null
                """
            )
        )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    cols = {c["name"] for c in insp.get_columns("customers")}

    if "birth_year" in cols:
        op.drop_column("customers", "birth_year")
    if "birth_day" in cols:
        op.drop_column("customers", "birth_day")
    if "birth_month" in cols:
        op.drop_column("customers", "birth_month")
