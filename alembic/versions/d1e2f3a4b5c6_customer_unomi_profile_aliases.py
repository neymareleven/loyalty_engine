"""customer unomi profile aliases (master vs session/merge sources)

Revision ID: d1e2f3a4b5c6
Revises: c0d1e2f3a4b5
Create Date: 2026-05-28

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "d1e2f3a4b5c6"
down_revision = "c0d1e2f3a4b5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if "customer_unomi_profile_aliases" in insp.get_table_names():
        return

    op.create_table(
        "customer_unomi_profile_aliases",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("brand", sa.String(length=50), nullable=False),
        sa.Column("customer_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("profile_id", sa.String(length=100), nullable=False),
        sa.Column("source", sa.String(length=30), nullable=False, server_default="session"),
        sa.Column("first_seen_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=False),
        sa.Column("last_seen_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["customer_id"],
            ["customers.id"],
            name="fk_customer_unomi_aliases_customer_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("brand", "profile_id", name="uq_customer_unomi_aliases_brand_profile_id"),
    )
    op.create_index(
        "ix_customer_unomi_aliases_customer_id",
        "customer_unomi_profile_aliases",
        ["customer_id"],
        unique=False,
    )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if "customer_unomi_profile_aliases" not in insp.get_table_names():
        return
    op.drop_index("ix_customer_unomi_aliases_customer_id", table_name="customer_unomi_profile_aliases")
    op.drop_table("customer_unomi_profile_aliases")
