"""unomi segmentation fields

Revision ID: a1b2c3d4e5f8
Revises: fe67ab89cd01
Create Date: 2026-05-25

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "a1b2c3d4e5f8"
down_revision = "fe67ab89cd01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "brand_loyalty_settings",
        sa.Column("segmentation_mode", sa.String(length=20), nullable=False, server_default="INTERNAL"),
    )
    op.add_column("brand_loyalty_settings", sa.Column("unomi_base_url", sa.String(length=500), nullable=True))
    op.add_column("brand_loyalty_settings", sa.Column("unomi_username", sa.String(length=255), nullable=True))
    op.add_column("brand_loyalty_settings", sa.Column("unomi_password", sa.String(length=255), nullable=True))
    op.add_column("brand_loyalty_settings", sa.Column("unomi_scope", sa.String(length=100), nullable=True))

    op.add_column(
        "segments",
        sa.Column("provider", sa.String(length=20), nullable=False, server_default="INTERNAL"),
    )
    op.add_column("segments", sa.Column("unomi_segment_id", sa.String(length=255), nullable=True))
    op.add_column("segments", sa.Column("unomi_scope", sa.String(length=100), nullable=True))
    op.add_column(
        "segments",
        sa.Column("manual_profile_ids", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "segments",
        sa.Column("unomi_condition", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_index(
        "ix_segments_brand_unomi_segment_id",
        "segments",
        ["brand", "unomi_segment_id"],
        unique=True,
        postgresql_where=sa.text("unomi_segment_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_segments_brand_unomi_segment_id", table_name="segments")
    op.drop_column("segments", "unomi_condition")
    op.drop_column("segments", "manual_profile_ids")
    op.drop_column("segments", "unomi_scope")
    op.drop_column("segments", "unomi_segment_id")
    op.drop_column("segments", "provider")
    op.drop_column("brand_loyalty_settings", "unomi_scope")
    op.drop_column("brand_loyalty_settings", "unomi_password")
    op.drop_column("brand_loyalty_settings", "unomi_username")
    op.drop_column("brand_loyalty_settings", "unomi_base_url")
    op.drop_column("brand_loyalty_settings", "segmentation_mode")
