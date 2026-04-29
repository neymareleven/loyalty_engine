"""segments and rule segments

Revision ID: c4d5e6f7a8b9
Revises: b3c4d5e6f7a8
Create Date: 2026-04-28

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "c4d5e6f7a8b9"
down_revision: Union[str, Sequence[str], None] = "b3c4d5e6f7a8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("segments"):
        op.create_table(
            "segments",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column("brand", sa.String(length=50), nullable=False),
            sa.Column("name", sa.String(length=255), nullable=False),
            sa.Column("description", sa.String(length=1000), nullable=True),
            sa.Column("is_dynamic", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("conditions", sa.JSON(), nullable=True),
            sa.Column("active", sa.Boolean(), nullable=True, server_default=sa.text("true")),
            sa.Column("last_computed_at", sa.TIMESTAMP(), nullable=True),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.UniqueConstraint("brand", "name", name="uq_segments_brand_name"),
        )
        op.create_index("ix_segments_brand", "segments", ["brand"], unique=False)

    if not insp.has_table("segment_members"):
        op.create_table(
            "segment_members",
            sa.Column("segment_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("customer_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("source", sa.String(length=20), nullable=False, server_default="DYNAMIC"),
            sa.Column("computed_at", sa.TIMESTAMP(), nullable=True),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.ForeignKeyConstraint(["segment_id"], ["segments.id"], name="fk_segment_members_segment_id", ondelete="CASCADE"),
            sa.ForeignKeyConstraint(["customer_id"], ["customers.id"], name="fk_segment_members_customer_id", ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("segment_id", "customer_id"),
            sa.UniqueConstraint("segment_id", "customer_id", name="uq_segment_members_segment_customer"),
        )
        op.create_index("ix_segment_members_segment_id", "segment_members", ["segment_id"], unique=False)
        op.create_index("ix_segment_members_customer_id", "segment_members", ["customer_id"], unique=False)

    cols = {c["name"] for c in insp.get_columns("rules")}
    if "segment_ids" not in cols:
        op.add_column(
            "rules",
            sa.Column("segment_ids", postgresql.ARRAY(postgresql.UUID(as_uuid=True)), nullable=True),
        )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    cols = {c["name"] for c in insp.get_columns("rules")}
    if "segment_ids" in cols:
        op.drop_column("rules", "segment_ids")

    if insp.has_table("segment_members"):
        op.drop_index("ix_segment_members_customer_id", table_name="segment_members")
        op.drop_index("ix_segment_members_segment_id", table_name="segment_members")
        op.drop_table("segment_members")

    if insp.has_table("segments"):
        op.drop_index("ix_segments_brand", table_name="segments")
        op.drop_table("segments")
