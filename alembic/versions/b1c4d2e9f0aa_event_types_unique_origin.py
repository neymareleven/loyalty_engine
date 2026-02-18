"""event types unique origin

Revision ID: b1c4d2e9f0aa
Revises: 8a9f1c2d4e3b
Create Date: 2026-02-18

"""

from alembic import op
import sqlalchemy as sa


revision = "b1c4d2e9f0aa"
down_revision = "8a9f1c2d4e3b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    existing_indexes = {ix["name"] for ix in insp.get_indexes("event_types")}

    if "uq_event_types_brand_key" in existing_indexes:
        op.drop_index("uq_event_types_brand_key", table_name="event_types")

    if "uq_event_types_global_key" in existing_indexes:
        op.drop_index("uq_event_types_global_key", table_name="event_types")

    existing_indexes = {ix["name"] for ix in insp.get_indexes("event_types")}

    if "uq_event_types_brand_key_origin" not in existing_indexes:
        op.create_index(
            "uq_event_types_brand_key_origin",
            "event_types",
            ["brand", "key", "origin"],
            unique=True,
        )

    if "uq_event_types_global_key_origin" not in existing_indexes:
        op.create_index(
            "uq_event_types_global_key_origin",
            "event_types",
            ["key", "origin"],
            unique=True,
            postgresql_where=sa.text("brand is null"),
        )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    existing_indexes = {ix["name"] for ix in insp.get_indexes("event_types")}

    if "uq_event_types_global_key_origin" in existing_indexes:
        op.drop_index("uq_event_types_global_key_origin", table_name="event_types")

    if "uq_event_types_brand_key_origin" in existing_indexes:
        op.drop_index("uq_event_types_brand_key_origin", table_name="event_types")

    existing_indexes = {ix["name"] for ix in insp.get_indexes("event_types")}

    if "uq_event_types_brand_key" not in existing_indexes:
        op.create_index(
            "uq_event_types_brand_key",
            "event_types",
            ["brand", "key"],
            unique=True,
        )

    if "uq_event_types_global_key" not in existing_indexes:
        op.create_index(
            "uq_event_types_global_key",
            "event_types",
            ["key"],
            unique=True,
            postgresql_where=sa.text("brand is null"),
        )
