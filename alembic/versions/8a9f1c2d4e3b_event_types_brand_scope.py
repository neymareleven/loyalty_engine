"""event types brand scope

Revision ID: 8a9f1c2d4e3b
Revises: 3b2a1b3c6e1a
Create Date: 2026-02-18

"""

from alembic import op
import sqlalchemy as sa


revision = "8a9f1c2d4e3b"
down_revision = "3b2a1b3c6e1a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    cols = {c["name"] for c in insp.get_columns("event_types")}
    if "brand" not in cols:
        op.add_column("event_types", sa.Column("brand", sa.String(length=50), nullable=True))

    for uc in insp.get_unique_constraints("event_types"):
        if uc.get("column_names") == ["key"]:
            op.drop_constraint(uc["name"], "event_types", type_="unique")

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


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    existing_indexes = {ix["name"] for ix in insp.get_indexes("event_types")}
    if "uq_event_types_global_key" in existing_indexes:
        op.drop_index("uq_event_types_global_key", table_name="event_types")
    if "uq_event_types_brand_key" in existing_indexes:
        op.drop_index("uq_event_types_brand_key", table_name="event_types")

    ucs = {tuple(uc.get("column_names") or []): uc.get("name") for uc in insp.get_unique_constraints("event_types")}
    if ("key",) not in ucs:
        op.create_unique_constraint("event_types_key_key", "event_types", ["key"])

    cols = {c["name"] for c in insp.get_columns("event_types")}
    if "brand" in cols:
        op.drop_column("event_types", "brand")
