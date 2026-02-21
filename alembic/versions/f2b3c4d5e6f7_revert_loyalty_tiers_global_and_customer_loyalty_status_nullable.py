"""revert loyalty_tiers global support and allow customer loyalty_status null

Revision ID: f2b3c4d5e6f7
Revises: e1a2b3c4d5e6
Create Date: 2026-02-19 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f2b3c4d5e6f7"
down_revision: Union[str, Sequence[str], None] = "e1a2b3c4d5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    # -----------------------------
    # loyalty_tiers: brand-scoped only
    # -----------------------------
    if inspector.has_table("loyalty_tiers"):
        cols = {c["name"] for c in inspector.get_columns("loyalty_tiers")}
        if "brand" in cols:
            # Clean up any global rows that may exist from previous migration/tests
            op.execute(sa.text("DELETE FROM loyalty_tiers WHERE brand IS NULL"))

        existing_indexes = {ix["name"] for ix in inspector.get_indexes("loyalty_tiers")}
        if "ux_loyalty_tiers_key_global" in existing_indexes:
            op.drop_index("ux_loyalty_tiers_key_global", table_name="loyalty_tiers")
        if "ux_loyalty_tiers_brand_key_not_null" in existing_indexes:
            op.drop_index("ux_loyalty_tiers_brand_key_not_null", table_name="loyalty_tiers")

        existing_uqs = {uq["name"] for uq in inspector.get_unique_constraints("loyalty_tiers")}
        if "uq_loyalty_tiers_brand_key" not in existing_uqs:
            op.create_unique_constraint("uq_loyalty_tiers_brand_key", "loyalty_tiers", ["brand", "key"])

        if "brand" in cols:
            op.alter_column("loyalty_tiers", "brand", existing_type=sa.String(length=50), nullable=False)

    # -----------------------------
    # customers: allow loyalty_status to be NULL
    # -----------------------------
    if inspector.has_table("customers"):
        cols = {c["name"] for c in inspector.get_columns("customers")}
        if "loyalty_status" in cols:
            op.alter_column(
                "customers",
                "loyalty_status",
                existing_type=sa.String(length=20),
                nullable=True,
                server_default=None,
            )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    # customers: restore NOT NULL + default BRONZE (previous behavior)
    if inspector.has_table("customers"):
        cols = {c["name"] for c in inspector.get_columns("customers")}
        if "loyalty_status" in cols:
            op.execute(sa.text("UPDATE customers SET loyalty_status = 'BRONZE' WHERE loyalty_status IS NULL"))
            op.alter_column(
                "customers",
                "loyalty_status",
                existing_type=sa.String(length=20),
                nullable=False,
                server_default="BRONZE",
            )

    # loyalty_tiers: restore global support (brand nullable + partial unique indexes)
    if inspector.has_table("loyalty_tiers"):
        cols = {c["name"] for c in inspector.get_columns("loyalty_tiers")}
        if "brand" in cols:
            op.alter_column("loyalty_tiers", "brand", existing_type=sa.String(length=50), nullable=True)

        existing_uqs = {uq["name"] for uq in inspector.get_unique_constraints("loyalty_tiers")}
        if "uq_loyalty_tiers_brand_key" in existing_uqs:
            op.drop_constraint("uq_loyalty_tiers_brand_key", "loyalty_tiers", type_="unique")

        existing_indexes = {ix["name"] for ix in inspector.get_indexes("loyalty_tiers")}
        if "ux_loyalty_tiers_brand_key_not_null" not in existing_indexes:
            op.create_index(
                "ux_loyalty_tiers_brand_key_not_null",
                "loyalty_tiers",
                ["brand", "key"],
                unique=True,
                postgresql_where=sa.text("brand is not null"),
            )
        if "ux_loyalty_tiers_key_global" not in existing_indexes:
            op.create_index(
                "ux_loyalty_tiers_key_global",
                "loyalty_tiers",
                ["key"],
                unique=True,
                postgresql_where=sa.text("brand is null"),
            )
