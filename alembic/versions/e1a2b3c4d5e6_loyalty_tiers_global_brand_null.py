"""loyalty_tiers global brand null

Revision ID: e1a2b3c4d5e6
Revises: d5f2a8c1e0b1
Create Date: 2026-02-19 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "e1a2b3c4d5e6"
down_revision: Union[str, Sequence[str], None] = "d5f2a8c1e0b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table("loyalty_tiers"):
        return

    cols = {c["name"] for c in inspector.get_columns("loyalty_tiers")}
    if "brand" in cols:
        op.alter_column("loyalty_tiers", "brand", existing_type=sa.String(length=50), nullable=True)

    # Drop old uniqueness constraint (brand, key)
    existing_uqs = {uq["name"] for uq in inspector.get_unique_constraints("loyalty_tiers")}
    if "uq_loyalty_tiers_brand_key" in existing_uqs:
        op.drop_constraint("uq_loyalty_tiers_brand_key", "loyalty_tiers", type_="unique")

    existing_indexes = {ix["name"] for ix in inspector.get_indexes("loyalty_tiers")}

    # Unique for brand-specific tiers
    if "ux_loyalty_tiers_brand_key_not_null" not in existing_indexes:
        op.create_index(
            "ux_loyalty_tiers_brand_key_not_null",
            "loyalty_tiers",
            ["brand", "key"],
            unique=True,
            postgresql_where=sa.text("brand is not null"),
        )

    # Unique for global tiers (brand is null)
    if "ux_loyalty_tiers_key_global" not in existing_indexes:
        op.create_index(
            "ux_loyalty_tiers_key_global",
            "loyalty_tiers",
            ["key"],
            unique=True,
            postgresql_where=sa.text("brand is null"),
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table("loyalty_tiers"):
        return

    existing_indexes = {ix["name"] for ix in inspector.get_indexes("loyalty_tiers")}
    if "ux_loyalty_tiers_key_global" in existing_indexes:
        op.drop_index("ux_loyalty_tiers_key_global", table_name="loyalty_tiers")
    if "ux_loyalty_tiers_brand_key_not_null" in existing_indexes:
        op.drop_index("ux_loyalty_tiers_brand_key_not_null", table_name="loyalty_tiers")

    existing_uqs = {uq["name"] for uq in inspector.get_unique_constraints("loyalty_tiers")}
    if "uq_loyalty_tiers_brand_key" not in existing_uqs:
        op.create_unique_constraint("uq_loyalty_tiers_brand_key", "loyalty_tiers", ["brand", "key"])

    cols = {c["name"] for c in inspector.get_columns("loyalty_tiers")}
    if "brand" in cols:
        op.alter_column("loyalty_tiers", "brand", existing_type=sa.String(length=50), nullable=False)
