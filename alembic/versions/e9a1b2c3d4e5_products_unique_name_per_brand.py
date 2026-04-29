"""products unique name per brand

Revision ID: e9a1b2c3d4e5
Revises: d6e7f8a9b0c1
Create Date: 2026-04-29

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "e9a1b2c3d4e5"
down_revision: Union[str, Sequence[str], None] = "d6e7f8a9b0c1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("products"):
        return

    existing = {uc.get("name") for uc in insp.get_unique_constraints("products")}
    if "uq_products_brand_name" not in existing:
        op.create_unique_constraint(
            "uq_products_brand_name",
            "products",
            ["brand", "name"],
        )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("products"):
        return

    existing = {uc.get("name") for uc in insp.get_unique_constraints("products")}
    if "uq_products_brand_name" in existing:
        op.drop_constraint("uq_products_brand_name", "products", type_="unique")
