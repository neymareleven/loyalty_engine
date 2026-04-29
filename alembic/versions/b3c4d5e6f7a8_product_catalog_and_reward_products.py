"""product catalog + reward products

Revision ID: b3c4d5e6f7a8
Revises: a1b2c3d4e5f7
Create Date: 2026-04-27

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "b3c4d5e6f7a8"
down_revision: Union[str, Sequence[str], None] = "a1b2c3d4e5f7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("product_categories"):
        op.create_table(
            "product_categories",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column("brand", sa.String(length=50), nullable=False),
            sa.Column("name", sa.String(length=200), nullable=False),
            sa.Column("description", sa.String(length=1000), nullable=True),
            sa.Column("active", sa.Boolean(), nullable=True, server_default=sa.text("true")),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.UniqueConstraint("brand", "name", name="uq_product_categories_brand_name"),
        )
        op.create_index(
            "ix_product_categories_brand",
            "product_categories",
            ["brand"],
            unique=False,
        )

    if not insp.has_table("products"):
        op.create_table(
            "products",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column("brand", sa.String(length=50), nullable=False),
            sa.Column("category_id", postgresql.UUID(as_uuid=True), nullable=True),
            sa.Column("name", sa.String(length=255), nullable=False),
            sa.Column("match_key", sa.String(length=255), nullable=False),
            sa.Column("points_value", sa.Integer(), nullable=True),
            sa.Column("active", sa.Boolean(), nullable=True, server_default=sa.text("true")),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.ForeignKeyConstraint(["category_id"], ["product_categories.id"], name="fk_products_category_id", ondelete="RESTRICT"),
            sa.UniqueConstraint("brand", "match_key", name="uq_products_brand_match_key"),
        )
        op.create_index(
            "ix_products_brand",
            "products",
            ["brand"],
            unique=False,
        )
        op.create_index(
            "ix_products_brand_match_key",
            "products",
            ["brand", "match_key"],
            unique=False,
        )

    if not insp.has_table("reward_products"):
        op.create_table(
            "reward_products",
            sa.Column("reward_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("product_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("quantity", sa.Integer(), nullable=False, server_default="1"),
            sa.ForeignKeyConstraint(["reward_id"], ["rewards.id"], name="fk_reward_products_reward_id", ondelete="CASCADE"),
            sa.ForeignKeyConstraint(["product_id"], ["products.id"], name="fk_reward_products_product_id", ondelete="RESTRICT"),
            sa.PrimaryKeyConstraint("reward_id", "product_id"),
            sa.UniqueConstraint("reward_id", "product_id", name="uq_reward_products_reward_product"),
        )
        op.create_index(
            "ix_reward_products_reward_id",
            "reward_products",
            ["reward_id"],
            unique=False,
        )
        op.create_index(
            "ix_reward_products_product_id",
            "reward_products",
            ["product_id"],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if insp.has_table("reward_products"):
        op.drop_index("ix_reward_products_product_id", table_name="reward_products")
        op.drop_index("ix_reward_products_reward_id", table_name="reward_products")
        op.drop_table("reward_products")

    if insp.has_table("products"):
        op.drop_index("ix_products_brand_match_key", table_name="products")
        op.drop_index("ix_products_brand", table_name="products")
        op.drop_table("products")

    if insp.has_table("product_categories"):
        op.drop_index("ix_product_categories_brand", table_name="product_categories")
        op.drop_table("product_categories")
