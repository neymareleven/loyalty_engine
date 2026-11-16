"""storefront benefits, coupon codes, redemptions, api clients

Revision ID: f8a9b0c1d2e4
Revises: e7f8a9b0c1d3
Create Date: 2026-11-16

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "f8a9b0c1d2e4"
down_revision: Union[str, Sequence[str], None] = "e7f8a9b0c1d3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


def _add_col(table: str, column: sa.Column, existing: set[str]) -> None:
    if column.name not in existing:
        op.add_column(table, column)


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    settings_cols = {c["name"] for c in inspector.get_columns("brand_loyalty_settings")} if inspector.has_table("brand_loyalty_settings") else set()
    _add_col("brand_loyalty_settings", sa.Column("max_discount_benefits_per_cart", sa.Integer(), nullable=False, server_default="1"), settings_cols)
    _add_col("brand_loyalty_settings", sa.Column("discount_base", sa.String(length=30), nullable=False, server_default="SUBTOTAL_INCL_TAX"), settings_cols)

    reward_cols = {c["name"] for c in inspector.get_columns("rewards")} if inspector.has_table("rewards") else set()
    _add_col("rewards", sa.Column("benefit_type", sa.String(length=30), nullable=False, server_default="CUSTOM"), reward_cols)
    _add_col("rewards", sa.Column("benefit_value", sa.Integer(), nullable=True), reward_cols)
    _add_col("rewards", sa.Column("max_discount_amount", sa.Integer(), nullable=True), reward_cols)
    _add_col("rewards", sa.Column("min_cart_amount", sa.Integer(), nullable=True), reward_cols)
    _add_col("rewards", sa.Column("applies_to", sa.String(length=20), nullable=False, server_default="CART"), reward_cols)
    _add_col("rewards", sa.Column("free_product_quantity", sa.Integer(), nullable=False, server_default="1"), reward_cols)
    _add_col("rewards", sa.Column("stackable", sa.Boolean(), nullable=False, server_default=sa.false()), reward_cols)

    coupon_cols = {c["name"] for c in inspector.get_columns("customer_coupons")} if inspector.has_table("customer_coupons") else set()
    _add_col("customer_coupons", sa.Column("code", sa.String(length=32), nullable=True), coupon_cols)
    _add_col("customer_coupons", sa.Column("shareable", sa.Boolean(), nullable=False, server_default=sa.false()), coupon_cols)
    _add_col("customer_coupons", sa.Column("reserved_until", sa.TIMESTAMP(), nullable=True), coupon_cols)
    _add_col("customer_coupons", sa.Column("reserved_by_redemption_id", postgresql.UUID(as_uuid=True), nullable=True), coupon_cols)

    cr_cols = {c["name"] for c in inspector.get_columns("customer_rewards")} if inspector.has_table("customer_rewards") else set()
    _add_col("customer_rewards", sa.Column("shareable", sa.Boolean(), nullable=False, server_default=sa.false()), cr_cols)
    _add_col("customer_rewards", sa.Column("reserved_until", sa.TIMESTAMP(), nullable=True), cr_cols)
    _add_col("customer_rewards", sa.Column("reserved_by_redemption_id", postgresql.UUID(as_uuid=True), nullable=True), cr_cols)

    if not inspector.has_table("api_clients"):
        op.create_table(
            "api_clients",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("brand", sa.String(length=50), nullable=False),
            sa.Column("name", sa.String(length=150), nullable=False),
            sa.Column("key_hash", sa.String(length=64), nullable=False),
            sa.Column("key_prefix", sa.String(length=16), nullable=False),
            sa.Column("scopes", sa.JSON(), nullable=False),
            sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("last_used_at", sa.TIMESTAMP(), nullable=True),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()")),
            sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.text("now()")),
            sa.UniqueConstraint("key_hash", name="uq_api_clients_key_hash"),
        )

    if not inspector.has_table("redemptions"):
        op.create_table(
            "redemptions",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("brand", sa.String(length=50), nullable=False),
            sa.Column("customer_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("customers.id"), nullable=False),
            sa.Column("customer_coupon_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("customer_coupons.id"), nullable=True),
            sa.Column("customer_reward_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("customer_rewards.id"), nullable=True),
            sa.Column("site_id", sa.String(length=100), nullable=True),
            sa.Column("cart_ref", sa.String(length=150), nullable=True),
            sa.Column("order_ref", sa.String(length=150), nullable=True),
            sa.Column("status", sa.String(length=20), nullable=False, server_default="RESERVED"),
            sa.Column("benefit", sa.JSON(), nullable=True),
            sa.Column("currency", sa.String(length=10), nullable=True),
            sa.Column("cart_snapshot", sa.JSON(), nullable=True),
            sa.Column("idempotency_key", sa.String(length=255), nullable=True),
            sa.Column("reserved_until", sa.TIMESTAMP(), nullable=True),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()")),
            sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.text("now()")),
            sa.UniqueConstraint("idempotency_key", name="uq_redemptions_idempotency_key"),
        )
        op.create_index("ix_redemptions_brand_status", "redemptions", ["brand", "status"])
        op.create_index("ix_redemptions_coupon", "redemptions", ["customer_coupon_id"])
        op.create_index("ix_redemptions_reserved_until", "redemptions", ["reserved_until"])

    # Backfill coupon codes without colliding.
    rows = bind.execute(sa.text("SELECT id FROM customer_coupons WHERE code IS NULL")).fetchall()
    used = {r[0] for r in bind.execute(sa.text("SELECT code FROM customer_coupons WHERE code IS NOT NULL")).fetchall()}
    import secrets

    def _code() -> str:
        chunk = lambda: "".join(secrets.choice(_ALPHABET) for _ in range(4))
        return f"LOY-{chunk()}-{chunk()}"

    for (coupon_id,) in rows:
        code = _code()
        while code in used:
            code = _code()
        used.add(code)
        bind.execute(
            sa.text("UPDATE customer_coupons SET code = :code WHERE id = :id"),
            {"code": code, "id": coupon_id},
        )

    coupon_indexes = {idx["name"] for idx in inspector.get_indexes("customer_coupons")} if inspector.has_table("customer_coupons") else set()
    if "uq_customer_coupons_code" not in coupon_indexes:
        op.create_index("uq_customer_coupons_code", "customer_coupons", ["code"], unique=True)
    if "ix_customer_coupons_code" not in coupon_indexes:
        op.create_index("ix_customer_coupons_code", "customer_coupons", ["code"])


def downgrade() -> None:
    op.drop_index("ix_customer_coupons_code", table_name="customer_coupons")
    op.drop_index("uq_customer_coupons_code", table_name="customer_coupons")
    op.drop_table("redemptions")
    op.drop_table("api_clients")
