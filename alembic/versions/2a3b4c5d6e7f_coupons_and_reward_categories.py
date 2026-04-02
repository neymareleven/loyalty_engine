"""coupons and reward categories

Revision ID: 2a3b4c5d6e7f
Revises: 1c2d3e4f5a6b
Create Date: 2026-03-31

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "2a3b4c5d6e7f"
down_revision: Union[str, Sequence[str], None] = "1c2d3e4f5a6b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_column(insp, table_name: str, column_name: str) -> bool:
    try:
        cols = insp.get_columns(table_name)
    except Exception:
        return False
    return any(c.get("name") == column_name for c in cols)


def _has_index(insp, table_name: str, index_name: str) -> bool:
    try:
        idx = insp.get_indexes(table_name)
    except Exception:
        return False
    return any(i.get("name") == index_name for i in idx)


def _has_unique_constraint(insp, table_name: str, constraint_name: str) -> bool:
    try:
        uqs = insp.get_unique_constraints(table_name)
    except Exception:
        return False
    return any((uq.get("name") == constraint_name) for uq in uqs)


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # 1) brand_loyalty_settings: coupon_validity_days
    if insp.has_table("brand_loyalty_settings"):
        if not _has_column(insp, "brand_loyalty_settings", "coupon_validity_days"):
            op.add_column(
                "brand_loyalty_settings",
                sa.Column("coupon_validity_days", sa.Integer(), nullable=True),
            )

    # 2) reward_categories
    if not insp.has_table("reward_categories"):
        op.create_table(
            "reward_categories",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column("brand", sa.String(length=50), nullable=False),
            sa.Column("code", sa.String(length=100), nullable=False),
            sa.Column("name", sa.String(length=200), nullable=False),
            sa.Column("description", sa.String(length=1000), nullable=True),
            sa.Column("active", sa.Boolean(), nullable=True, server_default=sa.text("true")),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.UniqueConstraint("brand", "code", name="uq_reward_categories_brand_code"),
        )

    # 3) coupon_types
    if not insp.has_table("coupon_types"):
        op.create_table(
            "coupon_types",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column("brand", sa.String(length=50), nullable=False),
            sa.Column("code", sa.String(length=100), nullable=False),
            sa.Column("name", sa.String(length=200), nullable=False),
            sa.Column("description", sa.String(length=1000), nullable=True),
            sa.Column(
                "reward_category_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("reward_categories.id", ondelete="RESTRICT"),
                nullable=False,
            ),
            sa.Column("active", sa.Boolean(), nullable=True, server_default=sa.text("true")),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.UniqueConstraint("brand", "code", name="uq_coupon_types_brand_code"),
        )

    # 4) customer_coupons
    if not insp.has_table("customer_coupons"):
        op.create_table(
            "customer_coupons",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column(
                "customer_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("customers.id"),
                nullable=False,
            ),
            sa.Column(
                "coupon_type_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("coupon_types.id"),
                nullable=False,
            ),
            sa.Column("calendar_year", sa.Integer(), nullable=False),
            sa.Column("status", sa.String(length=20), nullable=False, server_default=sa.text("'ISSUED'")),
            sa.Column("issued_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.Column("expires_at", sa.TIMESTAMP(), nullable=True),
            sa.Column("used_at", sa.TIMESTAMP(), nullable=True),
            sa.Column(
                "source_transaction_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("transactions.id"),
                nullable=True,
            ),
            sa.Column(
                "rule_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("rules.id", ondelete="SET NULL"),
                nullable=True,
            ),
            sa.Column(
                "rule_execution_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey("transaction_rule_execution.id", ondelete="SET NULL"),
                nullable=True,
            ),
            sa.Column("idempotency_key", sa.String(length=255), nullable=True),
            sa.Column("payload", sa.JSON(), nullable=True),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
            sa.UniqueConstraint(
                "customer_id",
                "coupon_type_id",
                "calendar_year",
                name="uq_customer_coupons_customer_coupon_type_year",
            ),
            sa.UniqueConstraint("idempotency_key", name="uq_customer_coupons_idempotency_key"),
        )

        if not _has_index(insp, "customer_coupons", "ix_customer_coupons_customer_id"):
            op.create_index("ix_customer_coupons_customer_id", "customer_coupons", ["customer_id"], unique=False)
        if not _has_index(insp, "customer_coupons", "ix_customer_coupons_coupon_type_id"):
            op.create_index("ix_customer_coupons_coupon_type_id", "customer_coupons", ["coupon_type_id"], unique=False)
        if not _has_index(insp, "customer_coupons", "ix_customer_coupons_status"):
            op.create_index("ix_customer_coupons_status", "customer_coupons", ["status"], unique=False)

    # 5) rewards: add reward_category_id
    if insp.has_table("rewards"):
        if not _has_column(insp, "rewards", "reward_category_id"):
            op.add_column(
                "rewards",
                sa.Column(
                    "reward_category_id",
                    postgresql.UUID(as_uuid=True),
                    nullable=True,
                ),
            )
            op.create_foreign_key(
                "fk_rewards_reward_category_id",
                "rewards",
                "reward_categories",
                ["reward_category_id"],
                ["id"],
                ondelete="RESTRICT",
            )
        if not _has_index(insp, "rewards", "ix_rewards_brand_reward_category_id"):
            op.create_index(
                "ix_rewards_brand_reward_category_id",
                "rewards",
                ["brand", "reward_category_id"],
                unique=False,
            )

    # 6) customer_rewards: add customer_coupon_id
    if insp.has_table("customer_rewards"):
        if not _has_column(insp, "customer_rewards", "customer_coupon_id"):
            op.add_column(
                "customer_rewards",
                sa.Column(
                    "customer_coupon_id",
                    postgresql.UUID(as_uuid=True),
                    nullable=True,
                ),
            )
            op.create_foreign_key(
                "fk_customer_rewards_customer_coupon_id",
                "customer_rewards",
                "customer_coupons",
                ["customer_coupon_id"],
                ["id"],
                ondelete="SET NULL",
            )
        if not _has_index(insp, "customer_rewards", "ix_customer_rewards_customer_coupon_id"):
            op.create_index(
                "ix_customer_rewards_customer_coupon_id",
                "customer_rewards",
                ["customer_coupon_id"],
                unique=False,
            )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # customer_rewards: drop FK/column/index
    if insp.has_table("customer_rewards"):
        if _has_index(insp, "customer_rewards", "ix_customer_rewards_customer_coupon_id"):
            op.drop_index("ix_customer_rewards_customer_coupon_id", table_name="customer_rewards")
        cols = {c["name"] for c in insp.get_columns("customer_rewards")}
        if "customer_coupon_id" in cols:
            # FK name could differ if DB was created differently; best-effort drop.
            try:
                op.drop_constraint("fk_customer_rewards_customer_coupon_id", "customer_rewards", type_="foreignkey")
            except Exception:
                pass
            op.drop_column("customer_rewards", "customer_coupon_id")

    # rewards: drop FK/column/index
    if insp.has_table("rewards"):
        if _has_index(insp, "rewards", "ix_rewards_brand_reward_category_id"):
            op.drop_index("ix_rewards_brand_reward_category_id", table_name="rewards")
        cols = {c["name"] for c in insp.get_columns("rewards")}
        if "reward_category_id" in cols:
            try:
                op.drop_constraint("fk_rewards_reward_category_id", "rewards", type_="foreignkey")
            except Exception:
                pass
            op.drop_column("rewards", "reward_category_id")

    # customer_coupons
    if insp.has_table("customer_coupons"):
        if _has_index(insp, "customer_coupons", "ix_customer_coupons_status"):
            op.drop_index("ix_customer_coupons_status", table_name="customer_coupons")
        if _has_index(insp, "customer_coupons", "ix_customer_coupons_coupon_type_id"):
            op.drop_index("ix_customer_coupons_coupon_type_id", table_name="customer_coupons")
        if _has_index(insp, "customer_coupons", "ix_customer_coupons_customer_id"):
            op.drop_index("ix_customer_coupons_customer_id", table_name="customer_coupons")
        op.drop_table("customer_coupons")

    # coupon_types
    if insp.has_table("coupon_types"):
        op.drop_table("coupon_types")

    # reward_categories
    if insp.has_table("reward_categories"):
        op.drop_table("reward_categories")

    # brand_loyalty_settings: coupon_validity_days
    if insp.has_table("brand_loyalty_settings"):
        cols = {c["name"] for c in insp.get_columns("brand_loyalty_settings")}
        if "coupon_validity_days" in cols:
            op.drop_column("brand_loyalty_settings", "coupon_validity_days")
