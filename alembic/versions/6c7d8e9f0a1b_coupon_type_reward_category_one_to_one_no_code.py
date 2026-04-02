"""coupon type reward category one-to-one and remove code

Revision ID: 6c7d8e9f0a1b
Revises: 5b6c7d8e9f0a
Create Date: 2026-04-01

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "6c7d8e9f0a1b"
down_revision: Union[str, Sequence[str], None] = "5b6c7d8e9f0a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_column(insp, table_name: str, column_name: str) -> bool:
    try:
        cols = insp.get_columns(table_name)
    except Exception:
        return False
    return any(c.get("name") == column_name for c in cols)


def _drop_unique_if_exists(insp, table_name: str, name: str) -> None:
    try:
        uniques = insp.get_unique_constraints(table_name)
    except Exception:
        return
    if any(u.get("name") == name for u in uniques):
        op.drop_constraint(name, table_name, type_="unique")


def _drop_fk_to_table_if_exists(insp, table_name: str, referred_table: str, constrained_columns: list[str]) -> None:
    try:
        fks = insp.get_foreign_keys(table_name)
    except Exception:
        return
    for fk in fks:
        if fk.get("referred_table") != referred_table:
            continue
        if list(fk.get("constrained_columns") or []) != constrained_columns:
            continue
        name = fk.get("name")
        if name:
            op.drop_constraint(name, table_name, type_="foreignkey")


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # 1) reward_categories.coupon_type_id (nullable for backfill)
    if insp.has_table("reward_categories"):
        if not _has_column(insp, "reward_categories", "coupon_type_id"):
            op.add_column(
                "reward_categories",
                sa.Column("coupon_type_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=True),
            )

    # 2) Backfill coupon_type_id from legacy coupon_types.reward_category_id
    if insp.has_table("coupon_types") and insp.has_table("reward_categories"):
        if _has_column(insp, "coupon_types", "reward_category_id") and _has_column(insp, "reward_categories", "coupon_type_id"):
            op.execute(
                """
                UPDATE reward_categories rc
                SET coupon_type_id = ct.id
                FROM coupon_types ct
                WHERE ct.reward_category_id = rc.id
                """
            )

    # 3) Add FK + unique (1-1)
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if insp.has_table("reward_categories") and _has_column(insp, "reward_categories", "coupon_type_id"):
        _drop_fk_to_table_if_exists(insp, "reward_categories", "coupon_types", ["coupon_type_id"])
        op.create_foreign_key(
            "fk_reward_categories_coupon_type_id",
            "reward_categories",
            "coupon_types",
            ["coupon_type_id"],
            ["id"],
            ondelete="RESTRICT",
        )
        op.create_unique_constraint("uq_reward_categories_coupon_type_id", "reward_categories", ["coupon_type_id"])

        # Make NOT NULL after backfill
        op.alter_column("reward_categories", "coupon_type_id", existing_type=sa.dialects.postgresql.UUID(as_uuid=True), nullable=False)

    # 4) Drop legacy coupon_types.reward_category_id
    if insp.has_table("coupon_types") and _has_column(insp, "coupon_types", "reward_category_id"):
        _drop_fk_to_table_if_exists(insp, "coupon_types", "reward_categories", ["reward_category_id"])
        op.drop_column("coupon_types", "reward_category_id")

    # 5) Drop code columns + unique constraints based on code
    if insp.has_table("coupon_types"):
        _drop_unique_if_exists(insp, "coupon_types", "uq_coupon_types_brand_code")
        if _has_column(insp, "coupon_types", "code"):
            op.drop_column("coupon_types", "code")

    if insp.has_table("reward_categories"):
        _drop_unique_if_exists(insp, "reward_categories", "uq_reward_categories_brand_code")
        if _has_column(insp, "reward_categories", "code"):
            op.drop_column("reward_categories", "code")


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # Re-add code columns + uniques
    if insp.has_table("coupon_types"):
        if not _has_column(insp, "coupon_types", "code"):
            op.add_column("coupon_types", sa.Column("code", sa.String(length=100), nullable=True))
        op.create_unique_constraint("uq_coupon_types_brand_code", "coupon_types", ["brand", "code"])

    if insp.has_table("reward_categories"):
        if not _has_column(insp, "reward_categories", "code"):
            op.add_column("reward_categories", sa.Column("code", sa.String(length=100), nullable=True))
        op.create_unique_constraint("uq_reward_categories_brand_code", "reward_categories", ["brand", "code"])

    # Re-add legacy coupon_types.reward_category_id
    if insp.has_table("coupon_types") and not _has_column(insp, "coupon_types", "reward_category_id"):
        op.add_column(
            "coupon_types",
            sa.Column("reward_category_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=True),
        )
        op.create_foreign_key(
            "fk_coupon_types_reward_category_id",
            "coupon_types",
            "reward_categories",
            ["reward_category_id"],
            ["id"],
            ondelete="RESTRICT",
        )

    # Backfill reward_category_id from reward_categories.coupon_type_id
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if insp.has_table("coupon_types") and insp.has_table("reward_categories"):
        if _has_column(insp, "coupon_types", "reward_category_id") and _has_column(insp, "reward_categories", "coupon_type_id"):
            op.execute(
                """
                UPDATE coupon_types ct
                SET reward_category_id = rc.id
                FROM reward_categories rc
                WHERE rc.coupon_type_id = ct.id
                """
            )

    # Drop 1-1 FK/unique and column coupon_type_id
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if insp.has_table("reward_categories") and _has_column(insp, "reward_categories", "coupon_type_id"):
        _drop_unique_if_exists(insp, "reward_categories", "uq_reward_categories_coupon_type_id")
        _drop_fk_to_table_if_exists(insp, "reward_categories", "coupon_types", ["coupon_type_id"])
        op.drop_column("reward_categories", "coupon_type_id")
