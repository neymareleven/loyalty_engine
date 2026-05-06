"""coupon types link to reward categories (many-to-one)

Revision ID: fb34cd56ef78
Revises: fa12bc34de56
Create Date: 2026-05-05

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "fb34cd56ef78"
down_revision: Union[str, Sequence[str], None] = "fa12bc34de56"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_column(insp, table_name: str, column_name: str) -> bool:
    try:
        cols = insp.get_columns(table_name)
    except Exception:
        return False
    return any(c.get("name") == column_name for c in cols)


def _find_constraint_name(insp, table_name: str, constraint_name: str) -> str | None:
    try:
        uqs = insp.get_unique_constraints(table_name)
    except Exception:
        uqs = []
    for uq in uqs:
        if uq.get("name") == constraint_name:
            return uq.get("name")
    return None


def _find_fk_name(insp, *, table_name: str, constrained_columns: list[str], referred_table: str) -> str | None:
    try:
        fks = insp.get_foreign_keys(table_name)
    except Exception:
        return None
    for fk in fks:
        if fk.get("referred_table") != referred_table:
            continue
        if list(fk.get("constrained_columns") or []) != constrained_columns:
            continue
        return fk.get("name")
    return None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("coupon_types") or not insp.has_table("reward_categories"):
        return

    # 1) Add coupon_types.reward_category_id
    if not _has_column(insp, "coupon_types", "reward_category_id"):
        with op.batch_alter_table("coupon_types") as batch_op:
            batch_op.add_column(sa.Column("reward_category_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=True))
            batch_op.create_foreign_key(
                "fk_coupon_types_reward_category_id_reward_categories",
                "reward_categories",
                ["reward_category_id"],
                ["id"],
                ondelete="RESTRICT",
            )

    # 2) Backfill from legacy reward_categories.coupon_type_id if present
    insp = sa.inspect(bind)
    if _has_column(insp, "reward_categories", "coupon_type_id"):
        op.execute(
            """
            UPDATE coupon_types ct
            SET reward_category_id = rc.id
            FROM reward_categories rc
            WHERE rc.coupon_type_id = ct.id
              AND ct.reward_category_id IS NULL
            """
        )

        # 3) Drop legacy unique constraint and column
        uq_name = _find_constraint_name(insp, "reward_categories", "uq_reward_categories_coupon_type_id")
        fk_name = _find_fk_name(
            insp,
            table_name="reward_categories",
            constrained_columns=["coupon_type_id"],
            referred_table="coupon_types",
        )

        with op.batch_alter_table("reward_categories") as batch_op:
            if uq_name:
                batch_op.drop_constraint(uq_name, type_="unique")
            if fk_name:
                batch_op.drop_constraint(fk_name, type_="foreignkey")
            batch_op.drop_column("coupon_type_id")


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("coupon_types") or not insp.has_table("reward_categories"):
        return

    # Recreate legacy coupon_type_id on reward_categories (best-effort; picks an arbitrary coupon type if many share a category).
    if not _has_column(insp, "reward_categories", "coupon_type_id"):
        with op.batch_alter_table("reward_categories") as batch_op:
            batch_op.add_column(sa.Column("coupon_type_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=True))
            batch_op.create_foreign_key(
                "fk_reward_categories_coupon_type_id_coupon_types",
                "coupon_types",
                ["coupon_type_id"],
                ["id"],
                ondelete="RESTRICT",
            )
            batch_op.create_unique_constraint("uq_reward_categories_coupon_type_id", ["coupon_type_id"])

        # Backfill with an arbitrary mapping
        op.execute(
            """
            UPDATE reward_categories rc
            SET coupon_type_id = sub.ctid
            FROM (
                SELECT reward_category_id, MIN(id) AS ctid
                FROM coupon_types
                WHERE reward_category_id IS NOT NULL
                GROUP BY reward_category_id
            ) sub
            WHERE rc.id = sub.reward_category_id
            """
        )

    # Drop coupon_types.reward_category_id
    insp = sa.inspect(bind)
    if _has_column(insp, "coupon_types", "reward_category_id"):
        fk_name = _find_fk_name(
            insp,
            table_name="coupon_types",
            constrained_columns=["reward_category_id"],
            referred_table="reward_categories",
        )
        with op.batch_alter_table("coupon_types") as batch_op:
            if fk_name:
                batch_op.drop_constraint(fk_name, type_="foreignkey")
            batch_op.drop_column("reward_category_id")
