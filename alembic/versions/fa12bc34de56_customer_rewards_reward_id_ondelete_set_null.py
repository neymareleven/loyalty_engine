"""customer_rewards reward_id nullable + ondelete SET NULL

Revision ID: fa12bc34de56
Revises: e9a1b2c3d4e5
Create Date: 2026-05-05

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "fa12bc34de56"
down_revision: Union[str, Sequence[str], None] = "e9a1b2c3d4e5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_column(insp, table_name: str, column_name: str) -> bool:
    try:
        cols = insp.get_columns(table_name)
    except Exception:
        return False
    return any(c.get("name") == column_name for c in cols)


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

    if not insp.has_table("customer_rewards"):
        return

    if not _has_column(insp, "customer_rewards", "reward_id"):
        return

    fk_name = _find_fk_name(
        insp,
        table_name="customer_rewards",
        constrained_columns=["reward_id"],
        referred_table="rewards",
    )

    with op.batch_alter_table("customer_rewards") as batch_op:
        # Make nullable
        batch_op.alter_column("reward_id", existing_type=sa.dialects.postgresql.UUID(as_uuid=True), nullable=True)

        # Replace FK to allow deleting rewards while preserving customer_rewards history.
        if fk_name:
            batch_op.drop_constraint(fk_name, type_="foreignkey")

        batch_op.create_foreign_key(
            "fk_customer_rewards_reward_id_rewards",
            "rewards",
            ["reward_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("customer_rewards"):
        return

    if not _has_column(insp, "customer_rewards", "reward_id"):
        return

    # Best-effort: downgrade requires reward_id NOT NULL; delete orphaned history rows.
    op.execute("DELETE FROM customer_rewards WHERE reward_id IS NULL")

    fk_name = _find_fk_name(
        insp,
        table_name="customer_rewards",
        constrained_columns=["reward_id"],
        referred_table="rewards",
    )

    with op.batch_alter_table("customer_rewards") as batch_op:
        if fk_name:
            batch_op.drop_constraint(fk_name, type_="foreignkey")

        batch_op.create_foreign_key(
            "fk_customer_rewards_reward_id_rewards",
            "rewards",
            ["reward_id"],
            ["id"],
        )

        batch_op.alter_column("reward_id", existing_type=sa.dialects.postgresql.UUID(as_uuid=True), nullable=False)
