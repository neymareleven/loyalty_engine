"""coupon_type_rewards junction table

Revision ID: fc45de67ab89
Revises: fb34cd56ef78
Create Date: 2026-05-22

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "fc45de67ab89"
down_revision: Union[str, Sequence[str], None] = "fb34cd56ef78"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if not insp.has_table("coupon_types") or not insp.has_table("rewards"):
        return

    if not insp.has_table("coupon_type_rewards"):
        op.create_table(
            "coupon_type_rewards",
            sa.Column("coupon_type_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("reward_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=False),
            sa.ForeignKeyConstraint(
                ["coupon_type_id"],
                ["coupon_types.id"],
                name="fk_coupon_type_rewards_coupon_type_id",
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                ["reward_id"],
                ["rewards.id"],
                name="fk_coupon_type_rewards_reward_id",
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("coupon_type_id", "reward_id", name="pk_coupon_type_rewards"),
            sa.UniqueConstraint(
                "coupon_type_id",
                "reward_id",
                name="uq_coupon_type_rewards_coupon_reward",
            ),
        )

    # Backfill explicit links from reward_category for existing coupon types.
    cols_ct = {c["name"] for c in insp.get_columns("coupon_types")}
    cols_r = {c["name"] for c in insp.get_columns("rewards")}
    if "reward_category_id" in cols_ct and "reward_category_id" in cols_r:
        op.execute(
            """
            INSERT INTO coupon_type_rewards (coupon_type_id, reward_id)
            SELECT ct.id, r.id
            FROM coupon_types ct
            JOIN rewards r
              ON r.reward_category_id = ct.reward_category_id
             AND r.brand = ct.brand
            WHERE ct.reward_category_id IS NOT NULL
            ON CONFLICT DO NOTHING
            """
        )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if insp.has_table("coupon_type_rewards"):
        op.drop_table("coupon_type_rewards")
