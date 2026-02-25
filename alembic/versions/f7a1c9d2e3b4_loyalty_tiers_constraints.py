"""loyalty tiers constraints

Revision ID: f7a1c9d2e3b4
Revises: aa12bb34cc56
Create Date: 2026-02-25

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f7a1c9d2e3b4"
down_revision: Union[str, Sequence[str], None] = "aa12bb34cc56"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("loyalty_tiers"):
        return

    existing_indexes = {ix["name"] for ix in insp.get_indexes("loyalty_tiers")}

    if "uq_loyalty_tiers_brand_rank" not in existing_indexes:
        op.create_index(
            "uq_loyalty_tiers_brand_rank",
            "loyalty_tiers",
            ["brand", "rank"],
            unique=True,
        )

    if "uq_loyalty_tiers_brand_min_status_points" not in existing_indexes:
        op.create_index(
            "uq_loyalty_tiers_brand_min_status_points",
            "loyalty_tiers",
            ["brand", "min_status_points"],
            unique=True,
        )

    existing_checks = {ck.get("name") for ck in insp.get_check_constraints("loyalty_tiers")}

    if "ck_loyalty_tiers_rank_non_negative" not in existing_checks:
        op.create_check_constraint(
            "ck_loyalty_tiers_rank_non_negative",
            "loyalty_tiers",
            "rank >= 0",
        )

    if "ck_loyalty_tiers_min_status_points_non_negative" not in existing_checks:
        op.create_check_constraint(
            "ck_loyalty_tiers_min_status_points_non_negative",
            "loyalty_tiers",
            "min_status_points >= 0",
        )

    if "ck_loyalty_tiers_rank0_min0" not in existing_checks:
        op.create_check_constraint(
            "ck_loyalty_tiers_rank0_min0",
            "loyalty_tiers",
            "rank <> 0 OR min_status_points = 0",
        )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("loyalty_tiers"):
        return

    existing_indexes = {ix["name"] for ix in insp.get_indexes("loyalty_tiers")}
    if "uq_loyalty_tiers_brand_min_status_points" in existing_indexes:
        op.drop_index("uq_loyalty_tiers_brand_min_status_points", table_name="loyalty_tiers")
    if "uq_loyalty_tiers_brand_rank" in existing_indexes:
        op.drop_index("uq_loyalty_tiers_brand_rank", table_name="loyalty_tiers")

    existing_checks = {ck.get("name") for ck in insp.get_check_constraints("loyalty_tiers")}
    if "ck_loyalty_tiers_rank0_min0" in existing_checks:
        op.drop_constraint("ck_loyalty_tiers_rank0_min0", "loyalty_tiers", type_="check")
    if "ck_loyalty_tiers_min_status_points_non_negative" in existing_checks:
        op.drop_constraint("ck_loyalty_tiers_min_status_points_non_negative", "loyalty_tiers", type_="check")
    if "ck_loyalty_tiers_rank_non_negative" in existing_checks:
        op.drop_constraint("ck_loyalty_tiers_rank_non_negative", "loyalty_tiers", type_="check")
