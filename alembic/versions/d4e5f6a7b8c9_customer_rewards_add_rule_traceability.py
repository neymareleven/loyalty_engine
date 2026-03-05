"""customer_rewards add rule traceability

Revision ID: d4e5f6a7b8c9
Revises: c1d2e3f4a5b6
Create Date: 2026-03-04

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d4e5f6a7b8c9"
down_revision: Union[str, Sequence[str], None] = "c1d2e3f4a5b6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_column(insp, table_name: str, column_name: str) -> bool:
    try:
        cols = insp.get_columns(table_name)
    except Exception:
        return False
    return any(c.get("name") == column_name for c in cols)


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("customer_rewards"):
        return

    if not _has_column(insp, "customer_rewards", "rule_id"):
        op.add_column(
            "customer_rewards",
            sa.Column("rule_id", sa.dialects.postgresql.UUID(as_uuid=True), sa.ForeignKey("rules.id", ondelete="SET NULL"), nullable=True),
        )
        op.create_index("ix_customer_rewards_rule_id", "customer_rewards", ["rule_id"])

    if not _has_column(insp, "customer_rewards", "rule_execution_id"):
        op.add_column(
            "customer_rewards",
            sa.Column(
                "rule_execution_id",
                sa.dialects.postgresql.UUID(as_uuid=True),
                sa.ForeignKey("transaction_rule_execution.id", ondelete="SET NULL"),
                nullable=True,
            ),
        )
        op.create_index("ix_customer_rewards_rule_execution_id", "customer_rewards", ["rule_execution_id"])


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("customer_rewards"):
        return

    existing_indexes = {ix["name"] for ix in insp.get_indexes("customer_rewards")}

    if "ix_customer_rewards_rule_execution_id" in existing_indexes:
        op.drop_index("ix_customer_rewards_rule_execution_id", table_name="customer_rewards")
    if "ix_customer_rewards_rule_id" in existing_indexes:
        op.drop_index("ix_customer_rewards_rule_id", table_name="customer_rewards")

    if _has_column(insp, "customer_rewards", "rule_execution_id"):
        op.drop_column("customer_rewards", "rule_execution_id")
    if _has_column(insp, "customer_rewards", "rule_id"):
        op.drop_column("customer_rewards", "rule_id")
