"""customer_rewards add idempotency_key

Revision ID: e7f8a9b0c1d2
Revises: d4e5f6a7b8c9
Create Date: 2026-03-04

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "e7f8a9b0c1d2"
down_revision: Union[str, Sequence[str], None] = "d4e5f6a7b8c9"
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

    if not _has_column(insp, "customer_rewards", "idempotency_key"):
        op.add_column(
            "customer_rewards",
            sa.Column("idempotency_key", sa.String(length=255), nullable=True),
        )

    existing_indexes = {ix["name"] for ix in insp.get_indexes("customer_rewards")}

    if "uq_customer_rewards_idempotency_key" not in existing_indexes:
        op.create_index(
            "uq_customer_rewards_idempotency_key",
            "customer_rewards",
            ["idempotency_key"],
            unique=True,
            postgresql_where=sa.text("idempotency_key IS NOT NULL"),
        )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("customer_rewards"):
        return

    existing_indexes = {ix["name"] for ix in insp.get_indexes("customer_rewards")}
    if "uq_customer_rewards_idempotency_key" in existing_indexes:
        op.drop_index("uq_customer_rewards_idempotency_key", table_name="customer_rewards")

    if _has_column(insp, "customer_rewards", "idempotency_key"):
        op.drop_column("customer_rewards", "idempotency_key")
