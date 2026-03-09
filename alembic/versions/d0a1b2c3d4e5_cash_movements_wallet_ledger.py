"""cash movements wallet ledger

Revision ID: d0a1b2c3d4e5
Revises: c4a6f1b2d3e4
Create Date: 2026-03-06

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d0a1b2c3d4e5"
down_revision: Union[str, Sequence[str], None] = "c4a6f1b2d3e4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if insp.has_table("cash_movements"):
        return

    op.create_table(
        "cash_movements",
        sa.Column("id", sa.dialects.postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("customer_id", sa.dialects.postgresql.UUID(as_uuid=True), sa.ForeignKey("customers.id"), nullable=False),
        sa.Column("amount", sa.Integer(), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("type", sa.String(length=20), nullable=False),
        sa.Column(
            "source_transaction_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            sa.ForeignKey("transactions.id"),
            nullable=True,
        ),
        sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
    )

    op.create_index("ix_cash_movements_customer_id", "cash_movements", ["customer_id"], unique=False)
    op.create_index("ix_cash_movements_customer_currency", "cash_movements", ["customer_id", "currency"], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("cash_movements"):
        return

    op.drop_index("ix_cash_movements_customer_currency", table_name="cash_movements")
    op.drop_index("ix_cash_movements_customer_id", table_name="cash_movements")
    op.drop_table("cash_movements")
