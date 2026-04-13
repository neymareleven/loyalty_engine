"""rules multi transaction types (ANY/OR)

Revision ID: f8c9d0e1a2b3
Revises: f7a1c9d2e3b4
Create Date: 2026-04-13

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "f8c9d0e1a2b3"
down_revision: Union[str, Sequence[str], None] = "f7a1c9d2e3b4"
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

    if not insp.has_table("rules"):
        return

    # Add new column: transaction_types (array of text)
    if not _has_column(insp, "rules", "transaction_types"):
        op.add_column(
            "rules",
            sa.Column(
                "transaction_types",
                postgresql.ARRAY(sa.String(length=50)),
                nullable=True,
            ),
        )

    # Backfill from legacy column transaction_type -> transaction_types
    cols = {c["name"] for c in insp.get_columns("rules")}
    if "transaction_type" in cols and "transaction_types" in cols:
        op.execute(
            sa.text(
                "UPDATE rules SET transaction_types = ARRAY[transaction_type] "
                "WHERE transaction_types IS NULL AND transaction_type IS NOT NULL"
            )
        )

    # Ensure non-null semantics for new logic (best-effort). We keep nullable at DB-level
    # for compatibility across environments, and enforce semantics at API/service layer.


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("rules"):
        return

    if _has_column(insp, "rules", "transaction_types"):
        op.drop_column("rules", "transaction_types")
