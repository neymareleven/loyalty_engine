"""rename event types to transaction types

Revision ID: b2c3d4e5f6a7
Revises: a9b8c7d6e5f4
Create Date: 2026-03-05

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, Sequence[str], None] = "a9b8c7d6e5f4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(insp, name: str) -> bool:
    try:
        return insp.has_table(name)
    except Exception:
        return False


def _has_column(insp, table: str, col: str) -> bool:
    try:
        cols = insp.get_columns(table)
    except Exception:
        return False
    return any(c.get("name") == col for c in cols)


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # 1) event_types -> transaction_types
    if _table_exists(insp, "event_types") and not _table_exists(insp, "transaction_types"):
        op.rename_table("event_types", "transaction_types")

    # Refresh inspector after rename
    insp = sa.inspect(bind)

    # 2) Rename indexes on transaction_types (best-effort, only if they exist)
    if _table_exists(insp, "transaction_types"):
        existing_indexes = {ix["name"] for ix in insp.get_indexes("transaction_types")}

        renames = {
            "uq_event_types_brand_key_origin": "uq_transaction_types_brand_key_origin",
            "uq_event_types_global_key_origin": "uq_transaction_types_global_key_origin",
            "uq_event_types_brand_key": "uq_transaction_types_brand_key",
            "uq_event_types_global_key": "uq_transaction_types_global_key",
        }
        for old, new in renames.items():
            if old in existing_indexes and new not in existing_indexes:
                op.execute(sa.text(f"ALTER INDEX {old} RENAME TO {new}"))

    # 3) transactions.event_type -> transactions.transaction_type
    if _table_exists(insp, "transactions") and _has_column(insp, "transactions", "event_type") and not _has_column(
        insp, "transactions", "transaction_type"
    ):
        op.alter_column("transactions", "event_type", new_column_name="transaction_type")

    # 4) rules.event_type -> rules.transaction_type
    if _table_exists(insp, "rules") and _has_column(insp, "rules", "event_type") and not _has_column(
        insp, "rules", "transaction_type"
    ):
        op.alter_column("rules", "event_type", new_column_name="transaction_type")


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # Reverse column renames first
    if _table_exists(insp, "transactions") and _has_column(insp, "transactions", "transaction_type") and not _has_column(
        insp, "transactions", "event_type"
    ):
        op.alter_column("transactions", "transaction_type", new_column_name="event_type")

    if _table_exists(insp, "rules") and _has_column(insp, "rules", "transaction_type") and not _has_column(
        insp, "rules", "event_type"
    ):
        op.alter_column("rules", "transaction_type", new_column_name="event_type")

    insp = sa.inspect(bind)

    # Rename indexes back (best-effort)
    if _table_exists(insp, "transaction_types"):
        existing_indexes = {ix["name"] for ix in insp.get_indexes("transaction_types")}

        renames = {
            "uq_transaction_types_brand_key_origin": "uq_event_types_brand_key_origin",
            "uq_transaction_types_global_key_origin": "uq_event_types_global_key_origin",
            "uq_transaction_types_brand_key": "uq_event_types_brand_key",
            "uq_transaction_types_global_key": "uq_event_types_global_key",
        }
        for old, new in renames.items():
            if old in existing_indexes and new not in existing_indexes:
                op.execute(sa.text(f"ALTER INDEX {old} RENAME TO {new}"))

    # transaction_types -> event_types
    if _table_exists(insp, "transaction_types") and not _table_exists(insp, "event_types"):
        op.rename_table("transaction_types", "event_types")
