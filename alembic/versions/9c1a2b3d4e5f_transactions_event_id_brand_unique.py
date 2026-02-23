"""transactions event_id unique per brand

Revision ID: 9c1a2b3d4e5f
Revises: f2b3c4d5e6f7
Create Date: 2026-02-21

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "9c1a2b3d4e5f"
down_revision: Union[str, Sequence[str], None] = "f2b3c4d5e6f7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table("transactions"):
        return

    # Drop legacy unique constraint(s) / index on event_id (global uniqueness)
    for uq in inspector.get_unique_constraints("transactions"):
        cols = tuple(uq.get("column_names") or [])
        if cols == ("event_id",):
            op.drop_constraint(uq["name"], "transactions", type_="unique")

    for ix in inspector.get_indexes("transactions"):
        cols = tuple(ix.get("column_names") or [])
        if ix.get("unique") and cols == ("event_id",):
            op.drop_index(ix["name"], table_name="transactions")

    # Ensure composite unique exists on (brand, event_id)
    existing_uqs = {tuple((uq.get("column_names") or [])) for uq in inspector.get_unique_constraints("transactions")}
    if ("brand", "event_id") not in existing_uqs:
        op.create_unique_constraint(
            "uq_transactions_brand_event_id",
            "transactions",
            ["brand", "event_id"],
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table("transactions"):
        return

    existing_uqs = {uq.get("name") for uq in inspector.get_unique_constraints("transactions")}
    if "uq_transactions_brand_event_id" in existing_uqs:
        op.drop_constraint("uq_transactions_brand_event_id", "transactions", type_="unique")

    # Restore global uniqueness on event_id (best-effort)
    existing_uqs = {tuple((uq.get("column_names") or [])) for uq in inspector.get_unique_constraints("transactions")}
    if ("event_id",) not in existing_uqs:
        op.create_unique_constraint("uq_transactions_event_id", "transactions", ["event_id"])
