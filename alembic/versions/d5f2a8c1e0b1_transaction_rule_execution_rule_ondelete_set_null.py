"""transaction_rule_execution rule fk on delete set null

Revision ID: d5f2a8c1e0b1
Revises: c3d7e4f1a2b0
Create Date: 2026-02-19 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d5f2a8c1e0b1"
down_revision: Union[str, Sequence[str], None] = "c3d7e4f1a2b0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table("transaction_rule_execution"):
        return

    fk_list = inspector.get_foreign_keys("transaction_rule_execution")
    rule_fks = [fk for fk in fk_list if fk.get("referred_table") == "rules" and "rule_id" in (fk.get("constrained_columns") or [])]

    for fk in rule_fks:
        name = fk.get("name")
        if name:
            op.drop_constraint(name, "transaction_rule_execution", type_="foreignkey")

    cols = {c["name"] for c in inspector.get_columns("transaction_rule_execution")}
    if "rule_id" in cols:
        op.alter_column("transaction_rule_execution", "rule_id", existing_type=sa.dialects.postgresql.UUID(as_uuid=True), nullable=True)

    op.create_foreign_key(
        "fk_transaction_rule_execution_rule_id_rules",
        "transaction_rule_execution",
        "rules",
        ["rule_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table("transaction_rule_execution"):
        return

    fk_list = inspector.get_foreign_keys("transaction_rule_execution")
    for fk in fk_list:
        if fk.get("name") == "fk_transaction_rule_execution_rule_id_rules":
            op.drop_constraint("fk_transaction_rule_execution_rule_id_rules", "transaction_rule_execution", type_="foreignkey")

    cols = {c["name"] for c in inspector.get_columns("transaction_rule_execution")}
    if "rule_id" in cols:
        op.alter_column("transaction_rule_execution", "rule_id", existing_type=sa.dialects.postgresql.UUID(as_uuid=True), nullable=False)

    op.create_foreign_key(
        "transaction_rule_execution_rule_id_fkey",
        "transaction_rule_execution",
        "rules",
        ["rule_id"],
        ["id"],
    )
