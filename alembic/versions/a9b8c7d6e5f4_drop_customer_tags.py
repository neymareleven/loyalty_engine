"""drop customer_tags

Revision ID: a9b8c7d6e5f4
Revises: f1a2b3c4d5e6
Create Date: 2026-03-05

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a9b8c7d6e5f4"
down_revision: Union[str, Sequence[str], None] = "f1a2b3c4d5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    insp = sa.inspect(bind)
    try:
        return insp.has_table(table_name)
    except Exception:
        return False


def upgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, "customer_tags"):
        return

    insp = sa.inspect(bind)
    uqs = {uc["name"] for uc in insp.get_unique_constraints("customer_tags")}
    if "uq_customer_tags_customer_id_tag" in uqs:
        op.drop_constraint(
            "uq_customer_tags_customer_id_tag",
            "customer_tags",
            type_="unique",
        )

    op.drop_table("customer_tags")


def downgrade() -> None:
    raise RuntimeError("Irreversible migration")
