"""customer coupons drop calendar_year + add issued_at index

Revision ID: 9d3a2b1c0f4e
Revises: 8c2a1f0d9e4b
Create Date: 2026-04-21

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "9d3a2b1c0f4e"
down_revision: Union[str, Sequence[str], None] = "8c2a1f0d9e4b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_column(insp, table_name: str, column_name: str) -> bool:
    try:
        cols = insp.get_columns(table_name)
    except Exception:
        return False
    return any(c.get("name") == column_name for c in cols)


def _index_exists(insp, table_name: str, index_name: str) -> bool:
    try:
        idxs = insp.get_indexes(table_name)
    except Exception:
        return False
    return any(i.get("name") == index_name for i in idxs)


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("customer_coupons"):
        return

    # Drop legacy unique constraint (customer_id, coupon_type_id, calendar_year)
    # Best-effort: constraint might not exist in some environments.
    try:
        op.drop_constraint(
            "uq_customer_coupons_customer_coupon_type_year",
            "customer_coupons",
            type_="unique",
        )
    except Exception:
        pass

    # Drop calendar_year column
    if _has_column(insp, "customer_coupons", "calendar_year"):
        op.drop_column("customer_coupons", "calendar_year")

    # Add index to support rolling-year lookup: (customer_id, coupon_type_id, issued_at)
    index_name = "ix_customer_coupons_customer_coupon_type_issued_at"
    if not _index_exists(insp, "customer_coupons", index_name):
        op.create_index(
            index_name,
            "customer_coupons",
            ["customer_id", "coupon_type_id", "issued_at"],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not insp.has_table("customer_coupons"):
        return

    index_name = "ix_customer_coupons_customer_coupon_type_issued_at"
    if _index_exists(insp, "customer_coupons", index_name):
        op.drop_index(index_name, table_name="customer_coupons")

    # Re-add calendar_year column (nullable for downgrade safety) + recreate unique
    if not _has_column(insp, "customer_coupons", "calendar_year"):
        op.add_column("customer_coupons", sa.Column("calendar_year", sa.Integer(), nullable=True))

    try:
        op.create_unique_constraint(
            "uq_customer_coupons_customer_coupon_type_year",
            "customer_coupons",
            ["customer_id", "coupon_type_id", "calendar_year"],
        )
    except Exception:
        pass
