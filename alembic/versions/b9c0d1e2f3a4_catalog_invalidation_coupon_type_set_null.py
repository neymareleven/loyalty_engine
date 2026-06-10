"""customer_coupons coupon_type_id nullable + ON DELETE SET NULL

Revision ID: b9c0d1e2f3a4
Revises: a1b2c3d4e5f8
Create Date: 2026-05-28

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b9c0d1e2f3a4"
down_revision: Union[str, Sequence[str], None] = "a1b2c3d4e5f8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _find_fk_name(insp, *, table_name: str, constrained_columns: list[str], referred_table: str) -> str | None:
    try:
        fks = insp.get_foreign_keys(table_name)
    except Exception:
        return None
    for fk in fks:
        if fk.get("referred_table") != referred_table:
            continue
        if list(fk.get("constrained_columns") or []) != constrained_columns:
            continue
        return fk.get("name")
    return None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if not insp.has_table("customer_coupons"):
        return

    fk_name = _find_fk_name(
        insp,
        table_name="customer_coupons",
        constrained_columns=["coupon_type_id"],
        referred_table="coupon_types",
    )

    with op.batch_alter_table("customer_coupons") as batch_op:
        if fk_name:
            batch_op.drop_constraint(fk_name, type_="foreignkey")
        batch_op.alter_column(
            "coupon_type_id",
            existing_type=sa.dialects.postgresql.UUID(as_uuid=True),
            nullable=True,
        )
        batch_op.create_foreign_key(
            "fk_customer_coupons_coupon_type_id_coupon_types",
            "coupon_types",
            ["coupon_type_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if not insp.has_table("customer_coupons"):
        return

    op.execute("DELETE FROM customer_coupons WHERE coupon_type_id IS NULL")

    fk_name = _find_fk_name(
        insp,
        table_name="customer_coupons",
        constrained_columns=["coupon_type_id"],
        referred_table="coupon_types",
    )

    with op.batch_alter_table("customer_coupons") as batch_op:
        if fk_name:
            batch_op.drop_constraint(fk_name, type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_customer_coupons_coupon_type_id_coupon_types",
            "coupon_types",
            ["coupon_type_id"],
            ["id"],
        )
        batch_op.alter_column(
            "coupon_type_id",
            existing_type=sa.dialects.postgresql.UUID(as_uuid=True),
            nullable=False,
        )
