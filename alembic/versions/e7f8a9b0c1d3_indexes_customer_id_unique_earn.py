"""indexes, transactions.customer_id, unique purchase EARN

Revision ID: e7f8a9b0c1d3
Revises: d6e7f8a9b0c2
Create Date: 2026-11-16

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "e7f8a9b0c1d3"
down_revision: Union[str, Sequence[str], None] = "d6e7f8a9b0c2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _index_names(inspector, table: str) -> set[str]:
    if not inspector.has_table(table):
        return set()
    return {idx["name"] for idx in inspector.get_indexes(table) if idx.get("name")}


def _create_index_sql(statement: str, *, postgresql: bool) -> None:
    if postgresql:
        with op.get_context().autocommit_block():
            op.execute(sa.text(statement))
    else:
        op.execute(sa.text(statement.replace(" CONCURRENTLY", "")))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    is_pg = bind.dialect.name == "postgresql"

    if inspector.has_table("transactions"):
        cols = {c["name"] for c in inspector.get_columns("transactions")}
        if "customer_id" not in cols:
            op.add_column(
                "transactions",
                sa.Column("customer_id", postgresql.UUID(as_uuid=True), nullable=True),
            )
            op.create_foreign_key(
                "fk_transactions_customer_id",
                "transactions",
                "customers",
                ["customer_id"],
                ["id"],
            )
            op.execute(
                sa.text(
                    """
                    UPDATE transactions AS t
                    SET customer_id = c.id
                    FROM customers AS c
                    WHERE t.customer_id IS NULL
                      AND t.brand = c.brand
                      AND t.profile_id = c.profile_id
                    """
                )
            )

    existing_tx = _index_names(inspector, "transactions")
    existing_cust = _index_names(inspector, "customers")
    existing_pm = _index_names(inspector, "purchase_movements")
    existing_pts = _index_names(inspector, "point_movements")
    existing_cc = _index_names(inspector, "customer_coupons")

    if "ix_transactions_brand_profile_created" not in existing_tx:
        _create_index_sql(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_transactions_brand_profile_created "
            "ON transactions (brand, profile_id, created_at)",
            postgresql=is_pg,
        )
    if "ix_transactions_customer_id" not in existing_tx:
        _create_index_sql(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_transactions_customer_id "
            "ON transactions (customer_id)",
            postgresql=is_pg,
        )
    if "ix_customers_brand_profile_id" not in existing_cust:
        _create_index_sql(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_customers_brand_profile_id "
            "ON customers (brand, profile_id)",
            postgresql=is_pg,
        )
    if "ix_purchase_movements_customer_expires" not in existing_pm:
        _create_index_sql(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_purchase_movements_customer_expires "
            "ON purchase_movements (customer_id, expires_at)",
            postgresql=is_pg,
        )
    if "ix_point_movements_customer_expires" not in existing_pts:
        _create_index_sql(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_point_movements_customer_expires "
            "ON point_movements (customer_id, expires_at)",
            postgresql=is_pg,
        )
    if "ix_customer_coupons_customer_type_issued" not in existing_cc:
        _create_index_sql(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_customer_coupons_customer_type_issued "
            "ON customer_coupons (customer_id, coupon_type_id, issued_at)",
            postgresql=is_pg,
        )

    dups = bind.execute(
        sa.text(
            """
            SELECT brand, profile_id, COUNT(*) AS n
            FROM customers
            GROUP BY brand, profile_id
            HAVING COUNT(*) > 1
            """
        )
    ).fetchall()
    if not dups and "uq_customers_brand_profile_id" not in existing_cust:
        _create_index_sql(
            "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_customers_brand_profile_id "
            "ON customers (brand, profile_id)",
            postgresql=is_pg,
        )

    if "uq_purchase_movements_earn_source_tx" not in existing_pm:
        _create_index_sql(
            "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_purchase_movements_earn_source_tx "
            "ON purchase_movements (source_transaction_id) "
            "WHERE type = 'EARN' AND source_transaction_id IS NOT NULL",
            postgresql=is_pg,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    is_pg = bind.dialect.name == "postgresql"

    def drop(name: str, table: str) -> None:
        if name not in _index_names(inspector, table):
            return
        if is_pg:
            with op.get_context().autocommit_block():
                op.execute(sa.text(f"DROP INDEX CONCURRENTLY IF EXISTS {name}"))
        else:
            op.drop_index(name, table_name=table)

    drop("uq_purchase_movements_earn_source_tx", "purchase_movements")
    drop("uq_customers_brand_profile_id", "customers")
    drop("ix_customer_coupons_customer_type_issued", "customer_coupons")
    drop("ix_point_movements_customer_expires", "point_movements")
    drop("ix_purchase_movements_customer_expires", "purchase_movements")
    drop("ix_customers_brand_profile_id", "customers")
    drop("ix_transactions_customer_id", "transactions")
    drop("ix_transactions_brand_profile_created", "transactions")

    inspector = sa.inspect(bind)
    if inspector.has_table("transactions"):
        cols = {c["name"] for c in inspector.get_columns("transactions")}
        fks = {fk["name"] for fk in inspector.get_foreign_keys("transactions") if fk.get("name")}
        if "fk_transactions_customer_id" in fks:
            op.drop_constraint("fk_transactions_customer_id", "transactions", type_="foreignkey")
        if "customer_id" in cols:
            op.drop_column("transactions", "customer_id")
