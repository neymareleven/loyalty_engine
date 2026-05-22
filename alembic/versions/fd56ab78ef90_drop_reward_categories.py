"""drop reward_categories and category FK columns

Revision ID: fd56ab78ef90
Revises: fc45de67ab89
Create Date: 2026-05-22

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "fd56ab78ef90"
down_revision: Union[str, Sequence[str], None] = "fc45de67ab89"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_table(insp, name: str) -> bool:
    try:
        return insp.has_table(name)
    except Exception:
        return False


def _has_column(insp, table: str, column: str) -> bool:
    try:
        cols = insp.get_columns(table)
    except Exception:
        return False
    return any(c.get("name") == column for c in cols)


def _drop_fk_by_column(bind, table: str, column: str, referred_table: str) -> None:
    """Drop FK constraints via pg_catalog (reliable on PostgreSQL)."""
    rows = bind.execute(
        sa.text(
            """
            SELECT c.conname
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = 'public'
              AND t.relname = :table_name
              AND c.contype = 'f'
              AND EXISTS (
                SELECT 1
                FROM unnest(c.conkey) WITH ORDINALITY AS cols(attnum, ord)
                JOIN pg_attribute a
                  ON a.attrelid = c.conrelid
                 AND a.attnum = cols.attnum
                WHERE a.attname = :column_name
              )
              AND EXISTS (
                SELECT 1 FROM pg_class rt
                WHERE rt.oid = c.confrelid AND rt.relname = :referred_table
              )
            """
        ),
        {"table_name": table, "column_name": column, "referred_table": referred_table},
    ).fetchall()
    for (conname,) in rows:
        bind.execute(sa.text(f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{conname}"'))


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # Backfill junction from legacy category links when columns still exist.
    if (
        _has_table(insp, "coupon_type_rewards")
        and _has_table(insp, "coupon_types")
        and _has_table(insp, "rewards")
        and _has_column(insp, "coupon_types", "reward_category_id")
        and _has_column(insp, "rewards", "reward_category_id")
    ):
        op.execute(
            """
            INSERT INTO coupon_type_rewards (coupon_type_id, reward_id)
            SELECT ct.id, r.id
            FROM coupon_types ct
            JOIN rewards r
              ON r.reward_category_id = ct.reward_category_id
             AND r.brand = ct.brand
            WHERE ct.reward_category_id IS NOT NULL
            ON CONFLICT DO NOTHING
            """
        )

    if _has_table(insp, "coupon_types") and _has_column(insp, "coupon_types", "reward_category_id"):
        _drop_fk_by_column(bind, "coupon_types", "reward_category_id", "reward_categories")
        op.execute("ALTER TABLE coupon_types DROP COLUMN IF EXISTS reward_category_id")

    if _has_table(insp, "rewards") and _has_column(insp, "rewards", "reward_category_id"):
        _drop_fk_by_column(bind, "rewards", "reward_category_id", "reward_categories")
        op.execute("DROP INDEX IF EXISTS ix_rewards_brand_reward_category_id")
        op.execute("DROP INDEX IF EXISTS ix_rewards_reward_category_id")
        op.execute("ALTER TABLE rewards DROP COLUMN IF EXISTS reward_category_id")

    if _has_table(insp, "reward_categories"):
        op.execute("DROP TABLE IF EXISTS reward_categories CASCADE")


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    if not _has_table(insp, "reward_categories"):
        op.create_table(
            "reward_categories",
            sa.Column("id", sa.dialects.postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("brand", sa.String(50), nullable=False),
            sa.Column("name", sa.String(200), nullable=False),
            sa.Column("description", sa.String(1000), nullable=True),
            sa.Column("active", sa.Boolean(), server_default=sa.text("true")),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()")),
            sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.text("now()")),
        )

    if _has_table(insp, "rewards") and not _has_column(insp, "rewards", "reward_category_id"):
        op.execute(
            "ALTER TABLE rewards ADD COLUMN IF NOT EXISTS reward_category_id UUID"
        )
        op.execute(
            """
            DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'fk_rewards_reward_category_id'
              ) THEN
                ALTER TABLE rewards
                  ADD CONSTRAINT fk_rewards_reward_category_id
                  FOREIGN KEY (reward_category_id) REFERENCES reward_categories(id)
                  ON DELETE RESTRICT;
              END IF;
            END $$;
            """
        )

    if _has_table(insp, "coupon_types") and not _has_column(insp, "coupon_types", "reward_category_id"):
        op.execute(
            "ALTER TABLE coupon_types ADD COLUMN IF NOT EXISTS reward_category_id UUID"
        )
        op.execute(
            """
            DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'fk_coupon_types_reward_category_id_reward_categories'
              ) THEN
                ALTER TABLE coupon_types
                  ADD CONSTRAINT fk_coupon_types_reward_category_id_reward_categories
                  FOREIGN KEY (reward_category_id) REFERENCES reward_categories(id)
                  ON DELETE RESTRICT;
              END IF;
            END $$;
            """
        )
