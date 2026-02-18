from alembic import op
import sqlalchemy as sa


revision = "3b2a1b3c6e1a"
down_revision = "0c057ed5526f"
branch_labels = None
depends_on = None


def _table_exists(bind, table_name: str) -> bool:
    insp = sa.inspect(bind)
    return table_name in insp.get_table_names()


def upgrade() -> None:
    bind = op.get_bind()

    if not _table_exists(bind, "customer_tags"):
        op.create_table(
            "customer_tags",
            sa.Column("id", sa.dialects.postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column("customer_id", sa.dialects.postgresql.UUID(as_uuid=True), sa.ForeignKey("customers.id"), nullable=False),
            sa.Column("tag", sa.String(length=100), nullable=False),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()"), nullable=True),
        )

    insp = sa.inspect(bind)
    indexes = {ix["name"] for ix in insp.get_indexes("customer_tags")} if _table_exists(bind, "customer_tags") else set()
    if "uq_customer_tags_customer_id_tag" not in indexes:
        op.create_index(
            "uq_customer_tags_customer_id_tag",
            "customer_tags",
            ["customer_id", "tag"],
            unique=True,
        )


def downgrade() -> None:
    bind = op.get_bind()

    if _table_exists(bind, "customer_tags"):
        insp = sa.inspect(bind)
        indexes = {ix["name"] for ix in insp.get_indexes("customer_tags")}
        if "uq_customer_tags_customer_id_tag" in indexes:
            op.drop_index("uq_customer_tags_customer_id_tag", table_name="customer_tags")

        op.drop_table("customer_tags")
