"""customer anonymize columns + erased_identities + unomi delete queue

Revision ID: k3d4e5f6a7b9
Revises: j2c3d4e5f6a8
Create Date: 2026-11-24
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "k3d4e5f6a7b9"
down_revision: Union[str, Sequence[str], None] = "j2c3d4e5f6a8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    cols = {c["name"] for c in insp.get_columns("customers")}
    if "anonymized_at" not in cols:
        op.add_column("customers", sa.Column("anonymized_at", sa.TIMESTAMP(), nullable=True))
    if "email_hash" not in cols:
        op.add_column("customers", sa.Column("email_hash", sa.String(length=64), nullable=True))

    tables = set(insp.get_table_names())
    if "erased_identities" not in tables:
        op.create_table(
            "erased_identities",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("brand", sa.String(length=50), nullable=False),
            sa.Column("email_hash", sa.String(length=64), nullable=False),
            sa.Column("erased_at", sa.TIMESTAMP(), server_default=sa.func.now(), nullable=False),
            sa.Column("bypass_used", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("mode", sa.String(length=20), nullable=False, server_default="anonymize"),
            sa.UniqueConstraint("brand", "email_hash", name="uq_erased_identities_brand_email_hash"),
        )
        op.create_index("ix_erased_identities_brand_hash", "erased_identities", ["brand", "email_hash"])
    if "unomi_delete_queue" not in tables:
        op.create_table(
            "unomi_delete_queue",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("brand", sa.String(length=50), nullable=False),
            sa.Column("profile_id", sa.String(length=100), nullable=False),
            sa.Column("status", sa.String(length=20), nullable=False, server_default="PENDING"),
            sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("last_error", sa.String(length=2000), nullable=True),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.func.now()),
            sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.func.now()),
        )
        op.create_index("ix_unomi_delete_queue_brand_status", "unomi_delete_queue", ["brand", "status"])


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    tables = set(insp.get_table_names())
    if "unomi_delete_queue" in tables:
        op.drop_index("ix_unomi_delete_queue_brand_status", table_name="unomi_delete_queue")
        op.drop_table("unomi_delete_queue")
    if "erased_identities" in tables:
        op.drop_index("ix_erased_identities_brand_hash", table_name="erased_identities")
        op.drop_table("erased_identities")
    cols = {c["name"] for c in insp.get_columns("customers")}
    if "email_hash" in cols:
        op.drop_column("customers", "email_hash")
    if "anonymized_at" in cols:
        op.drop_column("customers", "anonymized_at")
