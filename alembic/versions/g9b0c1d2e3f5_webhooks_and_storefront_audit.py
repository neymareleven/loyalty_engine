"""outgoing webhooks outbox + storefront audit

Revision ID: g9b0c1d2e3f5
Revises: f8a9b0c1d2e4
Create Date: 2026-11-16

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "g9b0c1d2e3f5"
down_revision: Union[str, Sequence[str], None] = "f8a9b0c1d2e4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table("webhook_endpoints"):
        op.create_table(
            "webhook_endpoints",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("brand", sa.String(length=50), nullable=False),
            sa.Column("url", sa.String(length=500), nullable=False),
            sa.Column("secret", sa.String(length=255), nullable=False),
            sa.Column("events", sa.JSON(), nullable=False),
            sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()")),
            sa.Column("updated_at", sa.TIMESTAMP(), server_default=sa.text("now()")),
        )
    if not inspector.has_table("webhook_outbox"):
        op.create_table(
            "webhook_outbox",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("brand", sa.String(length=50), nullable=False),
            sa.Column("endpoint_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("webhook_endpoints.id"), nullable=False),
            sa.Column("event", sa.String(length=80), nullable=False),
            sa.Column("payload", sa.JSON(), nullable=False),
            sa.Column("status", sa.String(length=20), nullable=False, server_default="PENDING"),
            sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("next_attempt_at", sa.TIMESTAMP(), nullable=True),
            sa.Column("last_error", sa.Text(), nullable=True),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()")),
            sa.Column("sent_at", sa.TIMESTAMP(), nullable=True),
        )
        op.create_index("ix_webhook_outbox_status_next", "webhook_outbox", ["status", "next_attempt_at"])
        op.create_index("ix_webhook_outbox_brand", "webhook_outbox", ["brand"])
    if not inspector.has_table("storefront_audit_events"):
        op.create_table(
            "storefront_audit_events",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("brand", sa.String(length=50), nullable=False),
            sa.Column("site_id", sa.String(length=100), nullable=True),
            sa.Column("action", sa.String(length=40), nullable=False),
            sa.Column("profile_id", sa.String(length=100), nullable=True),
            sa.Column("redemption_id", sa.String(length=64), nullable=True),
            sa.Column("request", sa.JSON(), nullable=True),
            sa.Column("response", sa.JSON(), nullable=True),
            sa.Column("created_at", sa.TIMESTAMP(), server_default=sa.text("now()")),
        )
        op.create_index("ix_storefront_audit_brand_created", "storefront_audit_events", ["brand", "created_at"])


def downgrade() -> None:
    op.drop_table("storefront_audit_events")
    op.drop_table("webhook_outbox")
    op.drop_table("webhook_endpoints")
