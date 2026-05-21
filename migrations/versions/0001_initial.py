"""initial schema

Revision ID: 0001_initial
Revises:
Create Date: 2026-05-19 00:00:00.000000
"""
import sqlalchemy as sa
from alembic import op

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "notification_batches",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("channel", sa.String(length=16), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=True),
        sa.Column("request_hash", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("idempotency_key", name="uq_notification_batches_idempotency_key"),
    )
    op.create_table(
        "notifications",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("batch_id", sa.Uuid(), sa.ForeignKey("notification_batches.id"), nullable=False),
        sa.Column("subscriber_id", sa.BigInteger(), nullable=False),
        sa.Column("channel", sa.String(length=16), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("provider_message_id", sa.String(length=255), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("processing_lock_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "batch_id",
            "subscriber_id",
            name="uq_notification_per_batch_subscriber",
        ),
    )
    op.create_table(
        "notification_status_history",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("notification_id", sa.Uuid(), sa.ForeignKey("notifications.id"), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "idempotency_keys",
        sa.Column("key", sa.String(length=255), primary_key=True),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("batch_id", sa.Uuid(), sa.ForeignKey("notification_batches.id"), nullable=True),
        sa.Column("response_payload", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("aggregate_type", sa.String(length=100), nullable=False),
        sa.Column("aggregate_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("idx_notifications_subscriber_id", "notifications", ["subscriber_id"])
    op.create_index("idx_notifications_status", "notifications", ["status"])
    op.create_index("idx_notifications_batch_id", "notifications", ["batch_id"])
    op.create_index(
        "idx_status_history_notification_id_created_at",
        "notification_status_history",
        ["notification_id", "created_at"],
    )
    op.create_index("idx_idempotency_expires_at", "idempotency_keys", ["expires_at"])
    op.create_index(
        "idx_outbox_status_available_at_created_at",
        "outbox_events",
        ["status", "available_at", "created_at"],
    )
    op.create_index("idx_outbox_aggregate_id", "outbox_events", ["aggregate_id"])


def downgrade() -> None:
    op.drop_index("idx_outbox_aggregate_id", table_name="outbox_events")
    op.drop_index("idx_outbox_status_available_at_created_at", table_name="outbox_events")
    op.drop_index("idx_idempotency_expires_at", table_name="idempotency_keys")
    op.drop_index(
        "idx_status_history_notification_id_created_at",
        table_name="notification_status_history",
    )
    op.drop_index("idx_notifications_batch_id", table_name="notifications")
    op.drop_index("idx_notifications_status", table_name="notifications")
    op.drop_index("idx_notifications_subscriber_id", table_name="notifications")
    op.drop_table("outbox_events")
    op.drop_table("idempotency_keys")
    op.drop_table("notification_status_history")
    op.drop_table("notifications")
    op.drop_table("notification_batches")
