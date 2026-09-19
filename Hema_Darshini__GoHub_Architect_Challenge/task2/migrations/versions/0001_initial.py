"""Initial seat inventory schema."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    status = postgresql.ENUM("available", "held", "booked", name="seat_status", create_type=False)
    status.create(op.get_bind(), checkfirst=True)
    op.create_table("seats",
        sa.Column("id", sa.String(80), primary_key=True),
        sa.Column("trip_id", sa.String(80), nullable=False),
        sa.Column("operator_id", sa.String(80), nullable=False),
        sa.Column("status", status, nullable=False, server_default="available"),
        sa.Column("hold_user_id", sa.String(80)), sa.Column("hold_created_at", sa.DateTime(timezone=True)),
        sa.Column("hold_expires_at", sa.DateTime(timezone=True)), sa.Column("booked_user_id", sa.String(80)),
        sa.Column("version", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()))
    op.create_index("ix_seats_trip_id", "seats", ["trip_id"])
    op.create_index("ix_seats_hold_expires_at", "seats", ["hold_expires_at"])
    op.create_table("idempotency_records",
        sa.Column("key", sa.String(200), primary_key=True), sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=False), sa.Column("response_json", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False))
    op.create_index("ix_idempotency_records_expires_at", "idempotency_records", ["expires_at"])
    op.create_table("audit_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True), sa.Column("seat_id", sa.String(80), nullable=False),
        sa.Column("action", sa.String(40), nullable=False), sa.Column("actor", sa.String(80), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("details", postgresql.JSONB(), nullable=False))
    op.create_index("ix_audit_events_seat_id", "audit_events", ["seat_id"])
    op.create_table("outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True), sa.Column("aggregate_id", sa.String(80), nullable=False),
        sa.Column("event_type", sa.String(80), nullable=False), sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("published_at", sa.DateTime(timezone=True)), sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"))
    op.create_index("ix_outbox_events_published_at", "outbox_events", ["published_at"])
    op.create_table("processed_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True), sa.Column("consumer", sa.String(80), primary_key=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("id", "consumer", name="uq_processed_event_consumer"))


def downgrade():
    for table in ("processed_events", "outbox_events", "audit_events", "idempotency_records", "seats"):
        op.drop_table(table)
    postgresql.ENUM(name="seat_status").drop(op.get_bind(), checkfirst=True)
