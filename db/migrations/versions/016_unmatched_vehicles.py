"""Unmatched vehicles: silent on a journey without a timetable trip, which cannot be stale.

Revision ID: 016
Revises: 015
"""

from alembic import op

revision = "016"
down_revision = "015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE rt.vehicle_status
            DROP CONSTRAINT ck_vehicle_status_status,
            ADD CONSTRAINT ck_vehicle_status_status
                CHECK (status IN ('active', 'waiting', 'stale', 'finished', 'unmatched'));
        UPDATE rt.vehicle_status SET status = 'unmatched'
        WHERE status = 'stale' AND planned_end IS NULL;
        ALTER TABLE meta.aggregator_metrics
            ADD COLUMN vehicles_unmatched INTEGER NOT NULL DEFAULT 0;
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE meta.aggregator_metrics DROP COLUMN vehicles_unmatched;
        UPDATE rt.vehicle_status SET status = 'stale' WHERE status = 'unmatched';
        ALTER TABLE rt.vehicle_status
            DROP CONSTRAINT ck_vehicle_status_status,
            ADD CONSTRAINT ck_vehicle_status_status
                CHECK (status IN ('active', 'waiting', 'stale', 'finished'));
    """)
