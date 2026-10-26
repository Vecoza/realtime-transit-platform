"""Waiting vehicles: silent before their trip's planned start, which is not yet stale.

Revision ID: 015
Revises: 014
"""

from alembic import op

revision = "015"
down_revision = "014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE rt.vehicle_status
            DROP CONSTRAINT ck_vehicle_status_status,
            ADD COLUMN planned_start TIMESTAMPTZ,
            ADD CONSTRAINT ck_vehicle_status_status
                CHECK (status IN ('active', 'waiting', 'stale', 'finished'));
    """)


def downgrade() -> None:
    op.execute("""
        UPDATE rt.vehicle_status SET status = 'stale' WHERE status = 'waiting';
        ALTER TABLE rt.vehicle_status
            DROP CONSTRAINT ck_vehicle_status_status,
            DROP COLUMN planned_start,
            ADD CONSTRAINT ck_vehicle_status_status
                CHECK (status IN ('active', 'stale', 'finished'));
    """)
