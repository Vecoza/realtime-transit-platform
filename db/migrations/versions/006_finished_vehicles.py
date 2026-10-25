"""Silent vehicles: stale while the trip is planned to run, finished after its planned end.

Revision ID: 006
Revises: 005
"""

from alembic import op

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE rt.vehicle_status
            DROP CONSTRAINT ck_vehicle_status_status,
            DROP CONSTRAINT ck_vehicle_status_stale_since;
        ALTER TABLE rt.vehicle_status RENAME COLUMN stale_since TO silent_since;
        ALTER TABLE rt.vehicle_status
            ADD COLUMN planned_end TIMESTAMPTZ,
            ADD CONSTRAINT ck_vehicle_status_status
                CHECK (status IN ('active', 'stale', 'finished')),
            ADD CONSTRAINT ck_vehicle_status_silent_since
                CHECK ((status = 'active') = (silent_since IS NULL));

        ALTER TABLE meta.aggregator_metrics
            ADD COLUMN vehicles_finished INTEGER NOT NULL DEFAULT 0;
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE meta.aggregator_metrics DROP COLUMN vehicles_finished;

        DELETE FROM rt.vehicle_status WHERE status = 'finished';
        ALTER TABLE rt.vehicle_status
            DROP CONSTRAINT ck_vehicle_status_status,
            DROP CONSTRAINT ck_vehicle_status_silent_since,
            DROP COLUMN planned_end;
        ALTER TABLE rt.vehicle_status RENAME COLUMN silent_since TO stale_since;
        ALTER TABLE rt.vehicle_status
            ADD CONSTRAINT ck_vehicle_status_status CHECK (status IN ('active', 'stale')),
            ADD CONSTRAINT ck_vehicle_status_stale_since
                CHECK ((status = 'stale') = (stale_since IS NOT NULL));
    """)
