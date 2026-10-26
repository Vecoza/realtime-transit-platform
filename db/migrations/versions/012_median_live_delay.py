"""Median live delay per minute for lines and stops, and the p90 for stops.

Windows emitted before this revision keep NULL in the new columns.

Revision ID: 012
Revises: 011
"""

from alembic import op

revision = "012"
down_revision = "011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE rt.route_minute_stats
            ADD COLUMN p50_live_delay_s DOUBLE PRECISION,
            ADD CONSTRAINT ck_route_minute_stats_median
                CHECK (p50_live_delay_s IS NULL OR live_delay_samples > 0);
        ALTER TABLE rt.stop_minute_stats
            ADD COLUMN p50_live_delay_s DOUBLE PRECISION,
            ADD COLUMN p90_live_delay_s DOUBLE PRECISION,
            ADD CONSTRAINT ck_stop_minute_stats_percentiles
                CHECK ((p50_live_delay_s IS NULL AND p90_live_delay_s IS NULL)
                       OR live_delay_samples > 0);
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE rt.stop_minute_stats
            DROP CONSTRAINT ck_stop_minute_stats_percentiles,
            DROP COLUMN p90_live_delay_s,
            DROP COLUMN p50_live_delay_s;
        ALTER TABLE rt.route_minute_stats
            DROP CONSTRAINT ck_route_minute_stats_median,
            DROP COLUMN p50_live_delay_s;
    """)
