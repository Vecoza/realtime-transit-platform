"""Delay figures per journey: the number of journeys behind each minute's delay figures.

From this revision the average, median and p90 are taken over journeys (each journey's
median delay in the minute), so coupled units of one train count once.

Revision ID: 014
Revises: 013
"""

from alembic import op

revision = "014"
down_revision = "013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE rt.route_minute_stats
            ADD COLUMN journeys INTEGER,
            ADD CONSTRAINT ck_route_minute_stats_journeys
                CHECK (journeys IS NULL OR journeys <= live_delay_samples);
        ALTER TABLE rt.stop_minute_stats
            ADD COLUMN journeys INTEGER,
            ADD CONSTRAINT ck_stop_minute_stats_journeys
                CHECK (journeys IS NULL OR journeys <= live_delay_samples);
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE rt.stop_minute_stats
            DROP CONSTRAINT ck_stop_minute_stats_journeys,
            DROP COLUMN journeys;
        ALTER TABLE rt.route_minute_stats
            DROP CONSTRAINT ck_route_minute_stats_journeys,
            DROP COLUMN journeys;
    """)
