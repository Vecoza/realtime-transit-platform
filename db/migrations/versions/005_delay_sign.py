"""One delay sign across the platform: positive means late.

``live_delay_s`` is HSL's own deviation (``dl``) with its sign turned at the boundary;
``measured_delay_s`` is the platform's delay against the GTFS planned time.

Revision ID: 005
Revises: 004
"""

from alembic import op

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None

RENAMES = (
    ("rt.vehicle_positions", "hfp_schedule_offset_s", "live_delay_s"),
    ("rt.stop_events", "hfp_schedule_offset_s", "live_delay_s"),
    ("rt.stop_events", "delay_seconds", "measured_delay_s"),
    ("rt.route_minute_stats", "delay_samples", "live_delay_samples"),
    ("rt.route_minute_stats", "avg_delay_s", "avg_live_delay_s"),
    ("rt.route_minute_stats", "p90_delay_s", "p90_live_delay_s"),
    ("rt.stop_minute_stats", "delay_samples", "live_delay_samples"),
    ("rt.stop_minute_stats", "avg_delay_s", "avg_live_delay_s"),
)
NEGATED = (("rt.vehicle_positions", "live_delay_s"), ("rt.stop_events", "live_delay_s"))


def upgrade() -> None:
    for table, old, new in RENAMES:
        op.execute(f"ALTER TABLE {table} RENAME COLUMN {old} TO {new}")
    for table, column in NEGATED:
        op.execute(f"UPDATE {table} SET {column} = -{column} WHERE {column} IS NOT NULL")


def downgrade() -> None:
    for table, column in NEGATED:
        op.execute(f"UPDATE {table} SET {column} = -{column} WHERE {column} IS NOT NULL")
    for table, old, new in RENAMES:
        op.execute(f"ALTER TABLE {table} RENAME COLUMN {new} TO {old}")
