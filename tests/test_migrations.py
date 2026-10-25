import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text

from transit_platform.config import DatabaseSettings

pytestmark = pytest.mark.db

GTFS_TABLES = {
    "feed_versions",
    "agencies",
    "routes",
    "stops",
    "calendar",
    "calendar_dates",
    "service_dates",
    "shapes",
    "trips",
    "stop_times",
}


RT_TABLES = {
    "rt.vehicle_positions",
    "rt.vehicle_positions_default",
    "rt.stop_events",
    "rt.stop_events_default",
    "meta.consumer_metrics",
    "rt.route_minute_stats",
    "rt.route_minute_stats_default",
    "rt.stop_minute_stats",
    "rt.stop_minute_stats_default",
    "rt.late_events",
    "rt.vehicle_status",
    "meta.aggregator_progress",
    "meta.aggregator_watermarks",
    "meta.aggregator_metrics",
    "meta.archive_runs",
    "meta.dbt_runs",
}
FUNCTIONS = 5


def schema_objects(settings: DatabaseSettings) -> tuple[set[str], int]:
    engine = create_engine(settings.url)
    with engine.connect() as connection:
        tables = set(
            connection.execute(
                text("""
                SELECT CASE WHEN schemaname = 'gtfs' THEN tablename
                            ELSE schemaname || '.' || tablename END
                FROM pg_tables
                WHERE schemaname IN ('gtfs', 'rt', 'meta')
                  AND tablename !~ '_[0-9]+$'
            """)
            ).scalars()
        )
        functions = connection.execute(
            text("""
            SELECT count(*) FROM pg_proc
            WHERE pronamespace IN ('gtfs'::regnamespace, 'rt'::regnamespace)
        """)
        ).scalar_one()
    engine.dispose()
    return tables, functions


def test_head_creates_the_timetable_and_real_time_model(migrated: DatabaseSettings) -> None:
    assert schema_objects(migrated) == (GTFS_TABLES | RT_TABLES, FUNCTIONS)


def test_downgrade_to_base_and_upgrade_again_round_trips(
    migrated: DatabaseSettings, migration_config: Config
) -> None:
    command.downgrade(migration_config, "base")
    assert schema_objects(migrated) == (set(), 0)

    command.upgrade(migration_config, "head")
    assert schema_objects(migrated) == (GTFS_TABLES | RT_TABLES, FUNCTIONS)
