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


def gtfs_objects(settings: DatabaseSettings) -> tuple[set[str], int]:
    engine = create_engine(settings.url)
    with engine.connect() as connection:
        tables = set(
            connection.execute(
                text("SELECT tablename FROM pg_tables WHERE schemaname = 'gtfs'")
            ).scalars()
        )
        functions = connection.execute(
            text("SELECT count(*) FROM pg_proc WHERE pronamespace = 'gtfs'::regnamespace")
        ).scalar_one()
    engine.dispose()
    return tables, functions


def test_head_creates_the_gtfs_model(migrated: DatabaseSettings) -> None:
    assert gtfs_objects(migrated) == (GTFS_TABLES, 3)


def test_downgrade_to_base_and_upgrade_again_round_trips(
    migrated: DatabaseSettings, migration_config: Config
) -> None:
    command.downgrade(migration_config, "base")
    assert gtfs_objects(migrated) == (set(), 0)

    command.upgrade(migration_config, "head")
    assert gtfs_objects(migrated) == (GTFS_TABLES, 3)
