"""Shared fixtures: a migrated test database and helpers for building GTFS zips."""

from __future__ import annotations

import os
import shutil
import zipfile
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import OperationalError

from gtfs.download import FeedFile
from gtfs.loader import load_feed
from transit_platform.config import DatabaseSettings, StreamSettings

REPO_ROOT = Path(__file__).resolve().parent.parent
INIT_SQL = sorted((REPO_ROOT / "docker" / "init").glob("*.sql"))
GTFS_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "gtfs_small"


@pytest.fixture(scope="session")
def admin_engine() -> Iterator[Engine]:
    """Autocommit connection to the server, used to create and drop the test database."""
    server = DatabaseSettings.from_env()
    engine = create_engine(
        server.url, isolation_level="AUTOCOMMIT", connect_args={"connect_timeout": 3}
    )
    try:
        with engine.connect():
            pass
    except OperationalError:
        engine.dispose()
        message = f"PostgreSQL is not reachable at {server.host}:{server.port}"
        if os.getenv("CI"):
            pytest.fail(message)
        pytest.skip(f"{message}; start it with `make up` to run database tests")
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def database_settings(admin_engine: Engine) -> Iterator[DatabaseSettings]:
    """An empty database next to the platform one, initialised like a fresh container."""
    server = DatabaseSettings.from_env()
    settings = replace(server, name=f"{server.name}_test")
    with admin_engine.connect() as connection:
        connection.execute(text(f'DROP DATABASE IF EXISTS "{settings.name}" WITH (FORCE)'))
        connection.execute(text(f'CREATE DATABASE "{settings.name}"'))
    with psycopg.connect(settings.conninfo, autocommit=True) as conn:
        for script in INIT_SQL:
            conn.execute(script.read_text())
    yield settings
    with admin_engine.connect() as connection:
        connection.execute(text(f'DROP DATABASE "{settings.name}" WITH (FORCE)'))


@pytest.fixture(scope="session")
def migration_config(database_settings: DatabaseSettings) -> Config:
    config = Config(str(REPO_ROOT / "alembic.ini"))
    config.attributes["database_url"] = database_settings.url
    return config


@pytest.fixture(scope="session")
def migrated(database_settings: DatabaseSettings, migration_config: Config) -> DatabaseSettings:
    command.upgrade(migration_config, "head")
    return database_settings


@pytest.fixture
def conn(migrated: DatabaseSettings) -> Iterator[psycopg.Connection]:
    """A connection to the migrated test database; GTFS data is removed afterwards."""
    with psycopg.connect(migrated.conninfo) as connection:
        yield connection
        connection.rollback()
        reset_data(connection)


def reset_data(conn: psycopg.Connection) -> None:
    with conn.transaction():
        partitions = conn.execute("""
            SELECT inhrelid::regclass::TEXT
            FROM pg_inherits
            WHERE inhparent = 'gtfs.stop_times'::regclass
        """).fetchall()
        for (partition,) in partitions:
            conn.execute(f"DROP TABLE {partition}")
        conn.execute("TRUNCATE gtfs.feed_versions RESTART IDENTITY CASCADE")
        conn.execute("""
            TRUNCATE rt.vehicle_positions, rt.stop_events, meta.consumer_metrics,
                rt.route_minute_stats, rt.stop_minute_stats, rt.late_events, rt.vehicle_status,
                meta.aggregator_progress, meta.aggregator_watermarks, meta.aggregator_metrics,
                meta.archive_runs
        """)


@pytest.fixture
def feed_dir(tmp_path: Path) -> Path:
    """A writable copy of the small GTFS fixture."""
    target = tmp_path / "gtfs_small"
    shutil.copytree(GTFS_FIXTURE, target)
    return target


def zip_feed(source_dir: Path, target: Path) -> Path:
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(source_dir.glob("*.txt")):
            archive.write(path, path.name)
    return target


@pytest.fixture(scope="session")
def stream_settings() -> StreamSettings:
    """Settings for a reachable broker; stream tests are skipped locally if there is none."""
    from confluent_kafka.admin import AdminClient

    settings = StreamSettings.from_env()
    admin = AdminClient(
        {"bootstrap.servers": settings.bootstrap_servers, "socket.timeout.ms": 3000}
    )
    try:
        admin.list_topics(timeout=5)
    except Exception:
        message = f"Kafka broker is not reachable at {settings.bootstrap_servers}"
        if os.getenv("CI"):
            pytest.fail(message)
        pytest.skip(f"{message}; start it with `make up` to run stream tests")
    return settings


def load_build(conn: psycopg.Connection, feed_dir: Path, tmp_path: Path, built: str) -> int:
    """Load the GTFS fixture as a feed that HSL built on ``built`` (YYYY-MM-DD)."""
    info = feed_dir / "feed_info.txt"
    lines = info.read_text().splitlines()
    lines[1] = lines[1].rsplit(",", 1)[0] + f",{built} 04:11:14"
    info.write_text("\n".join(lines) + "\n")
    feed = FeedFile.from_path(zip_feed(feed_dir, tmp_path / f"{built}.zip"))
    return load_feed(conn, feed).feed_id
