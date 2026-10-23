"""Shared fixtures: a migrated test database."""

from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import OperationalError

from transit_platform.config import DatabaseSettings

REPO_ROOT = Path(__file__).resolve().parent.parent
INIT_SQL = sorted((REPO_ROOT / "docker" / "init").glob("*.sql"))


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
