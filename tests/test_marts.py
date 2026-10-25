"""The analytical marts give the same results before and after the days are archived.

Real messages of two Saturday routes are stored; the marts are built while the day is in
PostgreSQL, then the day is moved to Parquet and the timetable version it was matched to is
dropped, and the marts are built again from the archive.
"""

import shutil
import subprocess
import sys
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import duckdb
import psycopg
import pytest

from archive.lifecycle import Lifecycle, connect_duckdb
from processor.decode import Event, decode
from processor.store import ensure_partitions, event_days, write_events
from producer.recording import read_envelopes
from tests.conftest import REPO_ROOT, load_build
from transit_platform.config import ArchiveSettings, DatabaseSettings

FIXTURES = REPO_ROOT / "tests" / "fixtures"
DBT_DIR = REPO_ROOT / "dbt"
MARTS = (
    "mart_on_time_performance",
    "mart_stop_punctuality",
    "mart_bunching",
    "mart_data_completeness",
)


@pytest.fixture
def warehouse(migrated: DatabaseSettings, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    for name, value in {
        "POSTGRES_HOST": migrated.host,
        "POSTGRES_PORT": str(migrated.port),
        "POSTGRES_DB": migrated.name,
        "POSTGRES_USER": migrated.user,
        "POSTGRES_PASSWORD": migrated.password,
        "ARCHIVE_DIR": str(tmp_path / "archive"),
        "DUCKDB_PATH": str(tmp_path / "warehouse.duckdb"),
    }.items():
        monkeypatch.setenv(name, value)
    return tmp_path / "warehouse.duckdb"


@pytest.fixture
def lifecycle(migrated: DatabaseSettings, tmp_path: Path) -> Iterator[Lifecycle]:
    with (
        psycopg.connect(migrated.conninfo, autocommit=True) as conn,
        connect_duckdb(migrated) as duck,
    ):
        yield Lifecycle(conn, duck, ArchiveSettings(tmp_path / "archive"))


def store_saturday(conn: psycopg.Connection) -> int:
    events = [decode(e) for e in read_envelopes([FIXTURES / "hfp_saturday_routes.jsonl"])]
    assert all(isinstance(event, Event) for event in events)
    ensure_partitions(conn, event_days(events))
    with conn.transaction():
        result = write_events(conn, events)
    conn.commit()
    return result.inserted


def build_marts(warehouse: Path) -> dict[str, list[dict]]:
    dbt = Path(sys.executable).parent / "dbt"
    result = subprocess.run(
        [dbt, "build", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout[-3000:]
    with duckdb.connect(str(warehouse), read_only=True) as db:
        marts = {}
        for mart in MARTS:
            cursor = db.execute(f"SELECT * FROM {mart} ORDER BY ALL")
            columns = [column[0] for column in cursor.description]
            marts[mart] = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
        return marts


def test_marts_are_identical_before_and_after_archiving(
    conn: psycopg.Connection,
    lifecycle: Lifecycle,
    warehouse: Path,
    tmp_path: Path,
) -> None:
    feed_dir = tmp_path / "gtfs"
    shutil.copytree(FIXTURES / "gtfs_saturday_routes", feed_dir)
    matched_feed = load_build(conn, feed_dir, tmp_path, "2026-10-24")
    conn.commit()
    stored = store_saturday(conn)
    lifecycle.run(date(2026, 10, 25))

    before = build_marts(warehouse)

    load_build(conn, feed_dir, tmp_path, "2026-11-01")
    conn.commit()
    lifecycle.run(date(2026, 11, 10))
    after = build_marts(warehouse)

    check = lifecycle.conn
    assert check.execute("SELECT to_regclass('rt.stop_events_20261024')").fetchone() == (None,)
    assert check.execute(
        "SELECT count(*) FROM gtfs.feed_versions WHERE feed_id = %s", (matched_feed,)
    ).fetchone() == (0,)
    archived = check.execute(
        "SELECT sum(row_count) FROM meta.archive_runs WHERE step = 'dropped'"
        " AND source_table IN ('rt.vehicle_positions', 'rt.stop_events')"
    ).fetchone()[0]
    assert archived == stored
    assert all(before[mart] for mart in MARTS)
    assert sum(row["bunched_pairs"] for row in before["mart_bunching"]) > 0
    assert {row["route_short_name"] for row in before["mart_on_time_performance"]} == {"79", "400"}
    assert all(row["stop_name"] for row in before["mart_bunching"])
    assert after == before
