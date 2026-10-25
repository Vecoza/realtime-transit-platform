"""The analytical marts give the same results before and after the days are archived.

Real messages of two Saturday routes are stored; the marts are built while the day is in
PostgreSQL, then the day is moved to Parquet and the timetable version it was matched to is
dropped, and the marts are built again from the archive.
"""

import json
import shutil
import subprocess
import sys
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import duckdb
import psycopg
import pytest

from analytics.publish import run as publish_marts
from archive.lifecycle import Lifecycle, connect_duckdb
from processor.decode import Event, decode
from processor.store import ensure_partitions, event_days, write_events
from producer.recording import read_envelopes
from tests.conftest import REPO_ROOT, load_build
from transit_platform.config import AnalyticsSettings, ArchiveSettings, DatabaseSettings

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


def published(conn: psycopg.Connection, mart: str) -> list[dict]:
    cursor = conn.execute(f"SELECT * FROM marts.{mart}")
    columns = [column.name for column in cursor.description]
    rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
    return sorted(rows, key=lambda row: tuple(str(value) for value in row.values()))


def test_marts_are_published_to_postgres_and_replaced_by_the_next_build(
    conn: psycopg.Connection,
    migrated: DatabaseSettings,
    lifecycle: Lifecycle,
    warehouse: Path,
    tmp_path: Path,
) -> None:
    feed_dir = tmp_path / "gtfs"
    shutil.copytree(FIXTURES / "gtfs_saturday_routes", feed_dir)
    load_build(conn, feed_dir, tmp_path, "2026-10-24")
    conn.commit()
    store_saturday(conn)
    lifecycle.run(date(2026, 10, 25))
    results = DBT_DIR / "target" / "run_results.json"

    built = build_marts(warehouse)
    assert publish_marts(migrated, AnalyticsSettings(warehouse), results) == len(MARTS)
    publish_marts(migrated, AnalyticsSettings(warehouse), results)

    for mart in MARTS:
        expected = sorted(built[mart], key=lambda row: tuple(str(v) for v in row.values()))
        assert published(conn, mart) == expected
    assert conn.execute(
        "SELECT count(*) FROM pg_tables WHERE schemaname = 'marts' AND tablename LIKE '%%__next'"
    ).fetchone() == (0,)
    assert conn.execute(
        "SELECT models, failed, published_tables FROM meta.dbt_runs"
    ).fetchall() == [(12, 0, 4)]


def test_a_failed_build_is_recorded_and_publishes_nothing(
    conn: psycopg.Connection, migrated: DatabaseSettings, tmp_path: Path
) -> None:
    results = tmp_path / "run_results.json"
    results.write_text(
        json.dumps(
            {
                "metadata": {
                    "invocation_id": "6f0c1d3e-2a4b-4c5d-8e9f-0a1b2c3d4e5f",
                    "invocation_started_at": "2026-10-25T18:00:00Z",
                    "generated_at": "2026-10-25T18:00:09Z",
                },
                "elapsed_time": 9.0,
                "results": [
                    {"unique_id": "model.transit_platform.int_departures", "status": "success"},
                    {"unique_id": "test.transit_platform.on_time_counts", "status": "fail"},
                ],
            }
        )
    )

    assert publish_marts(migrated, AnalyticsSettings(tmp_path / "missing.duckdb"), results) == 0

    assert conn.execute(
        "SELECT models, tests, failed, published_tables, published_at FROM meta.dbt_runs"
    ).fetchall() == [(1, 1, 1, 0, None)]
