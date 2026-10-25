from collections.abc import Iterator
from datetime import date
from pathlib import Path

import duckdb
import psycopg
import pytest

from archive.lifecycle import ArchiveError, Lifecycle, connect_duckdb
from processor.decode import Event, decode
from processor.store import ensure_partitions, event_days, write_events
from tests.conftest import load_build
from tests.hfp_events import hfp_envelope
from transit_platform.config import ArchiveSettings, DatabaseSettings

TODAY = date(2026, 10, 30)
OLD_DAY = date(2026, 10, 20)
RECENT_DAY = date(2026, 10, 27)


@pytest.fixture
def autocommit(
    migrated: DatabaseSettings, conn: psycopg.Connection
) -> Iterator[psycopg.Connection]:
    with psycopg.connect(migrated.conninfo, autocommit=True) as connection:
        yield connection


@pytest.fixture
def duck(migrated: DatabaseSettings) -> Iterator[duckdb.DuckDBPyConnection]:
    with connect_duckdb(migrated) as connection:
        yield connection


@pytest.fixture
def lifecycle(
    autocommit: psycopg.Connection, duck: duckdb.DuckDBPyConnection, tmp_path: Path
) -> Lifecycle:
    return Lifecycle(autocommit, duck, ArchiveSettings(tmp_path / "archive"))


def store_days(conn: psycopg.Connection) -> None:
    envelopes = [
        hfp_envelope(tst=f"{day}T03:{minute:02d}:00Z", oday=str(day))
        for day in (OLD_DAY, RECENT_DAY)
        for minute in range(5)
    ]
    envelopes.append(hfp_envelope("arr", tst=f"{OLD_DAY}T02:40:00Z", stop=1010108))
    events = [decode(envelope) for envelope in envelopes]
    assert all(isinstance(event, Event) for event in events)
    ensure_partitions(conn, event_days(events))
    with conn.transaction():
        write_events(conn, events)
    conn.commit()


def steps(conn: psycopg.Connection, source: str, key: str) -> list[str]:
    rows = conn.execute(
        "SELECT step FROM meta.archive_runs WHERE source_table = %s AND object_key = %s"
        " ORDER BY step_id",
        (source, key),
    ).fetchall()
    return [step for (step,) in rows]


def partition_exists(conn: psycopg.Connection, name: str) -> bool:
    return conn.execute("SELECT to_regclass(%s)", (name,)).fetchone()[0] is not None


def test_each_table_keeps_its_own_number_of_days(
    conn: psycopg.Connection, lifecycle: Lifecycle, duck: duckdb.DuckDBPyConnection
) -> None:
    store_days(conn)
    stored = conn.execute(
        "SELECT tst, payload_hash FROM rt.vehicle_positions WHERE tst::date = %s ORDER BY 1",
        (OLD_DAY,),
    ).fetchall()
    conn.commit()

    lifecycle.run(TODAY)

    assert not partition_exists(lifecycle.conn, "rt.vehicle_positions_20261020")
    assert not partition_exists(lifecycle.conn, "rt.stop_events_20261020")
    assert not partition_exists(lifecycle.conn, "rt.vehicle_positions_20261027")
    assert partition_exists(lifecycle.conn, "rt.stop_events_20261027")
    assert partition_exists(lifecycle.conn, "rt.vehicle_positions_20261106")
    path = lifecycle.settings.directory / "vehicle_positions" / "utc_day=2026-10-20"
    archived = duck.execute(
        "SELECT tst, payload_hash FROM read_parquet(?) ORDER BY 1",
        [str(path / "part-0.parquet")],
    ).fetchall()
    assert [(t.astimezone(), bytes(h)) for t, h in archived] == [
        (t.astimezone(), h) for t, h in stored
    ]
    assert len(stored) == 5
    assert steps(lifecycle.conn, "rt.vehicle_positions", "2026-10-20") == [
        "exported",
        "verified",
        "detached",
        "dropped",
    ]
    assert steps(lifecycle.conn, "rt.vehicle_positions", "2026-10-27")[-1] == "dropped"
    assert steps(lifecycle.conn, "rt.stop_events", "2026-10-27") == []


def test_a_second_run_changes_nothing(conn: psycopg.Connection, lifecycle: Lifecycle) -> None:
    store_days(conn)
    lifecycle.run(TODAY)
    before = lifecycle.conn.execute("SELECT count(*) FROM meta.archive_runs").fetchone()

    lifecycle.run(TODAY)

    assert lifecycle.conn.execute("SELECT count(*) FROM meta.archive_runs").fetchone() == before


def test_run_stopped_after_the_export_resumes_without_exporting_again(
    conn: psycopg.Connection, lifecycle: Lifecycle, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_days(conn)
    record = Lifecycle.record

    def crash_on_detach(self: Lifecycle, source: str, key: str, step: str, **kwargs) -> None:
        if step == "detached":
            raise RuntimeError("stopped")
        record(self, source, key, step, **kwargs)

    monkeypatch.setattr(Lifecycle, "record", crash_on_detach)
    with pytest.raises(RuntimeError):
        lifecycle.run(TODAY)
    assert partition_exists(lifecycle.conn, "rt.vehicle_positions_20261020")
    monkeypatch.setattr(Lifecycle, "record", record)

    lifecycle.run(TODAY)

    assert not partition_exists(lifecycle.conn, "rt.vehicle_positions_20261020")
    assert steps(lifecycle.conn, "rt.vehicle_positions", "2026-10-20") == [
        "exported",
        "verified",
        "detached",
        "dropped",
    ]


def test_export_that_does_not_match_postgres_is_never_dropped(
    conn: psycopg.Connection, lifecycle: Lifecycle, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_days(conn)
    export = Lifecycle.export
    monkeypatch.setattr(
        Lifecycle,
        "export",
        lambda self, query, target, source, key: export(
            self, query + " LIMIT 1", target, source, key
        ),
    )

    with pytest.raises(ArchiveError):
        lifecycle.run(TODAY)

    assert partition_exists(lifecycle.conn, "rt.vehicle_positions_20261020")
    assert steps(lifecycle.conn, "rt.vehicle_positions", "2026-10-20") == ["exported", "failed"]


def test_feeds_are_exported_when_loaded_and_dropped_once_replaced_long_enough(
    conn: psycopg.Connection,
    lifecycle: Lifecycle,
    duck: duckdb.DuckDBPyConnection,
    feed_dir: Path,
    tmp_path: Path,
) -> None:
    old = load_build(conn, feed_dir, tmp_path, "2026-10-01")
    replaced_recently = load_build(conn, feed_dir, tmp_path, "2026-10-12")
    current = load_build(conn, feed_dir, tmp_path, "2026-10-26")
    conn.commit()
    stop_times = conn.execute(
        "SELECT count(*) FROM gtfs.stop_times WHERE feed_id = %s", (old,)
    ).fetchone()[0]
    conn.commit()

    lifecycle.run(TODAY)

    feeds = [
        row[0]
        for row in lifecycle.conn.execute("SELECT feed_id FROM gtfs.feed_versions ORDER BY 1")
    ]
    assert feeds == [replaced_recently, current]
    assert not partition_exists(lifecycle.conn, f"gtfs.stop_times_{old}")
    archived = lifecycle.settings.directory / "gtfs" / "stop_times" / f"feed_id={old}"
    assert duck.execute(
        "SELECT count(*) FROM read_parquet(?)", [str(archived / "part-0.parquet")]
    ).fetchone() == (stop_times,)
    for feed_id in (old, replaced_recently, current):
        assert steps(lifecycle.conn, "gtfs.trips", str(feed_id))[-1] == "verified"
    assert steps(lifecycle.conn, "gtfs.feed_versions", str(old)) == ["dropped"]


def test_a_run_while_another_is_active_does_nothing(
    conn: psycopg.Connection, lifecycle: Lifecycle, migrated: DatabaseSettings
) -> None:
    store_days(conn)
    with psycopg.connect(migrated.conninfo, autocommit=True) as other:
        other.execute("SELECT pg_advisory_lock(hashtext('archive.lifecycle'))")

        assert lifecycle.run(TODAY) is False

    assert partition_exists(lifecycle.conn, "rt.vehicle_positions_20261020")
    assert lifecycle.run(TODAY) is True
