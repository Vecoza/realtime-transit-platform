from datetime import UTC, date, datetime
from pathlib import Path

import psycopg
import pytest

from gtfs.download import FeedFile
from gtfs.loader import load_feed
from processor.decode import Event, decode
from processor.store import ensure_partitions, event_days, upcoming_days, write_events
from producer.hfp import Envelope
from tests.conftest import GTFS_FIXTURE, zip_feed
from tests.hfp_events import hfp_envelope

pytestmark = pytest.mark.db

NIGHT_TRIP = {"route": "2550", "dir": "2", "oday": "2026-10-24", "start": "23:50"}


@pytest.fixture
def feed_id(conn: psycopg.Connection, tmp_path: Path) -> int:
    return load_feed(
        conn, FeedFile.from_path(zip_feed(GTFS_FIXTURE, tmp_path / "feed.zip"))
    ).feed_id


def store(conn: psycopg.Connection, *envelopes: Envelope):
    events = [decode(envelope) for envelope in envelopes]
    assert all(isinstance(event, Event) for event in events)
    ensure_partitions(conn, event_days(events))
    with conn.transaction():
        return write_events(conn, events)


def rows(conn: psycopg.Connection, table: str) -> list[dict]:
    cursor = conn.execute(f"SELECT * FROM rt.{table} ORDER BY tst")
    columns = [c.name for c in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def position(conn: psycopg.Connection) -> list[dict]:
    return rows(conn, "vehicle_positions")


def stop_events(conn: psycopg.Connection) -> list[dict]:
    return rows(conn, "stop_events")


def test_position_is_matched_to_the_trip_of_its_journey(
    conn: psycopg.Connection, feed_id: int
) -> None:
    result = store(conn, hfp_envelope())

    (row,) = position(conn)
    assert (result.inserted, result.unmatched) == (1, 0)
    assert row["match_status"] == "matched"
    assert (row["feed_id"], row["trip_id"]) == (feed_id, "1001_WEEKDAY_0537")
    assert (row["route_id"], row["direction_id"], row["start_offset_seconds"]) == ("1001", 0, 20220)
    assert row["oday"].isoformat() == "2026-10-20"


@pytest.mark.parametrize(
    ("fields", "status"),
    [
        ({"route": "9999"}, "route_not_in_feed"),
        ({"start": "06:00"}, "no_trip"),
        ({"oday": "2026-10-23", "tst": "2026-10-23T02:38:00.000Z"}, "no_trip"),
        ({"oday": None}, "missing_journey"),
        ({"start": None}, "missing_journey"),
        ({"oday": "2027-01-05", "tst": "2027-01-05T03:38:00.000Z"}, "no_feed"),
    ],
)
def test_unmatched_positions_are_kept_with_a_reason(
    conn: psycopg.Connection, feed_id: int, fields: dict, status: str
) -> None:
    result = store(conn, hfp_envelope(**fields))

    (row,) = position(conn)
    assert result.unmatched == 1
    assert row["match_status"] == status
    assert row["trip_id"] is None


def test_arrival_after_midnight_gets_its_planned_time_and_own_delay(
    conn: psycopg.Connection, feed_id: int
) -> None:
    store(
        conn,
        hfp_envelope(
            "arr",
            tst="2026-10-24T22:12:00.000Z",
            stop=1050417,
            ttarr="2026-10-24T22:10:00.000Z",
            dl=-115,
            **NIGHT_TRIP,
        ),
        hfp_envelope(
            "dep",
            tst="2026-10-24T21:32:30.000Z",
            stop=1010108,
            ttdep="2026-10-24T21:31:00.000Z",
            dl=-80,
            **NIGHT_TRIP,
        ),
    )

    departure, arrival = stop_events(conn)
    assert arrival["match_status"] == "matched"
    assert (arrival["trip_id"], arrival["stop_sequence"]) == ("2550_WEEKEND_2350", 3)
    assert arrival["planned_time"] == datetime(2026, 10, 24, 22, 10, tzinfo=UTC)
    assert arrival["planned_time"] == arrival["hfp_planned_time"]
    assert arrival["measured_delay_s"] == 120
    assert arrival["live_delay_s"] == 115
    assert departure["stop_sequence"] == 2
    assert departure["planned_time"] == datetime(2026, 10, 24, 21, 31, tzinfo=UTC)
    assert departure["measured_delay_s"] == 90


@pytest.mark.parametrize(
    ("stop", "status"),
    [(1293101, "stop_not_on_trip"), (None, "missing_stop")],
)
def test_stop_events_on_a_matched_trip_without_its_stop_keep_a_reason(
    conn: psycopg.Connection, feed_id: int, stop: int | None, status: str
) -> None:
    store(conn, hfp_envelope("arr", stop=stop))

    (row,) = stop_events(conn)
    assert row["match_status"] == status
    assert row["trip_id"] == "1001_WEEKDAY_0537"
    assert row["planned_time"] is None and row["measured_delay_s"] is None


def test_exact_copies_are_dropped_and_distinct_positions_in_one_second_are_kept(
    conn: psycopg.Connection, feed_id: int
) -> None:
    same_second = "2026-10-20T02:38:00Z"
    first = store(
        conn,
        hfp_envelope(tst=same_second),
        hfp_envelope(tst=same_second),
        hfp_envelope(tst=same_second, lat=60.171),
    )
    second = store(conn, hfp_envelope(tst=same_second))

    assert (first.staged, first.inserted, first.duplicates) == (3, 2, 1)
    assert (second.inserted, second.duplicates) == (0, 1)
    assert len(position(conn)) == 2


def test_rows_land_in_the_partition_of_their_utc_event_day(
    conn: psycopg.Connection, feed_id: int
) -> None:
    store(conn, hfp_envelope("arr", tst="2026-10-24T22:12:00.000Z", stop=1050417, **NIGHT_TRIP))

    partition, oday = conn.execute(
        "SELECT tableoid::regclass::TEXT, oday FROM rt.stop_events"
    ).fetchone()
    assert partition == "rt.stop_events_20261024"
    assert oday.isoformat() == "2026-10-24"
    assert conn.execute("SELECT count(*) FROM rt.stop_events_default").fetchone() == (0,)


def load_build(conn: psycopg.Connection, feed_dir: Path, tmp_path: Path, built: str) -> int:
    info = feed_dir / "feed_info.txt"
    lines = info.read_text().splitlines()
    lines[1] = lines[1].rsplit(",", 1)[0] + f",{built} 04:11:14"
    info.write_text("\n".join(lines) + "\n")
    feed = FeedFile.from_path(zip_feed(feed_dir, tmp_path / f"{built}.zip"))
    return load_feed(conn, feed).feed_id


def test_feed_built_before_the_operating_day_is_used(
    conn: psycopg.Connection, feed_dir: Path, tmp_path: Path
) -> None:
    first = load_build(conn, feed_dir, tmp_path, "2026-10-19")
    second = load_build(conn, feed_dir, tmp_path, "2026-10-22")
    conn.commit()

    store(conn, hfp_envelope(), hfp_envelope(oday="2026-10-26", tst="2026-10-26T03:38:00Z"))

    feeds = [row["feed_id"] for row in position(conn)]
    assert feeds == [first, second]
    assert conn.execute(
        "SELECT build_date FROM gtfs.feed_versions WHERE feed_id = %s", (first,)
    ).fetchone() == (date(2026, 10, 19),)


def test_backfilled_older_build_is_used_for_its_own_days_only(
    conn: psycopg.Connection, feed_dir: Path, tmp_path: Path
) -> None:
    first = load_build(conn, feed_dir, tmp_path, "2026-10-19")
    third = load_build(conn, feed_dir, tmp_path, "2026-10-25")
    second = load_build(conn, feed_dir, tmp_path, "2026-10-22")
    conn.commit()

    store(
        conn,
        hfp_envelope(),
        hfp_envelope(oday="2026-10-23", tst="2026-10-23T03:38:00Z"),
        hfp_envelope(oday="2026-10-26", tst="2026-10-26T03:38:00Z"),
    )

    feeds = [row["feed_id"] for row in position(conn)]
    assert feeds == [first, second, third]


def test_without_a_build_date_the_load_time_decides(
    conn: psycopg.Connection, feed_dir: Path, tmp_path: Path
) -> None:
    first = load_build(conn, feed_dir, tmp_path, "2026-10-19")
    second = load_build(conn, feed_dir, tmp_path, "2026-10-22")
    conn.execute("UPDATE gtfs.feed_versions SET build_date = NULL")
    conn.execute(
        "UPDATE gtfs.feed_versions SET loaded_at = %s WHERE feed_id = %s",
        (datetime(2026, 10, 19, 12, tzinfo=UTC), first),
    )
    conn.execute(
        "UPDATE gtfs.feed_versions SET loaded_at = %s WHERE feed_id = %s",
        (datetime(2026, 10, 22, 12, tzinfo=UTC), second),
    )
    conn.commit()

    store(conn, hfp_envelope(), hfp_envelope(oday="2026-10-26", tst="2026-10-26T03:38:00Z"))

    feeds = [row["feed_id"] for row in position(conn)]
    assert feeds == [first, second]


def test_partitions_are_created_ahead_and_creation_is_idempotent(
    conn: psycopg.Connection,
) -> None:
    days = upcoming_days(date(2030, 1, 30))

    ensure_partitions(conn, days)
    ensure_partitions(conn, days)

    names = conn.execute(
        "SELECT relname FROM pg_class WHERE relname ~ '_20300(1|2)[0-9]{2}$' ORDER BY 1"
    ).fetchall()
    assert len(days) == 8
    assert [name for (name,) in names][:2] == [
        "route_minute_stats_20300130",
        "route_minute_stats_20300131",
    ]
    assert len(names) == 4 * 8
