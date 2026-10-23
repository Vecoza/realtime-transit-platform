from datetime import UTC, date, datetime
from pathlib import Path

import psycopg
import pytest

from gtfs.download import FeedFile
from gtfs.loader import InvalidFeedError, LoadStatus, load_feed
from gtfs.service_time import to_utc
from tests.conftest import zip_feed

EXPECTED_ROWS = {
    "agencies": 1,
    "routes": 2,
    "stops": 4,
    "calendar": 2,
    "calendar_dates": 3,
    "shapes": 3,
    "trips": 3,
    "stop_times": 7,
    "service_dates": 15,
}


def feed_from(source_dir: Path, tmp_path: Path, name: str = "feed.zip") -> FeedFile:
    return FeedFile.from_path(zip_feed(source_dir, tmp_path / name))


def table_counts(conn: psycopg.Connection) -> dict[str, int]:
    return {
        table: conn.execute(f"SELECT count(*) FROM gtfs.{table}").fetchone()[0]
        for table in EXPECTED_ROWS
    }


def current_feed(conn: psycopg.Connection) -> tuple[int, str]:
    return conn.execute(
        "SELECT feed_id, feed_version FROM gtfs.feed_versions WHERE is_current"
    ).fetchone()


pytestmark = pytest.mark.db


def test_load_creates_a_current_feed_version_with_all_rows(
    conn: psycopg.Connection, feed_dir: Path, tmp_path: Path
) -> None:
    feed = feed_from(feed_dir, tmp_path)

    result = load_feed(conn, feed)

    assert result.status is LoadStatus.LOADED
    assert result.row_counts == EXPECTED_ROWS
    assert table_counts(conn) == EXPECTED_ROWS
    assert (result.valid_from, result.valid_to) == (date(2026, 10, 19), date(2026, 11, 1))
    assert current_feed(conn) == (result.feed_id, "small-1")
    sha256, size = conn.execute("SELECT sha256, size_bytes FROM gtfs.feed_versions").fetchone()
    assert (sha256, size) == (feed.sha256, feed.size_bytes)


def test_blank_values_become_null_and_parent_stations_resolve(
    conn: psycopg.Connection, feed_dir: Path, tmp_path: Path
) -> None:
    load_feed(conn, feed_from(feed_dir, tmp_path))

    rows = dict(conn.execute("SELECT stop_id, parent_station FROM gtfs.stops").fetchall())
    assert rows == {"1000001": None, "1050417": None, "1010108": "1000001", "1293101": None}
    assert conn.execute(
        "SELECT route_type FROM gtfs.routes WHERE route_id = '2550'"
    ).fetchone() == (702,)


def test_reloading_the_same_feed_is_a_no_op(
    conn: psycopg.Connection, feed_dir: Path, tmp_path: Path
) -> None:
    feed = feed_from(feed_dir, tmp_path)
    first = load_feed(conn, feed)

    second = load_feed(conn, feed)

    assert second.status is LoadStatus.UNCHANGED
    assert second.feed_id == first.feed_id
    assert second.row_counts == {}
    assert table_counts(conn) == EXPECTED_ROWS
    assert conn.execute("SELECT count(*) FROM gtfs.feed_versions").fetchone() == (1,)


def test_new_feed_version_becomes_current_and_old_one_is_kept(
    conn: psycopg.Connection, feed_dir: Path, tmp_path: Path
) -> None:
    first = load_feed(conn, feed_from(feed_dir, tmp_path, "first.zip"))
    info = feed_dir / "feed_info.txt"
    info.write_text(info.read_text().replace("small-1", "small-2"))

    second = load_feed(conn, feed_from(feed_dir, tmp_path, "second.zip"))

    assert second.status is LoadStatus.LOADED
    assert second.feed_id != first.feed_id
    assert current_feed(conn) == (second.feed_id, "small-2")
    per_feed = conn.execute(
        "SELECT feed_id, count(*) FROM gtfs.stop_times GROUP BY feed_id ORDER BY feed_id"
    ).fetchall()
    assert per_feed == [(first.feed_id, 7), (second.feed_id, 7)]
    partitions = conn.execute(
        "SELECT count(*) FROM pg_inherits WHERE inhparent = 'gtfs.stop_times'::regclass"
    ).fetchone()
    assert partitions == (2,)


def test_loading_an_older_known_feed_again_makes_it_current_without_copying(
    conn: psycopg.Connection, feed_dir: Path, tmp_path: Path
) -> None:
    old = feed_from(feed_dir, tmp_path, "old.zip")
    first = load_feed(conn, old)
    info = feed_dir / "feed_info.txt"
    info.write_text(info.read_text().replace("small-1", "small-2"))
    load_feed(conn, feed_from(feed_dir, tmp_path, "new.zip"))

    result = load_feed(conn, old)

    assert result.status is LoadStatus.REACTIVATED
    assert current_feed(conn) == (first.feed_id, "small-1")
    assert conn.execute("SELECT count(*) FROM gtfs.feed_versions").fetchone() == (2,)


def test_failed_load_leaves_the_current_feed_untouched(
    conn: psycopg.Connection, feed_dir: Path, tmp_path: Path
) -> None:
    good = load_feed(conn, feed_from(feed_dir, tmp_path, "good.zip"))
    stop_times = feed_dir / "stop_times.txt"
    stop_times.write_text(
        stop_times.read_text(encoding="utf-8-sig").replace("25:10:00", "25:1x:00"),
        encoding="utf-8",
    )

    with pytest.raises(psycopg.errors.InvalidTextRepresentation):
        load_feed(conn, feed_from(feed_dir, tmp_path, "bad.zip"))

    assert current_feed(conn) == (good.feed_id, "small-1")
    assert conn.execute("SELECT count(*) FROM gtfs.feed_versions").fetchone() == (1,)
    assert table_counts(conn) == EXPECTED_ROWS


def test_feed_without_a_required_file_is_rejected(
    conn: psycopg.Connection, feed_dir: Path, tmp_path: Path
) -> None:
    (feed_dir / "trips.txt").unlink()

    with pytest.raises(InvalidFeedError, match="trips.txt"):
        load_feed(conn, feed_from(feed_dir, tmp_path))
    assert conn.execute("SELECT count(*) FROM gtfs.feed_versions").fetchone() == (0,)


def test_feed_with_only_calendar_dates_is_accepted(
    conn: psycopg.Connection, feed_dir: Path, tmp_path: Path
) -> None:
    (feed_dir / "calendar.txt").unlink()

    result = load_feed(conn, feed_from(feed_dir, tmp_path))

    assert result.row_counts["calendar"] == 0
    assert result.row_counts["service_dates"] == 2
    assert (result.valid_from, result.valid_to) == (date(2026, 10, 23), date(2026, 11, 1))


def test_service_dates_apply_weekly_pattern_and_exceptions(
    conn: psycopg.Connection, feed_dir: Path, tmp_path: Path
) -> None:
    load_feed(conn, feed_from(feed_dir, tmp_path))

    def days(service_id: str) -> list[date]:
        rows = conn.execute(
            "SELECT service_date FROM gtfs.service_dates WHERE service_id = %s ORDER BY 1",
            (service_id,),
        )
        return [row[0] for row in rows]

    weekdays = days("WEEKDAY")
    assert len(weekdays) == 9
    assert date(2026, 10, 23) not in weekdays
    assert all(day.isoweekday() <= 5 for day in weekdays)
    assert days("WEEKEND") == [
        date(2026, 10, 23),
        date(2026, 10, 24),
        date(2026, 10, 25),
        date(2026, 10, 31),
        date(2026, 11, 1),
    ]
    assert days("HOLIDAY") == [date(2026, 11, 1)]


def test_stop_times_are_stored_as_offsets_past_midnight(
    conn: psycopg.Connection, feed_dir: Path, tmp_path: Path
) -> None:
    load_feed(conn, feed_from(feed_dir, tmp_path))

    rows = conn.execute("""
        SELECT stop_sequence, arrival_offset_seconds, departure_offset_seconds
        FROM gtfs.stop_times
        WHERE trip_id = '2550_WEEKEND_2350'
        ORDER BY stop_sequence
    """).fetchall()
    assert rows == [(1, 85800, 85800), (2, 88200, 88260), (3, 90600, 90600)]


def test_scheduled_instants_of_a_night_trip_on_the_fall_back_weekend(
    conn: psycopg.Connection, feed_dir: Path, tmp_path: Path
) -> None:
    load_feed(conn, feed_from(feed_dir, tmp_path))

    rows = conn.execute("""
        SELECT sd.service_date, gtfs.to_utc(sd.service_date, st.arrival_offset_seconds)
        FROM gtfs.stop_times AS st
        JOIN gtfs.trips AS t USING (feed_id, trip_id)
        JOIN gtfs.service_dates AS sd USING (feed_id, service_id)
        WHERE st.trip_id = '2550_WEEKEND_2350'
          AND st.stop_sequence = 3
          AND sd.service_date IN ('2026-10-24', '2026-10-25')
        ORDER BY 1
    """).fetchall()
    assert rows == [
        (date(2026, 10, 24), datetime(2026, 10, 24, 22, 10, tzinfo=UTC)),
        (date(2026, 10, 25), datetime(2026, 10, 25, 23, 10, tzinfo=UTC)),
    ]


@pytest.mark.parametrize(
    "service_date",
    [date(2026, 1, 15), date(2026, 3, 28), date(2026, 3, 29), date(2026, 10, 25)],
)
@pytest.mark.parametrize("offset", [0, 3 * 3600 + 30 * 60, 12 * 3600, 25 * 3600 + 600])
def test_sql_and_python_conversions_agree(migrated, service_date: date, offset: int) -> None:
    with psycopg.connect(migrated.conninfo) as conn:
        conn.execute("SET TIME ZONE 'America/New_York'")
        from_sql = conn.execute("SELECT gtfs.to_utc(%s, %s)", (service_date, offset)).fetchone()
    assert from_sql[0] == to_utc(service_date, offset)


def test_stop_time_at_an_unknown_stop_fails_the_load(
    conn: psycopg.Connection, feed_dir: Path, tmp_path: Path
) -> None:
    stop_times = feed_dir / "stop_times.txt"
    stop_times.write_text(
        stop_times.read_text(encoding="utf-8-sig").replace(",1293101,", ",9999999,"),
        encoding="utf-8",
    )

    with pytest.raises(psycopg.errors.ForeignKeyViolation, match="fk_stop_times_stop"):
        load_feed(conn, feed_from(feed_dir, tmp_path))
    assert conn.execute("SELECT count(*) FROM gtfs.feed_versions").fetchone() == (0,)
    assert conn.execute("SELECT to_regclass('gtfs.stop_times_1')").fetchone() == (None,)
