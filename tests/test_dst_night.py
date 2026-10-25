"""The night the clocks go back: 25 October 2026, 04:00 EEST becomes 03:00 EET (01:00 UTC).

The recorded messages come from the start of that night (23:30–23:51 UTC, before the change).
The repeated hour itself is covered by arrivals built from two real timetable trips of route
1009N that both start at 03:13 on the wall clock, one before and one after the change.
"""

import csv
from datetime import date, timedelta
from pathlib import Path

import psycopg
import pytest

from gtfs.download import FeedFile
from gtfs.loader import load_feed
from gtfs.service_time import HELSINKI, to_utc
from processor.decode import Event, decode
from processor.store import ensure_partitions, event_days, write_events
from producer.hfp import Envelope
from producer.recording import read_envelopes
from tests.conftest import REPO_ROOT, zip_feed
from tests.hfp_events import hfp_envelope

FIXTURES = REPO_ROOT / "tests" / "fixtures"
ODAY = date(2026, 10, 24)


@pytest.fixture
def night_feed(conn: psycopg.Connection, tmp_path: Path) -> int:
    archive = zip_feed(FIXTURES / "gtfs_dst_night", tmp_path / "feed.zip")
    return load_feed(conn, FeedFile.from_path(archive)).feed_id


def store(conn: psycopg.Connection, envelopes: list[Envelope]) -> None:
    events = [decode(envelope) for envelope in envelopes]
    assert all(isinstance(event, Event) for event in events)
    ensure_partitions(conn, event_days(events))
    with conn.transaction():
        write_events(conn, events)


def stored(conn: psycopg.Connection, table: str) -> list[dict]:
    cursor = conn.execute(f"SELECT * FROM rt.{table} ORDER BY tst")
    columns = [c.name for c in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def approximate_stops(trip_ids: set[str]) -> set[tuple[str, str]]:
    """(trip, stop) pairs whose GTFS times are approximate (``timepoint`` 0)."""
    with (FIXTURES / "gtfs_dst_night" / "stop_times.txt").open(encoding="utf-8-sig") as f:
        return {
            (row["trip_id"], row["stop_id"])
            for row in csv.DictReader(f)
            if row["trip_id"] in trip_ids and row["timepoint"] == "0"
        }


def test_recorded_night_trips_are_stored_on_their_operating_day(
    conn: psycopg.Connection, night_feed: int
) -> None:
    envelopes = list(read_envelopes([FIXTURES / "hfp_dst_night.jsonl"]))
    store(conn, envelopes)

    rows = stored(conn, "vehicle_positions") + stored(conn, "stop_events")
    assert len(rows) == len({(e.topic, e.payload) for e in envelopes})
    assert {row["match_status"] for row in rows} == {"matched"}
    assert {row["oday"] for row in rows} == {ODAY}
    assert all(row["start_offset_seconds"] >= 24 * 3600 for row in rows)
    for row in rows:
        local = row["tst"].astimezone(HELSINKI)
        assert (local.date(), local.hour, local.utcoffset()) == (
            date(2026, 10, 25),
            2,
            timedelta(hours=3),
        )

    stops = [row for row in rows if "event_type" in row]
    approximate = approximate_stops({row["trip_id"] for row in stops})
    differing = [row for row in stops if row["planned_time"] != row["hfp_planned_time"]]
    assert len(stops) > 200
    assert all((row["trip_id"], row["stop_id"]) in approximate for row in differing)


@pytest.mark.parametrize(
    ("trip_id", "planned_offset", "utc_hour"),
    [
        ("1009N_20261023_La_1_2713", 27 * 3600 + 960, 0),
        ("1009N_20261023_La_1_2813", 28 * 3600 + 960, 1),
    ],
)
def test_night_trips_in_both_repeated_hours_match_their_planned_times(
    conn: psycopg.Connection, night_feed: int, trip_id: str, planned_offset: int, utc_hour: int
) -> None:
    planned = to_utc(ODAY, planned_offset)
    arrived = planned + timedelta(seconds=40)
    store(
        conn,
        [
            hfp_envelope(
                "arr",
                tst=arrived.isoformat().replace("+00:00", "Z"),
                mode="bus",
                operator="0018",
                route="1009N",
                dir="1",
                oday=ODAY.isoformat(),
                start="03:13",
                stop=1203420,
                ttarr=planned.isoformat().replace("+00:00", "Z"),
                dl=-40,
            )
        ],
    )

    (row,) = stored(conn, "stop_events")
    local = row["tst"].astimezone(HELSINKI)
    assert (row["tst"].hour, local.hour, local.minute) == (utc_hour, 3, 16)
    assert local.utcoffset() == timedelta(hours=3 - utc_hour)
    assert (row["oday"], row["trip_id"], row["match_status"]) == (ODAY, trip_id, "matched")
    assert row["planned_time"] == planned == row["hfp_planned_time"]
    assert row["measured_delay_s"] == row["live_delay_s"] == 40
