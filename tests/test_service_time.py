from datetime import UTC, date, datetime

import pytest

from gtfs.service_time import parse_gtfs_time, service_day_start, to_utc


@pytest.mark.parametrize(
    ("value", "seconds"),
    [
        ("00:00:00", 0),
        ("05:37:00", 20220),
        ("5:37:00", 20220),
        ("24:00:00", 86400),
        ("25:10:30", 90630),
        (" 29:47:00 ", 107220),
    ],
)
def test_parse_gtfs_time_accepts_times_past_midnight(value: str, seconds: int) -> None:
    assert parse_gtfs_time(value) == seconds


@pytest.mark.parametrize("value", ["", "5:37", "05:60:00", "05:00:61", "aa:bb:cc", "-1:00:00"])
def test_parse_gtfs_time_rejects_malformed_values(value: str) -> None:
    with pytest.raises(ValueError):
        parse_gtfs_time(value)


def test_service_day_starts_at_local_midnight_on_ordinary_days() -> None:
    assert service_day_start(date(2026, 1, 15)) == datetime(2026, 1, 14, 22, 0, tzinfo=UTC)
    assert service_day_start(date(2026, 7, 15)) == datetime(2026, 7, 14, 21, 0, tzinfo=UTC)


def test_trip_after_midnight_belongs_to_previous_service_day() -> None:
    assert to_utc(date(2026, 10, 20), parse_gtfs_time("25:10:00")) == datetime(
        2026, 10, 20, 22, 10, tzinfo=UTC
    )


def test_spring_forward_day_counts_from_noon_minus_twelve_hours() -> None:
    # 2026-03-29: clocks go from 03:00 EET to 04:00 EEST.
    assert service_day_start(date(2026, 3, 29)) == datetime(2026, 3, 28, 21, 0, tzinfo=UTC)
    assert to_utc(date(2026, 3, 29), parse_gtfs_time("05:00:00")) == datetime(
        2026, 3, 29, 2, 0, tzinfo=UTC
    )
    assert to_utc(date(2026, 3, 29), parse_gtfs_time("25:00:00")) == datetime(
        2026, 3, 29, 22, 0, tzinfo=UTC
    )


def test_fall_back_day_counts_from_noon_minus_twelve_hours() -> None:
    # 2026-10-25: clocks go from 04:00 EEST back to 03:00 EET.
    assert service_day_start(date(2026, 10, 25)) == datetime(2026, 10, 24, 22, 0, tzinfo=UTC)
    assert to_utc(date(2026, 10, 25), parse_gtfs_time("05:00:00")) == datetime(
        2026, 10, 25, 3, 0, tzinfo=UTC
    )


def test_night_trip_crossing_the_fall_back_change_keeps_wall_clock_time() -> None:
    # Service day 2026-10-24, 25:10 = 01:10 EEST on 25 October, before clocks change.
    assert to_utc(date(2026, 10, 24), parse_gtfs_time("25:10:00")) == datetime(
        2026, 10, 24, 22, 10, tzinfo=UTC
    )
