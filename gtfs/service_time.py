"""Conversion of GTFS service-day times to UTC instants.

GTFS times such as ``25:10:00`` are offsets from "noon minus 12 hours" on the service date,
in the agency's time zone. That reference point equals local midnight on ordinary days and
differs by one hour on the days daylight saving time starts or ends.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

HELSINKI = ZoneInfo("Europe/Helsinki")

_GTFS_TIME = re.compile(r"^(\d{1,3}):([0-5]\d):([0-5]\d)$")


def parse_gtfs_time(value: str) -> int:
    """Seconds since the start of the service day for a GTFS ``H:MM:SS`` time (may exceed 24h)."""
    match = _GTFS_TIME.match(value.strip())
    if match is None:
        raise ValueError(f"invalid GTFS time: {value!r}")
    hours, minutes, seconds = (int(part) for part in match.groups())
    return hours * 3600 + minutes * 60 + seconds


def service_day_start(service_date: date, tz: ZoneInfo = HELSINKI) -> datetime:
    """The UTC instant that GTFS offsets on ``service_date`` count from."""
    local_noon = datetime.combine(service_date, time(12), tzinfo=tz)
    return local_noon.astimezone(UTC) - timedelta(hours=12)


def to_utc(service_date: date, offset_seconds: int, tz: ZoneInfo = HELSINKI) -> datetime:
    """UTC instant of a scheduled time given as an offset on a service date."""
    return service_day_start(service_date, tz) + timedelta(seconds=offset_seconds)
