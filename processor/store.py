"""Batch writes of validated events: COPY into staging, then match and insert in SQL.

Matching runs once per distinct journey in the batch, not once per event: a journey is the
durable HFP identity (route, direction, operating day, scheduled start). The trip is looked
up in the feed that was current on the operating day.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, timedelta

import psycopg

from processor.decode import Event

COMMON_COLUMNS = (
    "tst",
    "operator_id",
    "vehicle_number",
    "payload_hash",
    "received_at",
    "transport_mode",
    "running_operator_id",
    "route_id",
    "direction_id",
    "oday",
    "start_time",
    "start_offset_seconds",
    "live_delay_s",
    "lat",
    "lon",
    "source_partition",
    "source_offset",
)
POSITION_COLUMNS = (
    *COMMON_COLUMNS,
    "speed_mps",
    "heading",
    "stop_id",
    "next_stop_id",
    "location_source",
)
STOP_EVENT_COLUMNS = (*COMMON_COLUMNS, "event_type", "stop_id", "hfp_planned_time")

STAGE_SQL = """
    CREATE TEMPORARY TABLE staged_positions
        (LIKE rt.vehicle_positions INCLUDING DEFAULTS) ON COMMIT DROP;
    ALTER TABLE staged_positions ALTER COLUMN match_status DROP NOT NULL;
    CREATE TEMPORARY TABLE staged_stop_events
        (LIKE rt.stop_events INCLUDING DEFAULTS) ON COMMIT DROP;
    ALTER TABLE staged_stop_events ALTER COLUMN match_status DROP NOT NULL;
"""

JOURNEY_MATCH_CTE = """
    journeys AS (
        SELECT DISTINCT route_id, direction_id, oday, start_offset_seconds
        FROM {staged}
        WHERE route_id IS NOT NULL AND direction_id IS NOT NULL
          AND oday IS NOT NULL AND start_offset_seconds IS NOT NULL
    ),
    feeds AS MATERIALIZED (
        SELECT oday, gtfs.feed_for_service_date(oday) AS feed_id
        FROM (SELECT DISTINCT oday FROM journeys) AS days
    ),
    matches AS MATERIALIZED (
        SELECT j.*, f.feed_id, trip.trip_id,
               CASE
                   WHEN f.feed_id IS NULL THEN 'no_feed'
                   WHEN NOT EXISTS (
                       SELECT 1 FROM gtfs.routes AS r
                       WHERE r.feed_id = f.feed_id AND r.route_id = j.route_id
                   ) THEN 'route_not_in_feed'
                   WHEN trip.trip_id IS NULL THEN 'no_trip'
                   ELSE 'matched'
               END AS journey_status
        FROM journeys AS j
        JOIN feeds AS f USING (oday)
        LEFT JOIN LATERAL (
            SELECT t.trip_id
            FROM gtfs.trips AS t
            JOIN gtfs.service_dates AS sd
              ON sd.feed_id = t.feed_id
             AND sd.service_id = t.service_id
             AND sd.service_date = j.oday
            WHERE t.feed_id = f.feed_id
              AND t.route_id = j.route_id
              AND t.direction_id = j.direction_id
              AND t.start_offset_seconds BETWEEN j.start_offset_seconds
                                             AND j.start_offset_seconds + 59
            ORDER BY t.start_offset_seconds, t.trip_id
            LIMIT 1
        ) AS trip ON true
    )
"""

INSERT_POSITIONS_SQL = (
    "WITH "
    + JOURNEY_MATCH_CTE.format(staged="staged_positions")
    + """,
    inserted AS (
        INSERT INTO rt.vehicle_positions (
            tst, operator_id, vehicle_number, payload_hash, received_at, transport_mode,
            running_operator_id, route_id, direction_id, oday, start_time,
            start_offset_seconds, feed_id, trip_id, match_status, lat, lon, speed_mps,
            heading, live_delay_s, stop_id, next_stop_id, location_source,
            source_partition, source_offset
        )
        SELECT s.tst, s.operator_id, s.vehicle_number, s.payload_hash, s.received_at,
               s.transport_mode, s.running_operator_id, s.route_id, s.direction_id, s.oday,
               s.start_time, s.start_offset_seconds, m.feed_id, m.trip_id,
               coalesce(m.journey_status, 'missing_journey'),
               s.lat, s.lon, s.speed_mps, s.heading, s.live_delay_s, s.stop_id,
               s.next_stop_id, s.location_source, s.source_partition, s.source_offset
        FROM staged_positions AS s
        LEFT JOIN matches AS m USING (route_id, direction_id, oday, start_offset_seconds)
        ON CONFLICT DO NOTHING
        RETURNING match_status, stored_at - tst AS latency
    )
    SELECT count(*),
           count(*) FILTER (WHERE match_status <> 'matched'),
           array_agg(latency)
    FROM inserted
"""
)

INSERT_STOP_EVENTS_SQL = (
    "WITH "
    + JOURNEY_MATCH_CTE.format(staged="staged_stop_events")
    + """,
    located AS (
        SELECT s.*, m.feed_id AS matched_feed_id, m.trip_id AS matched_trip_id,
               m.journey_status, st.stop_sequence AS matched_stop_sequence,
               st.planned_time AS matched_planned_time
        FROM staged_stop_events AS s
        LEFT JOIN matches AS m USING (route_id, direction_id, oday, start_offset_seconds)
        LEFT JOIN LATERAL (
            SELECT stop_times.stop_sequence,
                   gtfs.to_utc(
                       s.oday,
                       CASE s.event_type
                           WHEN 'arr' THEN stop_times.arrival_offset_seconds
                           ELSE stop_times.departure_offset_seconds
                       END
                   ) AS planned_time
            FROM gtfs.stop_times
            WHERE stop_times.feed_id = m.feed_id
              AND stop_times.trip_id = m.trip_id
              AND stop_times.stop_id = s.stop_id
            ORDER BY abs(extract(EPOCH FROM gtfs.to_utc(
                s.oday,
                CASE s.event_type
                    WHEN 'arr' THEN stop_times.arrival_offset_seconds
                    ELSE stop_times.departure_offset_seconds
                END
            ) - s.tst))
            LIMIT 1
        ) AS st ON m.journey_status = 'matched' AND s.stop_id IS NOT NULL
    ),
    inserted AS (
        INSERT INTO rt.stop_events (
            tst, operator_id, vehicle_number, event_type, payload_hash, received_at,
            transport_mode, running_operator_id, route_id, direction_id, oday, start_time,
            start_offset_seconds, feed_id, trip_id, stop_id, stop_sequence, match_status,
            planned_time, measured_delay_s, hfp_planned_time, live_delay_s, lat, lon,
            source_partition, source_offset
        )
        SELECT tst, operator_id, vehicle_number, event_type, payload_hash, received_at,
               transport_mode, running_operator_id, route_id, direction_id, oday, start_time,
               start_offset_seconds, matched_feed_id, matched_trip_id, stop_id,
               matched_stop_sequence,
               CASE
                   WHEN journey_status IS NULL THEN 'missing_journey'
                   WHEN journey_status <> 'matched' THEN journey_status
                   WHEN stop_id IS NULL THEN 'missing_stop'
                   WHEN matched_stop_sequence IS NULL THEN 'stop_not_on_trip'
                   ELSE 'matched'
               END,
               matched_planned_time,
               round(extract(EPOCH FROM tst - matched_planned_time))::INTEGER,
               hfp_planned_time, live_delay_s, lat, lon, source_partition, source_offset
        FROM located
        ON CONFLICT DO NOTHING
        RETURNING match_status, stored_at - tst AS latency
    )
    SELECT count(*),
           count(*) FILTER (WHERE match_status <> 'matched'),
           array_agg(latency)
    FROM inserted
"""
)


@dataclass(frozen=True)
class WriteResult:
    staged: int
    inserted: int
    unmatched: int
    latencies_ms: list[int]
    copies_in_batch: int = 0

    @property
    def duplicates(self) -> int:
        """Copies dropped before the insert plus rows whose key was already stored."""
        return self.copies_in_batch + self.staged - self.inserted

    def with_copies(self, copies: int) -> WriteResult:
        return replace(self, copies_in_batch=copies)


def event_days(events: Iterable[Event]) -> set[date]:
    return {event.tst.astimezone(UTC).date() for event in events}


def upcoming_days(today: date, ahead: int = 7) -> list[date]:
    """Today and the next ``ahead`` days, so partitions exist before the first event arrives."""
    return [today + timedelta(days=n) for n in range(ahead + 1)]


def ensure_partitions(conn: psycopg.Connection, days: Iterable[date]) -> None:
    """Create missing daily partitions in their own short transaction."""
    with conn.transaction():
        for day in sorted(days):
            conn.execute("SELECT rt.ensure_day_partitions(%s, %s)", (day, day))


def write_events(conn: psycopg.Connection, events: Sequence[Event]) -> WriteResult:
    """Insert one batch inside the caller's transaction; duplicates are skipped by key."""
    positions = [e for e in events if e.event_type == "vp"]
    stop_events = [e for e in events if e.event_type != "vp"]
    conn.execute(STAGE_SQL)
    copy_rows(conn, "staged_positions", POSITION_COLUMNS, map(position_row, positions))
    copy_rows(conn, "staged_stop_events", STOP_EVENT_COLUMNS, map(stop_event_row, stop_events))

    inserted = unmatched = 0
    latencies: list[int] = []
    batches = ((INSERT_POSITIONS_SQL, positions), (INSERT_STOP_EVENTS_SQL, stop_events))
    for statement, rows in batches:
        if not rows:
            continue
        count, not_matched, lags = conn.execute(statement).fetchone()
        inserted += count
        unmatched += not_matched
        latencies += [round(lag.total_seconds() * 1000) for lag in lags or ()]
    return WriteResult(len(events), inserted, unmatched, latencies)


def copy_rows(
    conn: psycopg.Connection, table: str, columns: Sequence[str], rows: Iterable[tuple]
) -> None:
    with conn.cursor().copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)


def common_values(event: Event) -> tuple:
    p = event.payload
    return (
        event.tst,
        event.operator_id,
        event.vehicle_number,
        event.payload_hash,
        event.received_at,
        event.transport_mode,
        p.oper,
        p.route,
        event.direction_id,
        p.oday,
        p.start,
        event.start_offset_seconds,
        event.live_delay_s,
        p.lat,
        p.long,
        event.source_partition,
        event.source_offset,
    )


def position_row(event: Event) -> tuple:
    p = event.payload
    return (*common_values(event), p.spd, p.hdg, p.stop, event.next_stop_id, p.loc)


def stop_event_row(event: Event) -> tuple:
    p = event.payload
    planned = p.ttarr if event.event_type == "arr" else p.ttdep
    return (*common_values(event), event.event_type, p.stop, planned)
