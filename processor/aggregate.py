"""Event-time aggregator: per-minute route and stop statistics, late corrections, stale vehicles.

A second consumer group on the raw topic, independent of the storage sink. Each batch:

1. consume, drop copies within the batch, validate, and add the events to the windows;
2. move the watermark: emit closed windows and corrections, mark silent vehicles stale or
   finished (looking up each new journey's planned end in the timetable once);
3. in one database transaction, upsert the window results as full values, log late events,
   update vehicle status, and store the consumed offsets, per-partition event time and
   watermark;
4. commit the offsets to Kafka as well, where they show the group's lag.

The offsets in the database are the ones that count: on start the consumer seeks to them, so
no event is added twice and none is skipped. The windows themselves live in memory. On
restart they are rebuilt from the rows the storage sink stored, taking exactly the events
below the aggregator's offsets, after waiting until the sink has stored all of those.
"""

from __future__ import annotations

import logging
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

import psycopg
from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    ConsumerGroupTopicPartitions,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient

from processor.decode import Event, Rejection, decode
from processor.sink import Hooks, consumer_lag, parse_envelope
from processor.store import JOURNEY_MATCH_CTE, copy_rows, ensure_partitions, upcoming_days
from processor.windows import (
    MINUTE,
    Emission,
    Journey,
    LateEvent,
    Outcome,
    RouteMinute,
    RouteWindow,
    StopMinute,
    StopWindow,
    VehicleState,
    Watermark,
    WindowAggregator,
    WindowKey,
    WindowRules,
    event_key,
    vehicle_id,
)
from producer.hfp import Envelope
from transit_platform.config import AggregatorSettings, DatabaseSettings, StreamSettings
from transit_platform.logs import configure_logging
from transit_platform.shutdown import stop_event_on_signals

logger = logging.getLogger(__name__)

ROUTE_COLUMNS = (
    "route_id",
    "direction_id",
    "window_start",
    "vehicles",
    "positions",
    "live_delay_samples",
    "avg_live_delay_s",
    "p90_live_delay_s",
    "late_events",
)
STOP_COLUMNS = (
    "stop_id",
    "window_start",
    "arrivals",
    "live_delay_samples",
    "avg_live_delay_s",
    "late_events",
)
VEHICLE_COLUMNS = (
    "operator_id",
    "vehicle_number",
    "last_tst",
    "last_received_at",
    "transport_mode",
    "route_id",
    "direction_id",
    "oday",
    "start_time",
    "lat",
    "lon",
    "planned_end",
    "status",
    "silent_since",
)
LATE_COLUMNS = (
    "operator_id",
    "vehicle_number",
    "event_type",
    "tst",
    "payload_hash",
    "received_at",
    "window_start",
    "route_id",
    "direction_id",
    "stop_id",
    "watermark",
    "outcome",
)


def upsert_sql(table: str, columns: tuple[str, ...], key: tuple[str, ...]) -> str:
    """Full-value upsert: a window is rewritten only when a value changed."""
    values = [column for column in columns if column not in key]
    return f"""
        INSERT INTO rt.{table} AS s ({", ".join(columns)})
        SELECT {", ".join(columns)} FROM staged_{table}
        ON CONFLICT ({", ".join(key)}) DO UPDATE
        SET {", ".join(f"{column} = excluded.{column}" for column in values)},
            revision = s.revision + 1,
            emitted_at = clock_timestamp()
        WHERE ({", ".join(f"s.{column}" for column in values)})
            IS DISTINCT FROM ({", ".join(f"excluded.{column}" for column in values)})
        RETURNING s.revision = 1 AS inserted
    """


UPSERT_ROUTES = upsert_sql(
    "route_minute_stats", ROUTE_COLUMNS, ("route_id", "direction_id", "window_start")
)
UPSERT_STOPS = upsert_sql("stop_minute_stats", STOP_COLUMNS, ("stop_id", "window_start"))
UPSERT_VEHICLES = f"""
    INSERT INTO rt.vehicle_status AS v ({", ".join(VEHICLE_COLUMNS)})
    SELECT {", ".join(VEHICLE_COLUMNS)} FROM staged_vehicle_status
    ON CONFLICT (operator_id, vehicle_number) DO UPDATE
    SET {", ".join(f"{c} = excluded.{c}" for c in VEHICLE_COLUMNS[2:])},
        updated_at = clock_timestamp()
    WHERE excluded.last_tst >= v.last_tst
"""
# A dropped copy excludes the event only if it was the first copy, the one the sink stored.
DROPPED_FILTER = """
    NOT EXISTS (
        SELECT 1 FROM rt.late_events AS l
        WHERE l.outcome = 'dropped'
          AND l.operator_id = e.operator_id AND l.vehicle_number = e.vehicle_number
          AND l.event_type = {event_type} AND l.tst = e.tst AND l.payload_hash = e.payload_hash
          AND l.received_at = e.received_at
    )
"""
# Only events the aggregator had consumed: the sink may already have stored later ones.
COUNTED_JOIN = """
    JOIN unnest(%s::INTEGER[], %s::BIGINT[]) AS counted (kafka_partition, next_offset)
      ON e.source_partition = counted.kafka_partition AND e.source_offset < counted.next_offset
"""
REBUILD_POSITIONS_SQL = f"""
    SELECT route_id, direction_id, date_trunc('minute', tst, 'UTC'),
           operator_id, vehicle_number, payload_hash, live_delay_s
    FROM rt.vehicle_positions AS e
    {COUNTED_JOIN}
    WHERE tst >= %s AND route_id IS NOT NULL AND direction_id IS NOT NULL
      AND {DROPPED_FILTER.format(event_type="'vp'")}
"""
REBUILD_ARRIVALS_SQL = f"""
    SELECT stop_id, date_trunc('minute', tst, 'UTC'),
           operator_id, vehicle_number, payload_hash, live_delay_s
    FROM rt.stop_events AS e
    {COUNTED_JOIN}
    WHERE event_type = 'arr' AND tst >= %s AND stop_id IS NOT NULL
      AND {DROPPED_FILTER.format(event_type="'arr'")}
"""
TRIP_ENDS_SQL = (
    "WITH "
    + JOURNEY_MATCH_CTE.format(
        staged="""(
            SELECT * FROM unnest(%s::TEXT[], %s::SMALLINT[], %s::DATE[], %s::INTEGER[])
                AS requested (route_id, direction_id, oday, start_offset_seconds)
        ) AS requested"""
    )
    + """
    SELECT m.route_id, m.direction_id, m.oday, m.start_offset_seconds,
           gtfs.to_utc(m.oday, last_stop.arrival_offset_seconds)
    FROM matches AS m
    LEFT JOIN LATERAL (
        SELECT max(st.arrival_offset_seconds) AS arrival_offset_seconds
        FROM gtfs.stop_times AS st
        WHERE st.feed_id = m.feed_id AND st.trip_id = m.trip_id
    ) AS last_stop ON true
"""
)
REBUILD_LATE_SQL = """
    SELECT window_start, route_id, direction_id, stop_id, operator_id, vehicle_number,
           payload_hash
    FROM rt.late_events
    WHERE outcome = 'corrected' AND window_start >= %s
"""


@dataclass
class Progress:
    offsets: dict[int, int] = field(default_factory=dict)
    max_event_times: dict[int, datetime] = field(default_factory=dict)
    watermark: datetime | None = None


@dataclass
class BatchOutcome:
    messages: int
    counts: Counter[str]
    emitted: int
    corrected: int
    stale: int
    watermark: datetime | None
    timings_ms: dict[str, int]


class StorageNotReady(RuntimeError):
    """The sink has not stored everything the aggregator had counted before it stopped."""


class EventTimeAggregator:
    def __init__(
        self,
        consumer: Consumer,
        conn: psycopg.Connection,
        settings: AggregatorSettings,
        topic: str,
        storage_offsets: Callable[[list[int]], dict[int, int]],
        hooks: Hooks | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.consumer = consumer
        self.conn = conn
        self.settings = settings
        self.topic = topic
        self.hooks = hooks or Hooks()
        self.rules = WindowRules(
            allowed_lateness=timedelta(seconds=settings.allowed_lateness_s),
            correction_horizon=timedelta(seconds=settings.correction_horizon_s),
            idle_timeout_s=settings.idle_timeout_s,
            stale_after=timedelta(seconds=settings.stale_after_s),
        )
        self.windows = WindowAggregator(self.rules, Watermark(settings.idle_timeout_s, clock))
        self.known_days: set[date] = set(upcoming_days(datetime.now(UTC).date()))
        ensure_partitions(conn, self.known_days)

        self.progress = load_progress(conn, settings.group_id, topic)
        self.windows.watermark.value = self.progress.watermark
        if self.progress.offsets:
            wait_for_storage(self.progress.offsets, storage_offsets, settings.storage_wait_s)
            rebuild(conn, self.windows, self.rules, self.progress.offsets)

    def subscribe(self) -> None:
        self.consumer.subscribe(
            [self.topic],
            on_assign=self.on_assign,
            on_revoke=lambda _c, parts: logger.info("event=revoked partitions=%d", len(parts)),
        )

    def on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Start every partition where the database says counting stopped."""
        for partition in partitions:
            partition.offset = self.progress.offsets.get(partition.partition, OFFSET_BEGINNING)
        self.windows.watermark.assign(
            [p.partition for p in partitions], self.progress.max_event_times
        )
        consumer.assign(partitions)
        logger.info(
            "event=assigned partitions=%s",
            ",".join(f"{p.partition}@{p.offset}" for p in partitions),
        )

    def run(self, stop: threading.Event) -> None:
        self.subscribe()
        while not stop.is_set():
            self.process_batch()

    def process_batch(self) -> BatchOutcome | None:
        started = time.perf_counter()
        messages = [
            message
            for message in self.consumer.consume(
                self.settings.batch_max_messages, self.settings.batch_max_ms / 1000
            )
            if not self._is_error(message)
        ]
        polled = time.perf_counter()

        counts: Counter[str] = Counter()
        seen: set[tuple[str, str]] = set()
        touched: dict[int, int] = {}
        events: list[tuple[Event, int]] = []
        for message in messages:
            touched[message.partition()] = message.offset() + 1
            envelope = parse_envelope(message)
            if isinstance(envelope, Envelope):
                marker = (envelope.topic, envelope.payload)
                if marker in seen:
                    counts["copies"] += 1
                    continue
                seen.add(marker)
                envelope = decode(envelope, message.partition(), message.offset())
            if isinstance(envelope, Rejection):
                counts["invalid"] += 1
                continue
            events.append((envelope, message.partition()))
        unknown = self.windows.unknown_journeys(event for event, _ in events)
        if unknown:
            self.windows.set_trip_ends(trip_ends(self.conn, unknown))
        for event, partition in events:
            counts[self.windows.add(event, partition)] += 1
        before = self.windows.watermark.value
        emission = self.windows.advance()
        late = self.windows.take_late_events()
        moved = self.windows.watermark.value != before
        if not (messages or moved or emission.routes or emission.stops or emission.vehicles):
            return None
        processed = time.perf_counter()

        self._ensure_window_partitions(emission)
        with self.conn.transaction():
            emitted, corrected = self._write(emission, late)
            self._write_progress(touched)
            timings = {
                "poll_ms": _ms(started, polled),
                "process_ms": _ms(polled, processed),
                "db_ms": _ms(processed, time.perf_counter()),
            }
            self._record_metrics(len(messages), counts, emitted, corrected, emission, timings)
            self.hooks.before_db_commit()
        self._remember_progress(touched)
        self.hooks.before_offset_commit()
        if touched:
            self.consumer.commit(
                offsets=[TopicPartition(self.topic, p, o) for p, o in touched.items()],
                asynchronous=False,
            )
        watermark = self.windows.watermark.value
        logger.info(
            "event=batch_committed messages=%d applied=%d late=%d dropped=%d duplicates=%d "
            "emitted=%d corrected=%d stale=%d finished=%d watermark=%s",
            len(messages),
            counts[Outcome.APPLIED] + counts[Outcome.LATE],
            counts[Outcome.LATE],
            counts[Outcome.DROPPED],
            counts[Outcome.DUPLICATE] + counts["copies"],
            emitted,
            corrected,
            emission.stale,
            emission.finished,
            watermark.isoformat() if watermark else None,
        )
        return BatchOutcome(
            len(messages), counts, emitted, corrected, emission.stale, watermark, timings
        )

    def _is_error(self, message: Message) -> bool:
        error = message.error()
        if error is None:
            return False
        if error.fatal():
            raise KafkaException(error)
        if error.code() != KafkaError._PARTITION_EOF:
            logger.error("event=consume_error error=%s", error)
        return True

    def _remember_progress(self, touched: dict[int, int]) -> None:
        """Keep the committed position for a later reassignment of the same partitions."""
        self.progress.offsets.update(touched)
        for partition, progress in self.windows.watermark.partitions.items():
            if progress.max_event_time is not None:
                self.progress.max_event_times[partition] = progress.max_event_time
        self.progress.watermark = self.windows.watermark.value

    def _ensure_window_partitions(self, emission: Emission) -> None:
        days = {row.window_start.astimezone(UTC).date() for row in emission.routes}
        days |= {row.window_start.astimezone(UTC).date() for row in emission.stops}
        new_days = days - self.known_days
        if new_days:
            ensure_partitions(self.conn, new_days)
            self.known_days |= new_days

    def _write(self, emission: Emission, late: list[LateEvent]) -> tuple[int, int]:
        emitted = corrected = 0
        for table, columns, rows, sql in (
            ("route_minute_stats", ROUTE_COLUMNS, emission.routes, UPSERT_ROUTES),
            ("stop_minute_stats", STOP_COLUMNS, emission.stops, UPSERT_STOPS),
        ):
            if not rows:
                continue
            stage(self.conn, table, columns)
            copy_rows(self.conn, f"staged_{table}", columns, (astuple(r, columns) for r in rows))
            for (inserted,) in self.conn.execute(sql).fetchall():
                emitted += inserted
                corrected += not inserted
        if emission.vehicles:
            stage(self.conn, "vehicle_status", VEHICLE_COLUMNS)
            copy_rows(
                self.conn,
                "staged_vehicle_status",
                VEHICLE_COLUMNS,
                (astuple(v, VEHICLE_COLUMNS) for v in emission.vehicles),
            )
            self.conn.execute(UPSERT_VEHICLES)
        if late:
            with self.conn.cursor() as cursor:
                cursor.executemany(
                    f"""
                    INSERT INTO rt.late_events ({", ".join(LATE_COLUMNS)})
                    VALUES ({", ".join(["%s"] * len(LATE_COLUMNS))})
                    ON CONFLICT DO NOTHING
                    """,
                    [late_row(item) for item in late],
                )
        return emitted, corrected

    def _write_progress(self, touched: dict[int, int]) -> None:
        partitions = self.windows.watermark.partitions
        with self.conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO meta.aggregator_progress
                    (group_id, topic, kafka_partition, next_offset, max_event_time)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (group_id, topic, kafka_partition) DO UPDATE
                SET next_offset = excluded.next_offset,
                    max_event_time = excluded.max_event_time,
                    updated_at = clock_timestamp()
                """,
                [
                    (
                        self.settings.group_id,
                        self.topic,
                        partition,
                        offset,
                        partitions[partition].max_event_time if partition in partitions else None,
                    )
                    for partition, offset in sorted(touched.items())
                ],
            )
        watermark = self.windows.watermark.value
        if watermark is not None:
            self.conn.execute(
                """
                INSERT INTO meta.aggregator_watermarks (group_id, watermark) VALUES (%s, %s)
                ON CONFLICT (group_id) DO UPDATE
                SET watermark = excluded.watermark, updated_at = clock_timestamp()
                """,
                (self.settings.group_id, watermark),
            )

    def _record_metrics(
        self,
        messages: int,
        counts: Counter[str],
        emitted: int,
        corrected: int,
        emission: Emission,
        timings: dict[str, int],
    ) -> None:
        partitions = self.windows.watermark.partitions.values()
        max_times = [p.max_event_time for p in partitions if p.max_event_time is not None]
        self.conn.execute(
            """
            INSERT INTO meta.aggregator_metrics (
                group_id, messages, invalid, copies, duplicates, applied, late, dropped,
                no_window, windows_emitted, windows_corrected, vehicles_stale,
                vehicles_finished, open_windows, retained_events, idle_partitions, watermark,
                max_event_time, consumer_lag, poll_ms, process_ms, db_ms
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s)
            """,
            (
                self.settings.group_id,
                messages,
                counts["invalid"],
                counts["copies"],
                counts[Outcome.DUPLICATE],
                counts[Outcome.APPLIED] + counts[Outcome.LATE],
                counts[Outcome.LATE],
                counts[Outcome.DROPPED],
                counts[Outcome.NO_WINDOW],
                emitted,
                corrected,
                emission.stale,
                emission.finished,
                self.windows.open_windows(),
                self.windows.retained_events(),
                len(self.windows.watermark.idle()),
                self.windows.watermark.value,
                max(max_times, default=None),
                consumer_lag(self.consumer),
                timings["poll_ms"],
                timings["process_ms"],
                timings["db_ms"],
            ),
        )


def stage(conn: psycopg.Connection, table: str, columns: tuple[str, ...]) -> None:
    conn.execute(
        f"CREATE TEMPORARY TABLE IF NOT EXISTS staged_{table} ON COMMIT DROP AS "
        f"SELECT {', '.join(columns)} FROM rt.{table} WITH NO DATA"
    )


def astuple(row: RouteMinute | StopMinute | VehicleState, columns: tuple[str, ...]) -> tuple:
    return tuple(getattr(row, column) for column in columns)


def late_row(item: LateEvent) -> tuple:
    event, window = item.event, item.window
    route = window if isinstance(window, RouteWindow) else None
    stop = window if isinstance(window, StopWindow) else None
    return (
        event.operator_id,
        event.vehicle_number,
        event.event_type,
        event.tst,
        event.payload_hash,
        event.received_at,
        window.start,
        route.route_id if route else None,
        route.direction_id if route else None,
        stop.stop_id if stop else None,
        item.watermark,
        item.outcome,
    )


def load_progress(conn: psycopg.Connection, group_id: str, topic: str) -> Progress:
    progress = Progress()
    for partition, offset, max_event_time in conn.execute(
        """
        SELECT kafka_partition, next_offset, max_event_time
        FROM meta.aggregator_progress
        WHERE group_id = %s AND topic = %s
        """,
        (group_id, topic),
    ):
        progress.offsets[partition] = offset
        if max_event_time is not None:
            progress.max_event_times[partition] = max_event_time
    row = conn.execute(
        "SELECT watermark FROM meta.aggregator_watermarks WHERE group_id = %s", (group_id,)
    ).fetchone()
    progress.watermark = row[0] if row else None
    return progress


def wait_for_storage(
    offsets: dict[int, int],
    storage_offsets: Callable[[list[int]], dict[int, int]],
    timeout_s: float,
    pause_s: float = 1.0,
) -> None:
    """Block until the sink has committed at least the aggregator's offsets everywhere."""
    deadline = time.monotonic() + timeout_s
    while True:
        stored = storage_offsets(sorted(offsets))
        behind = {p: o for p, o in offsets.items() if stored.get(p, -1) < o}
        if not behind:
            return
        if time.monotonic() >= deadline:
            raise StorageNotReady(f"sink behind the aggregator on partitions {sorted(behind)}")
        logger.info("event=waiting_for_storage partitions_behind=%d", len(behind))
        time.sleep(pause_s)


def rebuild(
    conn: psycopg.Connection,
    windows: WindowAggregator,
    rules: WindowRules,
    consumed: dict[int, int],
) -> None:
    """Rebuild the windows within the correction horizon from the events stored so far.

    Only events below the aggregator's own offsets are taken, so the windows come back exactly
    as they were at the last commit; later events are consumed again and classified as usual.
    """
    watermark = windows.watermark.value
    if watermark is None:
        return
    since = watermark - rules.correction_horizon - MINUTE
    started = time.perf_counter()
    params = (list(consumed), list(consumed.values()), since)

    contents: dict[WindowKey, dict[bytes, tuple[int, int | None]]] = {}
    for row in conn.execute(REBUILD_POSITIONS_SQL, params):
        route_id, direction_id, start, *event = row
        add_stored_event(
            contents, RouteWindow(route_id, direction_id, start.astimezone(UTC)), event
        )
    for row in conn.execute(REBUILD_ARRIVALS_SQL, params):
        stop_id, start, *event = row
        add_stored_event(contents, StopWindow(stop_id, start.astimezone(UTC)), event)

    late: dict[WindowKey, set[bytes]] = {}
    for row in conn.execute(REBUILD_LATE_SQL, (since,)):
        start, route_id, direction_id, stop_id, operator_id, vehicle_number, payload_hash = row
        start = start.astimezone(UTC)
        window: WindowKey = (
            StopWindow(stop_id, start)
            if stop_id is not None
            else RouteWindow(route_id, direction_id, start)
        )
        late.setdefault(window, set()).add(event_key(operator_id, vehicle_number, payload_hash))

    emitted: set[WindowKey] = {
        RouteWindow(route_id, direction_id, start.astimezone(UTC))
        for route_id, direction_id, start in conn.execute(
            "SELECT route_id, direction_id, window_start FROM rt.route_minute_stats"
            " WHERE window_start >= %s",
            (since,),
        )
    }
    emitted |= {
        StopWindow(stop_id, start.astimezone(UTC))
        for stop_id, start in conn.execute(
            "SELECT stop_id, window_start FROM rt.stop_minute_stats WHERE window_start >= %s",
            (since,),
        )
    }

    restored = 0
    for window, events in contents.items():
        if window.start + MINUTE + rules.correction_horizon > watermark:
            windows.restore_window(window, events, late.get(window, set()), window in emitted)
            restored += 1

    columns = ", ".join(VEHICLE_COLUMNS)
    for row in conn.execute(
        f"SELECT {columns} FROM rt.vehicle_status"
        " WHERE status = 'active' OR (status = 'stale' AND planned_end IS NOT NULL)"
    ):
        windows.restore_vehicle(VehicleState(**dict(zip(VEHICLE_COLUMNS, row, strict=True))))
    logger.info(
        "event=windows_rebuilt windows=%d events=%d vehicles=%d since=%s duration_ms=%d",
        restored,
        windows.retained_events(),
        len(windows.vehicles),
        since.isoformat(),
        _ms(started, time.perf_counter()),
    )


def trip_ends(conn: psycopg.Connection, journeys: set[Journey]) -> dict[Journey, datetime | None]:
    """Planned arrival at the last stop of each journey's trip; None when it has no trip."""
    params = [[getattr(j, name) for j in journeys] for name in Journey._fields]
    found = {Journey(*row[:4]): row[4] for row in conn.execute(TRIP_ENDS_SQL, params)}
    return {journey: found.get(journey) for journey in journeys}


def add_stored_event(
    contents: dict[WindowKey, dict[bytes, tuple[int, int | None]]],
    window: WindowKey,
    event: list,
) -> None:
    operator_id, vehicle_number, payload_hash, live_delay_s = event
    contents.setdefault(window, {})[event_key(operator_id, vehicle_number, payload_hash)] = (
        vehicle_id(operator_id, vehicle_number),
        live_delay_s,
    )


def kafka_storage_offsets(
    admin: AdminClient, group_id: str, topic: str
) -> Callable[[list[int]], dict[int, int]]:
    def committed(partitions: list[int]) -> dict[int, int]:
        request = ConsumerGroupTopicPartitions(
            group_id, [TopicPartition(topic, p) for p in partitions]
        )
        result = admin.list_consumer_group_offsets([request])[group_id].result(timeout=30)
        return {tp.partition: tp.offset for tp in result.topic_partitions}

    return committed


def consumer_config(stream: StreamSettings, settings: AggregatorSettings) -> dict[str, object]:
    """One aggregator owns every partition: a route's vehicles are spread over all of them."""
    config: dict[str, object] = {
        "bootstrap.servers": stream.bootstrap_servers,
        "group.id": settings.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "range",
    }
    if settings.instance_id:
        config["group.instance.id"] = settings.instance_id
    return config


def _ms(start: float, end: float) -> int:
    return round((end - start) * 1000)


def main() -> int:
    configure_logging()
    stop = stop_event_on_signals()
    stream = StreamSettings.from_env()
    settings = AggregatorSettings.from_env()
    consumer = Consumer(consumer_config(stream, settings))
    admin = AdminClient({"bootstrap.servers": stream.bootstrap_servers})
    storage = kafka_storage_offsets(admin, settings.storage_group_id, stream.raw_topic)
    with psycopg.connect(DatabaseSettings.from_env().conninfo, autocommit=True) as conn:
        try:
            EventTimeAggregator(consumer, conn, settings, stream.raw_topic, storage).run(stop)
        finally:
            consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
