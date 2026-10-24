"""Streaming aggregates against a batch recomputation over the same stored events."""

import math
import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta

import psycopg
import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

from processor.aggregate import EventTimeAggregator, consumer_config, kafka_storage_offsets
from processor.decode import Event, decode
from processor.sink import Hooks, StorageSink, build_consumer
from producer.hfp import Envelope
from producer.kafka_sink import PRODUCER_CONFIG, KafkaSink, create_topic, ensure_topic
from tests.conftest import REPO_ROOT
from tests.hfp_stream import stream
from transit_platform.config import (
    AggregatorSettings,
    DatabaseSettings,
    ProcessorSettings,
    StreamSettings,
)

pytestmark = [pytest.mark.db, pytest.mark.stream]

LATENESS_S = 10
HORIZON_S = 120
IDLE_S = 5
CHUNK = 125

ROUTE_BATCH_SQL = """
    SELECT route_id, direction_id, date_trunc('minute', tst, 'UTC') AS window_start,
           count(DISTINCT (operator_id, vehicle_number)), count(*),
           count(hfp_schedule_offset_s),
           avg(-hfp_schedule_offset_s)::DOUBLE PRECISION,
           percentile_cont(0.9) WITHIN GROUP (ORDER BY -hfp_schedule_offset_s)
    FROM rt.vehicle_positions AS e
    WHERE route_id IS NOT NULL AND direction_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM rt.late_events AS l
          WHERE l.outcome = 'dropped' AND l.event_type = 'vp'
            AND (l.operator_id, l.vehicle_number, l.tst, l.payload_hash, l.received_at)
              = (e.operator_id, e.vehicle_number, e.tst, e.payload_hash, e.received_at)
      )
    GROUP BY 1, 2, 3
"""
STOP_BATCH_SQL = """
    SELECT stop_id, date_trunc('minute', tst, 'UTC') AS window_start,
           count(*), count(hfp_schedule_offset_s),
           avg(-hfp_schedule_offset_s)::DOUBLE PRECISION
    FROM rt.stop_events AS e
    WHERE event_type = 'arr' AND stop_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM rt.late_events AS l
          WHERE l.outcome = 'dropped' AND l.event_type = 'arr'
            AND (l.operator_id, l.vehicle_number, l.tst, l.payload_hash, l.received_at)
              = (e.operator_id, e.vehicle_number, e.tst, e.payload_hash, e.received_at)
      )
    GROUP BY 1, 2
"""
ROUTE_STREAM_SQL = """
    SELECT route_id, direction_id, window_start, vehicles, positions, delay_samples,
           avg_delay_s, p90_delay_s
    FROM rt.route_minute_stats
"""
STOP_STREAM_SQL = """
    SELECT stop_id, window_start, arrivals, delay_samples, avg_delay_s
    FROM rt.stop_minute_stats
"""


class Crash(Exception):
    """Stands in for the process dying at a chosen point."""


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


@dataclass
class AggregatorRun:
    consumer: Consumer
    conn: psycopg.Connection
    service: EventTimeAggregator
    clock: Clock

    def close(self) -> None:
        self.consumer.close()
        self.conn.close()


@dataclass
class Harness:
    stream: StreamSettings
    sink_settings: ProcessorSettings
    settings: AggregatorSettings
    database: DatabaseSettings
    envelopes: list[Envelope]
    published: int = 0
    stored: int = 0
    sink: StorageSink | None = None
    producer: KafkaSink | None = None
    closers: list = field(default_factory=list)

    def publish(self, count: int = CHUNK) -> None:
        if self.producer is None:
            self.producer = KafkaSink(self.stream)
        for envelope in self.envelopes[self.published : self.published + count]:
            self.producer.send(envelope)
        assert self.producer.flush() == 0
        self.published = min(self.published + count, len(self.envelopes))

    def store(self) -> None:
        """Let the storage sink catch up with everything published."""
        if self.sink is None:
            consumer = build_consumer(self.stream, self.sink_settings)
            dlq = Producer({"bootstrap.servers": self.stream.bootstrap_servers, **PRODUCER_CONFIG})
            conn = psycopg.connect(self.database.conninfo, autocommit=True)
            self.sink = StorageSink(consumer, dlq, conn, self.sink_settings)
            self.closers += [consumer.close, conn.close]
        for _ in range(200):
            if self.stored >= self.published:
                return
            outcome = self.sink.process_batch()
            self.stored += outcome.messages if outcome else 0
        raise AssertionError("the sink did not catch up")

    def start(self, hooks: Hooks | None = None) -> AggregatorRun:
        consumer = Consumer(consumer_config(self.stream, self.settings))
        conn = psycopg.connect(self.database.conninfo, autocommit=True)
        admin = AdminClient({"bootstrap.servers": self.stream.bootstrap_servers})
        clock = Clock()
        service = EventTimeAggregator(
            consumer,
            conn,
            self.settings,
            self.stream.raw_topic,
            kafka_storage_offsets(admin, self.sink_settings.group_id, self.stream.raw_topic),
            hooks,
            clock,
        )
        service.subscribe()
        return AggregatorRun(consumer, conn, service, clock)

    def counted(self) -> int:
        with psycopg.connect(self.database.conninfo) as conn:
            return conn.execute(
                "SELECT coalesce(sum(next_offset), 0) FROM meta.aggregator_progress"
                " WHERE group_id = %s",
                (self.settings.group_id,),
            ).fetchone()[0]

    def aggregate(self, run: AggregatorRun) -> None:
        for _ in range(200):
            if self.counted() >= self.published:
                return
            run.service.process_batch()
        raise AssertionError("the aggregator did not catch up")

    def finish(self, run: AggregatorRun) -> None:
        """Let every partition go idle so the watermark reaches the last event."""
        run.clock.now += IDLE_S + 1
        for _ in range(3):
            run.service.process_batch()


@pytest.fixture
def harness(
    stream_settings: StreamSettings, migrated: DatabaseSettings, conn: psycopg.Connection
) -> Iterator[Harness]:
    suffix = uuid.uuid4().hex[:8]
    stream_config = replace(stream_settings, raw_topic=f"test.hfp.raw.{suffix}", raw_partitions=3)
    sink_settings = ProcessorSettings(
        group_id=f"test-sink-{suffix}",
        dlq_topic=f"test.hfp.dlq.{suffix}",
        batch_max_messages=CHUNK,
        batch_max_ms=50,
    )
    settings = AggregatorSettings(
        group_id=f"test-aggregator-{suffix}",
        storage_group_id=sink_settings.group_id,
        batch_max_messages=100,
        batch_max_ms=50,
        allowed_lateness_s=LATENESS_S,
        correction_horizon_s=HORIZON_S,
        idle_timeout_s=IDLE_S,
        storage_wait_s=30,
    )
    ensure_topic(stream_config)
    create_topic(stream_config.bootstrap_servers, sink_settings.dlq_topic, 1, 3_600_000)
    test_harness = Harness(stream_config, sink_settings, settings, migrated, stream())
    yield test_harness
    for close in test_harness.closers:
        close()
    admin = AdminClient({"bootstrap.servers": stream_config.bootstrap_servers})
    for future in admin.delete_topics([stream_config.raw_topic, sink_settings.dlq_topic]).values():
        future.result(timeout=30)


def final_watermark(envelopes: list[Envelope]) -> datetime:
    events = [event for event in map(decode, envelopes) if isinstance(event, Event)]
    return max(min(event.tst, event.received_at) for event in events)


def assert_matches_batch(database: DatabaseSettings, group_id: str) -> dict[str, int]:
    """Every due window equals the recomputation; no window that is not due was emitted."""
    with psycopg.connect(database.conninfo) as conn:
        (watermark,) = conn.execute(
            "SELECT watermark FROM meta.aggregator_watermarks WHERE group_id = %s", (group_id,)
        ).fetchone()
        due = watermark - timedelta(seconds=60 + LATENESS_S)
        for stream_sql, batch_sql, key_size in (
            (ROUTE_STREAM_SQL, ROUTE_BATCH_SQL, 3),
            (STOP_STREAM_SQL, STOP_BATCH_SQL, 2),
        ):
            streamed = {row[:key_size]: row[key_size:] for row in conn.execute(stream_sql)}
            batch = {
                row[:key_size]: row[key_size:]
                for row in conn.execute(batch_sql)
                if row[key_size - 1] <= due
            }
            assert streamed.keys() == batch.keys()
            for key, values in batch.items():
                for got, expected in zip(streamed[key], values, strict=True):
                    assert got == expected or math.isclose(got, expected, abs_tol=1e-9), key
        mismatched_late = conn.execute("""
            SELECT count(*)
            FROM (
                SELECT route_id, direction_id, window_start, late_events
                FROM rt.route_minute_stats
                UNION ALL
                SELECT stop_id, NULL, window_start, late_events FROM rt.stop_minute_stats
            ) AS s
            WHERE late_events <> (
                SELECT count(*) FROM rt.late_events AS l
                WHERE l.outcome = 'corrected' AND l.window_start = s.window_start
                  AND l.route_id IS NOT DISTINCT FROM (
                      CASE WHEN s.direction_id IS NOT NULL THEN s.route_id END)
                  AND l.direction_id IS NOT DISTINCT FROM s.direction_id
                  AND l.stop_id IS NOT DISTINCT FROM (
                      CASE WHEN s.direction_id IS NULL THEN s.route_id END)
            )
        """).fetchone()[0]
        assert mismatched_late == 0
        outcomes = dict(
            conn.execute("SELECT outcome, count(*) FROM rt.late_events GROUP BY 1").fetchall()
        )
        stale = conn.execute(
            "SELECT coalesce(sum(vehicles_stale), 0) FROM meta.aggregator_metrics"
            " WHERE group_id = %s",
            (group_id,),
        ).fetchone()[0]
        windows = conn.execute("SELECT count(*) FROM rt.route_minute_stats").fetchone()[0]
    return {**outcomes, "stale": stale, "route_windows": windows}


def run_live(harness: Harness, run: AggregatorRun, chunks: int | None = None) -> None:
    while harness.published < len(harness.envelopes) and chunks != 0:
        harness.publish()
        harness.store()
        harness.aggregate(run)
        chunks = None if chunks is None else chunks - 1


def test_streaming_aggregates_equal_a_batch_recomputation(harness: Harness) -> None:
    run = harness.start()
    run_live(harness, run)
    harness.finish(run)
    run.close()

    summary = assert_matches_batch(harness.database, harness.settings.group_id)

    assert summary["corrected"] > 0
    assert summary["dropped"] > 0
    assert summary["stale"] >= 1
    assert summary["route_windows"] == 6 * 5


@pytest.mark.parametrize("point", ["before_db_commit", "before_offset_commit", "clean_stop"])
def test_restart_mid_stream_rebuilds_windows_without_gaps_or_double_counts(
    harness: Harness, point: str
) -> None:
    run = harness.start()
    run_live(harness, run, chunks=5)

    def crash() -> None:
        raise Crash

    if point != "clean_stop":
        run.service.hooks = Hooks(**{point: crash})
        harness.publish()
        harness.store()
        with pytest.raises(Crash):
            for _ in range(50):
                run.service.process_batch()
    run.close()

    restarted = harness.start()
    assert restarted.service.windows.windows, "open windows are rebuilt from stored events"
    run_live(harness, restarted)
    harness.aggregate(restarted)
    harness.finish(restarted)
    restarted.close()

    summary = assert_matches_batch(harness.database, harness.settings.group_id)
    assert summary["corrected"] > 0
    assert summary["dropped"] > 0


def test_kill_minus_nine_in_the_middle_of_the_replay(harness: Harness) -> None:
    env = {
        **os.environ,
        "KAFKA_BOOTSTRAP_SERVERS": harness.stream.bootstrap_servers,
        "HFP_RAW_TOPIC": harness.stream.raw_topic,
        "POSTGRES_DB": harness.database.name,
        "SINK_GROUP_ID": harness.sink_settings.group_id,
        "AGGREGATOR_GROUP_ID": harness.settings.group_id,
        "AGGREGATOR_INSTANCE_ID": f"{harness.settings.group_id}-1",
        "AGGREGATOR_BATCH_MAX_MESSAGES": "100",
        "AGGREGATOR_BATCH_MAX_MS": "50",
        "AGGREGATOR_ALLOWED_LATENESS_S": str(LATENESS_S),
        "AGGREGATOR_CORRECTION_HORIZON_S": str(HORIZON_S),
        "AGGREGATOR_IDLE_TIMEOUT_S": "3",
        "AGGREGATOR_STORAGE_WAIT_S": "30",
    }

    def start() -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, "-m", "processor.aggregate"],
            cwd=REPO_ROOT,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def wait_until(condition, timeout_s: float = 60) -> None:
        deadline = time.monotonic() + timeout_s
        while not condition():
            assert time.monotonic() < deadline, "timed out"
            time.sleep(0.1)

    process = start()
    try:
        for _ in range(5):
            harness.publish()
            harness.store()
            wait_until(lambda: harness.counted() >= harness.published)
        harness.publish()
        harness.store()
        wait_until(lambda: harness.counted() > harness.published - CHUNK)
        process.send_signal(signal.SIGKILL)
        process.wait()
        killed_at = harness.counted()

        harness.publish()
        harness.store()
        process = start()
        wait_until(lambda: harness.counted() >= harness.published)
        while harness.published < len(harness.envelopes):
            harness.publish()
            harness.store()
            wait_until(lambda: harness.counted() >= harness.published)
        expected = final_watermark(harness.envelopes)

        def watermark_reached() -> bool:
            with psycopg.connect(harness.database.conninfo) as conn:
                row = conn.execute(
                    "SELECT watermark FROM meta.aggregator_watermarks WHERE group_id = %s",
                    (harness.settings.group_id,),
                ).fetchone()
            return row is not None and row[0] >= expected

        wait_until(watermark_reached)
    finally:
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=30)

    assert 0 < killed_at < len(harness.envelopes)
    summary = assert_matches_batch(harness.database, harness.settings.group_id)
    assert summary["corrected"] > 0
