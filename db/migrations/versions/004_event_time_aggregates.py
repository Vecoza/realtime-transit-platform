"""Event-time aggregates: minute windows, late events, vehicle status, source offsets.

Revision ID: 004
Revises: 003
"""

from alembic import op

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None

ENSURE_DAY_PARTITIONS = """
    CREATE OR REPLACE FUNCTION rt.ensure_day_partitions(first_day DATE, last_day DATE)
    RETURNS INTEGER
    LANGUAGE plpgsql AS $$
    DECLARE
        parent TEXT;
        day DATE;
        partition_name TEXT;
        created INTEGER := 0;
    BEGIN
        -- Services start concurrently; one creates a day at a time.
        PERFORM pg_advisory_xact_lock(hashtext('rt.ensure_day_partitions'));
        FOREACH parent IN ARRAY ARRAY[{parents}] LOOP
            FOR day IN SELECT generate_series(first_day, last_day, INTERVAL '1 day')::DATE
            LOOP
                partition_name := format('%s_%s', parent, to_char(day, 'YYYYMMDD'));
                IF to_regclass(format('rt.%I', partition_name)) IS NULL THEN
                    EXECUTE format(
                        'CREATE TABLE rt.%I PARTITION OF rt.%I FOR VALUES FROM (%L) TO (%L)',
                        partition_name, parent,
                        day::TIMESTAMP AT TIME ZONE 'UTC',
                        (day + 1)::TIMESTAMP AT TIME ZONE 'UTC'
                    );
                    created := created + 1;
                END IF;
            END LOOP;
        END LOOP;
        RETURN created;
    END;
    $$;
"""


def upgrade() -> None:
    op.execute("""
        ALTER TABLE rt.vehicle_positions
            ADD COLUMN source_partition SMALLINT,
            ADD COLUMN source_offset BIGINT;
        ALTER TABLE rt.stop_events
            ADD COLUMN source_partition SMALLINT,
            ADD COLUMN source_offset BIGINT;

        CREATE TABLE rt.route_minute_stats (
            route_id          TEXT NOT NULL,
            direction_id      SMALLINT NOT NULL,
            window_start      TIMESTAMPTZ NOT NULL,
            vehicles          INTEGER NOT NULL,
            positions         INTEGER NOT NULL,
            delay_samples     INTEGER NOT NULL,
            avg_delay_s       DOUBLE PRECISION,
            p90_delay_s       DOUBLE PRECISION,
            late_events       INTEGER NOT NULL,
            revision          INTEGER NOT NULL DEFAULT 1,
            first_emitted_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            emitted_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT pk_route_minute_stats PRIMARY KEY (route_id, direction_id, window_start),
            CONSTRAINT ck_route_minute_stats_minute
                CHECK (window_start = date_trunc('minute', window_start)),
            CONSTRAINT ck_route_minute_stats_counts
                CHECK (vehicles BETWEEN 1 AND positions AND delay_samples <= positions
                       AND late_events <= positions),
            CONSTRAINT ck_route_minute_stats_delay
                CHECK ((delay_samples = 0) = (avg_delay_s IS NULL AND p90_delay_s IS NULL))
        ) PARTITION BY RANGE (window_start);

        CREATE TABLE rt.route_minute_stats_default PARTITION OF rt.route_minute_stats DEFAULT;

        CREATE TABLE rt.stop_minute_stats (
            stop_id           TEXT NOT NULL,
            window_start      TIMESTAMPTZ NOT NULL,
            arrivals          INTEGER NOT NULL,
            delay_samples     INTEGER NOT NULL,
            avg_delay_s       DOUBLE PRECISION,
            late_events       INTEGER NOT NULL,
            revision          INTEGER NOT NULL DEFAULT 1,
            first_emitted_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            emitted_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT pk_stop_minute_stats PRIMARY KEY (stop_id, window_start),
            CONSTRAINT ck_stop_minute_stats_minute
                CHECK (window_start = date_trunc('minute', window_start)),
            CONSTRAINT ck_stop_minute_stats_counts
                CHECK (arrivals >= 1 AND delay_samples <= arrivals AND late_events <= arrivals),
            CONSTRAINT ck_stop_minute_stats_delay
                CHECK ((delay_samples = 0) = (avg_delay_s IS NULL))
        ) PARTITION BY RANGE (window_start);

        CREATE TABLE rt.stop_minute_stats_default PARTITION OF rt.stop_minute_stats DEFAULT;

        CREATE TABLE rt.late_events (
            operator_id     SMALLINT NOT NULL,
            vehicle_number  INTEGER NOT NULL,
            event_type      TEXT NOT NULL,
            tst             TIMESTAMPTZ NOT NULL,
            payload_hash    BYTEA NOT NULL,
            received_at     TIMESTAMPTZ NOT NULL,
            window_start    TIMESTAMPTZ NOT NULL,
            route_id        TEXT,
            direction_id    SMALLINT,
            stop_id         TEXT,
            watermark       TIMESTAMPTZ NOT NULL,
            outcome         TEXT NOT NULL,
            recorded_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT pk_late_events
                PRIMARY KEY (operator_id, vehicle_number, event_type, tst, payload_hash),
            CONSTRAINT ck_late_events_outcome CHECK (outcome IN ('corrected', 'dropped')),
            CONSTRAINT ck_late_events_window CHECK (
                (event_type = 'vp' AND route_id IS NOT NULL AND direction_id IS NOT NULL)
                OR (event_type = 'arr' AND stop_id IS NOT NULL)
            ),
            CONSTRAINT ck_late_events_behind_watermark CHECK (watermark > window_start)
        );

        CREATE INDEX ix_late_events_window ON rt.late_events (window_start);

        CREATE TABLE rt.vehicle_status (
            operator_id       SMALLINT NOT NULL,
            vehicle_number    INTEGER NOT NULL,
            status            TEXT NOT NULL,
            last_tst          TIMESTAMPTZ NOT NULL,
            last_received_at  TIMESTAMPTZ NOT NULL,
            transport_mode    TEXT NOT NULL,
            route_id          TEXT,
            direction_id      SMALLINT,
            oday              DATE,
            start_time        TIME,
            lat               DOUBLE PRECISION,
            lon               DOUBLE PRECISION,
            stale_since       TIMESTAMPTZ,
            updated_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT pk_vehicle_status PRIMARY KEY (operator_id, vehicle_number),
            CONSTRAINT ck_vehicle_status_status CHECK (status IN ('active', 'stale')),
            CONSTRAINT ck_vehicle_status_stale_since
                CHECK ((status = 'stale') = (stale_since IS NOT NULL))
        );

        CREATE INDEX ix_vehicle_status_status ON rt.vehicle_status (status, last_tst);

        CREATE TABLE meta.aggregator_progress (
            group_id        TEXT NOT NULL,
            topic           TEXT NOT NULL,
            kafka_partition INTEGER NOT NULL,
            next_offset     BIGINT NOT NULL,
            max_event_time  TIMESTAMPTZ,
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT pk_aggregator_progress PRIMARY KEY (group_id, topic, kafka_partition),
            CONSTRAINT ck_aggregator_progress_offset CHECK (next_offset >= 0)
        );

        CREATE TABLE meta.aggregator_watermarks (
            group_id    TEXT NOT NULL,
            watermark   TIMESTAMPTZ NOT NULL,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT pk_aggregator_watermarks PRIMARY KEY (group_id)
        );

        CREATE TABLE meta.aggregator_metrics (
            batch_id          BIGINT GENERATED ALWAYS AS IDENTITY,
            group_id          TEXT NOT NULL,
            committed_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            messages          INTEGER NOT NULL,
            invalid           INTEGER NOT NULL,
            copies            INTEGER NOT NULL,
            duplicates        INTEGER NOT NULL,
            applied           INTEGER NOT NULL,
            late              INTEGER NOT NULL,
            dropped           INTEGER NOT NULL,
            no_window         INTEGER NOT NULL,
            windows_emitted   INTEGER NOT NULL,
            windows_corrected INTEGER NOT NULL,
            vehicles_stale    INTEGER NOT NULL,
            open_windows      INTEGER NOT NULL,
            retained_events   INTEGER NOT NULL,
            idle_partitions   INTEGER NOT NULL,
            watermark         TIMESTAMPTZ,
            max_event_time    TIMESTAMPTZ,
            poll_ms           INTEGER NOT NULL,
            process_ms        INTEGER NOT NULL,
            db_ms             INTEGER NOT NULL,
            CONSTRAINT pk_aggregator_metrics PRIMARY KEY (batch_id),
            CONSTRAINT ck_aggregator_metrics_counts CHECK (
                invalid + copies + duplicates + applied + dropped + no_window = messages
                AND late <= applied
            )
        );
    """)
    op.execute(
        ENSURE_DAY_PARTITIONS.format(
            parents="'vehicle_positions', 'stop_events', 'route_minute_stats', 'stop_minute_stats'"
        )
    )


def downgrade() -> None:
    op.execute(ENSURE_DAY_PARTITIONS.format(parents="'vehicle_positions', 'stop_events'"))
    op.execute("""
        DROP TABLE meta.aggregator_metrics;
        DROP TABLE meta.aggregator_watermarks;
        DROP TABLE meta.aggregator_progress;
        DROP TABLE rt.vehicle_status;
        DROP TABLE rt.late_events;
        DROP TABLE rt.stop_minute_stats;
        DROP TABLE rt.route_minute_stats;
        ALTER TABLE rt.stop_events DROP COLUMN source_partition, DROP COLUMN source_offset;
        ALTER TABLE rt.vehicle_positions DROP COLUMN source_partition, DROP COLUMN source_offset;
    """)
