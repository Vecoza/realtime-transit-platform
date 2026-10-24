"""Real-time events: daily-partitioned positions and stop events, trip start offsets, metrics.

Revision ID: 002
Revises: 001
"""

from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE gtfs.trips ADD COLUMN start_offset_seconds INTEGER;

        UPDATE gtfs.trips AS t
        SET start_offset_seconds = first_stop.departure_offset_seconds
        FROM (
            SELECT DISTINCT ON (feed_id, trip_id) feed_id, trip_id, departure_offset_seconds
            FROM gtfs.stop_times
            ORDER BY feed_id, trip_id, stop_sequence
        ) AS first_stop
        WHERE first_stop.feed_id = t.feed_id AND first_stop.trip_id = t.trip_id;

        CREATE INDEX ix_trips_journey
            ON gtfs.trips (feed_id, route_id, direction_id, start_offset_seconds);

        CREATE FUNCTION gtfs.feed_for_service_date(service_date DATE) RETURNS BIGINT
        LANGUAGE sql STABLE STRICT PARALLEL SAFE AS $$
            SELECT feed_id
            FROM gtfs.feed_versions
            WHERE service_date BETWEEN valid_from AND valid_to
            ORDER BY loaded_at < service_date + 1 DESC,
                     CASE WHEN loaded_at < service_date + 1 THEN loaded_at END DESC,
                     loaded_at
            LIMIT 1
        $$;

        CREATE TABLE rt.vehicle_positions (
            tst                     TIMESTAMPTZ NOT NULL,
            operator_id             SMALLINT NOT NULL,
            vehicle_number          INTEGER NOT NULL,
            payload_hash            BYTEA NOT NULL,
            received_at             TIMESTAMPTZ NOT NULL,
            stored_at               TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            transport_mode          TEXT NOT NULL,
            running_operator_id     SMALLINT,
            route_id                TEXT,
            direction_id            SMALLINT,
            oday                    DATE,
            start_time              TIME,
            start_offset_seconds    INTEGER,
            feed_id                 BIGINT,
            trip_id                 TEXT,
            match_status            TEXT NOT NULL,
            lat                     DOUBLE PRECISION,
            lon                     DOUBLE PRECISION,
            speed_mps               REAL,
            heading                 SMALLINT,
            hfp_schedule_offset_s   INTEGER,
            stop_id                 TEXT,
            next_stop_id            TEXT,
            location_source         TEXT,
            CONSTRAINT pk_vehicle_positions
                PRIMARY KEY (operator_id, vehicle_number, tst, payload_hash),
            CONSTRAINT ck_vehicle_positions_direction CHECK (direction_id IN (0, 1)),
            CONSTRAINT ck_vehicle_positions_match_status CHECK (match_status IN (
                'matched', 'missing_journey', 'no_feed', 'route_not_in_feed', 'no_trip'
            ))
        ) PARTITION BY RANGE (tst);

        CREATE TABLE rt.vehicle_positions_default PARTITION OF rt.vehicle_positions DEFAULT;

        CREATE TABLE rt.stop_events (
            tst                     TIMESTAMPTZ NOT NULL,
            operator_id             SMALLINT NOT NULL,
            vehicle_number          INTEGER NOT NULL,
            event_type              TEXT NOT NULL,
            payload_hash            BYTEA NOT NULL,
            received_at             TIMESTAMPTZ NOT NULL,
            stored_at               TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            transport_mode          TEXT NOT NULL,
            running_operator_id     SMALLINT,
            route_id                TEXT,
            direction_id            SMALLINT,
            oday                    DATE,
            start_time              TIME,
            start_offset_seconds    INTEGER,
            feed_id                 BIGINT,
            trip_id                 TEXT,
            stop_id                 TEXT,
            stop_sequence           INTEGER,
            match_status            TEXT NOT NULL,
            planned_time            TIMESTAMPTZ,
            delay_seconds           INTEGER,
            hfp_planned_time        TIMESTAMPTZ,
            hfp_schedule_offset_s   INTEGER,
            lat                     DOUBLE PRECISION,
            lon                     DOUBLE PRECISION,
            CONSTRAINT pk_stop_events
                PRIMARY KEY (operator_id, vehicle_number, event_type, tst, payload_hash),
            CONSTRAINT ck_stop_events_event_type CHECK (event_type IN ('arr', 'dep')),
            CONSTRAINT ck_stop_events_direction CHECK (direction_id IN (0, 1)),
            CONSTRAINT ck_stop_events_match_status CHECK (match_status IN (
                'matched', 'missing_journey', 'no_feed', 'route_not_in_feed', 'no_trip',
                'missing_stop', 'stop_not_on_trip'
            )),
            CONSTRAINT ck_stop_events_planned_with_delay
                CHECK ((planned_time IS NULL) = (delay_seconds IS NULL))
        ) PARTITION BY RANGE (tst);

        CREATE TABLE rt.stop_events_default PARTITION OF rt.stop_events DEFAULT;

        CREATE FUNCTION rt.ensure_day_partitions(first_day DATE, last_day DATE)
        RETURNS INTEGER
        LANGUAGE plpgsql AS $$
        DECLARE
            parent TEXT;
            day DATE;
            partition_name TEXT;
            created INTEGER := 0;
        BEGIN
            FOREACH parent IN ARRAY ARRAY['vehicle_positions', 'stop_events'] LOOP
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

        CREATE TABLE meta.consumer_metrics (
            batch_id              BIGINT GENERATED ALWAYS AS IDENTITY,
            group_id              TEXT NOT NULL,
            committed_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            messages              INTEGER NOT NULL,
            invalid               INTEGER NOT NULL,
            inserted              INTEGER NOT NULL,
            duplicates            INTEGER NOT NULL,
            unmatched             INTEGER NOT NULL,
            consumer_lag          BIGINT,
            poll_ms               INTEGER NOT NULL,
            decode_ms             INTEGER NOT NULL,
            db_ms                 INTEGER NOT NULL,
            latency_p50_ms        INTEGER,
            latency_p95_ms        INTEGER,
            CONSTRAINT pk_consumer_metrics PRIMARY KEY (batch_id),
            CONSTRAINT ck_consumer_metrics_counts
                CHECK (inserted + duplicates + invalid = messages)
        );

        CREATE INDEX ix_consumer_metrics_committed_at
            ON meta.consumer_metrics (group_id, committed_at);
    """)


def downgrade() -> None:
    op.execute("""
        DROP TABLE meta.consumer_metrics;
        DROP FUNCTION rt.ensure_day_partitions(DATE, DATE);
        DROP TABLE rt.stop_events;
        DROP TABLE rt.vehicle_positions;
        DROP FUNCTION gtfs.feed_for_service_date(DATE);
        DROP INDEX gtfs.ix_trips_journey;
        ALTER TABLE gtfs.trips DROP COLUMN start_offset_seconds;
    """)
