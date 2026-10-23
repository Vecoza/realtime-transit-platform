"""GTFS timetable: feed versions, static tables, expanded service dates and time helpers.

Revision ID: 001
Revises:
"""

from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE gtfs.feed_versions (
            feed_id        BIGINT GENERATED ALWAYS AS IDENTITY,
            source_url     TEXT NOT NULL,
            sha256         TEXT NOT NULL,
            size_bytes     BIGINT NOT NULL,
            feed_version   TEXT,
            downloaded_at  TIMESTAMPTZ NOT NULL,
            loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            valid_from     DATE NOT NULL,
            valid_to       DATE NOT NULL,
            is_current     BOOLEAN NOT NULL DEFAULT false,
            CONSTRAINT pk_feed_versions PRIMARY KEY (feed_id),
            CONSTRAINT uq_feed_versions_sha256 UNIQUE (sha256),
            CONSTRAINT ck_feed_versions_sha256_hex CHECK (sha256 ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_feed_versions_size_positive CHECK (size_bytes > 0),
            CONSTRAINT ck_feed_versions_validity CHECK (valid_to >= valid_from)
        );

        CREATE UNIQUE INDEX uq_feed_versions_single_current
            ON gtfs.feed_versions (is_current) WHERE is_current;

        CREATE TABLE gtfs.agencies (
            feed_id          BIGINT NOT NULL,
            agency_id        TEXT NOT NULL,
            agency_name      TEXT NOT NULL,
            agency_url       TEXT NOT NULL,
            agency_timezone  TEXT NOT NULL,
            CONSTRAINT pk_agencies PRIMARY KEY (feed_id, agency_id),
            CONSTRAINT fk_agencies_feed FOREIGN KEY (feed_id)
                REFERENCES gtfs.feed_versions (feed_id) ON DELETE CASCADE
        );

        CREATE TABLE gtfs.routes (
            feed_id           BIGINT NOT NULL,
            route_id          TEXT NOT NULL,
            agency_id         TEXT NOT NULL,
            route_short_name  TEXT,
            route_long_name   TEXT,
            route_type        INTEGER NOT NULL,
            CONSTRAINT pk_routes PRIMARY KEY (feed_id, route_id),
            CONSTRAINT fk_routes_agency FOREIGN KEY (feed_id, agency_id)
                REFERENCES gtfs.agencies (feed_id, agency_id) ON DELETE CASCADE,
            CONSTRAINT ck_routes_name_present
                CHECK (route_short_name IS NOT NULL OR route_long_name IS NOT NULL)
        );

        CREATE TABLE gtfs.stops (
            feed_id         BIGINT NOT NULL,
            stop_id         TEXT NOT NULL,
            stop_code       TEXT,
            stop_name       TEXT,
            stop_lat        DOUBLE PRECISION,
            stop_lon        DOUBLE PRECISION,
            zone_id         TEXT,
            location_type   SMALLINT NOT NULL DEFAULT 0,
            parent_station  TEXT,
            platform_code   TEXT,
            CONSTRAINT pk_stops PRIMARY KEY (feed_id, stop_id),
            CONSTRAINT fk_stops_feed FOREIGN KEY (feed_id)
                REFERENCES gtfs.feed_versions (feed_id) ON DELETE CASCADE,
            CONSTRAINT fk_stops_parent_station FOREIGN KEY (feed_id, parent_station)
                REFERENCES gtfs.stops (feed_id, stop_id) ON DELETE CASCADE,
            CONSTRAINT ck_stops_location_type CHECK (location_type BETWEEN 0 AND 4),
            CONSTRAINT ck_stops_lat_range CHECK (stop_lat BETWEEN -90 AND 90),
            CONSTRAINT ck_stops_lon_range CHECK (stop_lon BETWEEN -180 AND 180)
        );

        CREATE TABLE gtfs.calendar (
            feed_id     BIGINT NOT NULL,
            service_id  TEXT NOT NULL,
            monday      BOOLEAN NOT NULL,
            tuesday     BOOLEAN NOT NULL,
            wednesday   BOOLEAN NOT NULL,
            thursday    BOOLEAN NOT NULL,
            friday      BOOLEAN NOT NULL,
            saturday    BOOLEAN NOT NULL,
            sunday      BOOLEAN NOT NULL,
            start_date  DATE NOT NULL,
            end_date    DATE NOT NULL,
            CONSTRAINT pk_calendar PRIMARY KEY (feed_id, service_id),
            CONSTRAINT fk_calendar_feed FOREIGN KEY (feed_id)
                REFERENCES gtfs.feed_versions (feed_id) ON DELETE CASCADE,
            CONSTRAINT ck_calendar_date_range CHECK (end_date >= start_date)
        );

        CREATE TABLE gtfs.calendar_dates (
            feed_id         BIGINT NOT NULL,
            service_id      TEXT NOT NULL,
            service_date    DATE NOT NULL,
            exception_type  SMALLINT NOT NULL,
            CONSTRAINT pk_calendar_dates PRIMARY KEY (feed_id, service_id, service_date),
            CONSTRAINT fk_calendar_dates_feed FOREIGN KEY (feed_id)
                REFERENCES gtfs.feed_versions (feed_id) ON DELETE CASCADE,
            CONSTRAINT ck_calendar_dates_exception_type CHECK (exception_type IN (1, 2))
        );

        CREATE TABLE gtfs.service_dates (
            feed_id       BIGINT NOT NULL,
            service_date  DATE NOT NULL,
            service_id    TEXT NOT NULL,
            CONSTRAINT pk_service_dates PRIMARY KEY (feed_id, service_date, service_id),
            CONSTRAINT fk_service_dates_feed FOREIGN KEY (feed_id)
                REFERENCES gtfs.feed_versions (feed_id) ON DELETE CASCADE
        );

        CREATE TABLE gtfs.shapes (
            feed_id              BIGINT NOT NULL,
            shape_id             TEXT NOT NULL,
            shape_pt_sequence    INTEGER NOT NULL,
            shape_pt_lat         DOUBLE PRECISION NOT NULL,
            shape_pt_lon         DOUBLE PRECISION NOT NULL,
            shape_dist_traveled  DOUBLE PRECISION,
            CONSTRAINT pk_shapes PRIMARY KEY (feed_id, shape_id, shape_pt_sequence),
            CONSTRAINT fk_shapes_feed FOREIGN KEY (feed_id)
                REFERENCES gtfs.feed_versions (feed_id) ON DELETE CASCADE
        );

        CREATE TABLE gtfs.trips (
            feed_id        BIGINT NOT NULL,
            trip_id        TEXT NOT NULL,
            route_id       TEXT NOT NULL,
            service_id     TEXT NOT NULL,
            trip_headsign  TEXT,
            direction_id   SMALLINT,
            shape_id       TEXT,
            CONSTRAINT pk_trips PRIMARY KEY (feed_id, trip_id),
            CONSTRAINT fk_trips_route FOREIGN KEY (feed_id, route_id)
                REFERENCES gtfs.routes (feed_id, route_id) ON DELETE CASCADE,
            CONSTRAINT ck_trips_direction_id CHECK (direction_id IN (0, 1))
        );

        CREATE TABLE gtfs.stop_times (
            feed_id                    BIGINT NOT NULL,
            trip_id                    TEXT NOT NULL,
            stop_sequence              INTEGER NOT NULL,
            stop_id                    TEXT NOT NULL,
            arrival_offset_seconds     INTEGER NOT NULL,
            departure_offset_seconds   INTEGER NOT NULL,
            stop_headsign              TEXT,
            pickup_type                SMALLINT,
            drop_off_type              SMALLINT,
            timepoint                  SMALLINT,
            CONSTRAINT pk_stop_times PRIMARY KEY (feed_id, trip_id, stop_sequence),
            CONSTRAINT fk_stop_times_trip FOREIGN KEY (feed_id, trip_id)
                REFERENCES gtfs.trips (feed_id, trip_id) ON DELETE CASCADE,
            CONSTRAINT fk_stop_times_stop FOREIGN KEY (feed_id, stop_id)
                REFERENCES gtfs.stops (feed_id, stop_id) ON DELETE CASCADE,
            CONSTRAINT ck_stop_times_arrival_non_negative CHECK (arrival_offset_seconds >= 0),
            CONSTRAINT ck_stop_times_departure_after_arrival
                CHECK (departure_offset_seconds >= arrival_offset_seconds)
        ) PARTITION BY LIST (feed_id);

        CREATE FUNCTION gtfs.time_to_seconds(gtfs_time TEXT) RETURNS INTEGER
        LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
            SELECT split_part(gtfs_time, ':', 1)::INTEGER * 3600
                 + split_part(gtfs_time, ':', 2)::INTEGER * 60
                 + split_part(gtfs_time, ':', 3)::INTEGER
        $$;

        CREATE FUNCTION gtfs.service_day_start(
            service_date DATE, time_zone TEXT DEFAULT 'Europe/Helsinki'
        ) RETURNS TIMESTAMPTZ
        LANGUAGE sql STABLE STRICT PARALLEL SAFE AS $$
            SELECT ((service_date + TIME '12:00') AT TIME ZONE time_zone) - INTERVAL '12 hours'
        $$;

        CREATE FUNCTION gtfs.to_utc(
            service_date DATE, offset_seconds INTEGER, time_zone TEXT DEFAULT 'Europe/Helsinki'
        ) RETURNS TIMESTAMPTZ
        LANGUAGE sql STABLE STRICT PARALLEL SAFE AS $$
            SELECT gtfs.service_day_start(service_date, time_zone)
                 + make_interval(secs => offset_seconds)
        $$;
    """)


def downgrade() -> None:
    op.execute("""
        DROP FUNCTION gtfs.to_utc(DATE, INTEGER, TEXT);
        DROP FUNCTION gtfs.service_day_start(DATE, TEXT);
        DROP FUNCTION gtfs.time_to_seconds(TEXT);
        DROP TABLE gtfs.stop_times;
        DROP TABLE gtfs.trips;
        DROP TABLE gtfs.shapes;
        DROP TABLE gtfs.service_dates;
        DROP TABLE gtfs.calendar_dates;
        DROP TABLE gtfs.calendar;
        DROP TABLE gtfs.stops;
        DROP TABLE gtfs.routes;
        DROP TABLE gtfs.agencies;
        DROP TABLE gtfs.feed_versions;
    """)
