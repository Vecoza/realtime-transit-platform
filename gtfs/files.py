"""The GTFS files the loader reads and how each one maps onto its table.

Every file is first copied as text into a temporary staging table named after the file.
For files with a ``table``, ``insert_sql`` casts, cleans and moves those rows into the
``gtfs`` schema; ``{table}`` is filled in by the loader.
Blank and whitespace-only values become NULL.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GtfsFile:
    name: str
    table: str | None
    required_columns: tuple[str, ...]
    optional_columns: tuple[str, ...] = ()
    required: bool = True
    insert_sql: str | None = None

    @property
    def staging_table(self) -> str:
        return f"staging_{self.name.removesuffix('.txt')}"

    @property
    def columns(self) -> tuple[str, ...]:
        return self.required_columns + self.optional_columns


FEED_INFO = GtfsFile(
    name="feed_info.txt",
    table=None,
    required=False,
    required_columns=(),
    optional_columns=("feed_version",),
)

AGENCY = GtfsFile(
    name="agency.txt",
    table="agencies",
    required_columns=("agency_id", "agency_name", "agency_url", "agency_timezone"),
    insert_sql="""
        INSERT INTO {table} (feed_id, agency_id, agency_name, agency_url, agency_timezone)
        SELECT %(feed_id)s, btrim(agency_id), btrim(agency_name), btrim(agency_url),
               btrim(agency_timezone)
        FROM staging_agency
    """,
)

ROUTES = GtfsFile(
    name="routes.txt",
    table="routes",
    required_columns=("route_id", "route_type"),
    optional_columns=("agency_id", "route_short_name", "route_long_name"),
    insert_sql="""
        INSERT INTO {table} (
            feed_id, route_id, agency_id, route_short_name, route_long_name, route_type
        )
        SELECT %(feed_id)s, btrim(route_id),
               coalesce(NULLIF(btrim(agency_id), ''),
                        (SELECT min(agency_id) FROM gtfs.agencies WHERE feed_id = %(feed_id)s)),
               NULLIF(btrim(route_short_name), ''), NULLIF(btrim(route_long_name), ''),
               btrim(route_type)::INTEGER
        FROM staging_routes
    """,
)

STOPS = GtfsFile(
    name="stops.txt",
    table="stops",
    required_columns=("stop_id",),
    optional_columns=(
        "stop_code",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "zone_id",
        "location_type",
        "parent_station",
        "platform_code",
    ),
    insert_sql="""
        INSERT INTO {table} (
            feed_id, stop_id, stop_code, stop_name, stop_lat, stop_lon, zone_id,
            location_type, parent_station, platform_code
        )
        SELECT %(feed_id)s, btrim(stop_id), NULLIF(btrim(stop_code), ''),
               NULLIF(btrim(stop_name), ''),
               NULLIF(btrim(stop_lat), '')::DOUBLE PRECISION,
               NULLIF(btrim(stop_lon), '')::DOUBLE PRECISION,
               NULLIF(btrim(zone_id), ''),
               coalesce(NULLIF(btrim(location_type), '')::SMALLINT, 0),
               NULLIF(btrim(parent_station), ''), NULLIF(btrim(platform_code), '')
        FROM staging_stops
    """,
)

CALENDAR = GtfsFile(
    name="calendar.txt",
    table="calendar",
    required=False,
    required_columns=(
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "start_date",
        "end_date",
    ),
    insert_sql="""
        INSERT INTO {table} (
            feed_id, service_id, monday, tuesday, wednesday, thursday, friday, saturday,
            sunday, start_date, end_date
        )
        SELECT %(feed_id)s, btrim(service_id),
               btrim(monday) = '1', btrim(tuesday) = '1', btrim(wednesday) = '1',
               btrim(thursday) = '1', btrim(friday) = '1', btrim(saturday) = '1',
               btrim(sunday) = '1',
               to_date(btrim(start_date), 'YYYYMMDD'), to_date(btrim(end_date), 'YYYYMMDD')
        FROM staging_calendar
    """,
)

CALENDAR_DATES = GtfsFile(
    name="calendar_dates.txt",
    table="calendar_dates",
    required=False,
    required_columns=("service_id", "date", "exception_type"),
    insert_sql="""
        INSERT INTO {table} (feed_id, service_id, service_date, exception_type)
        SELECT %(feed_id)s, btrim(service_id), to_date(btrim(date), 'YYYYMMDD'),
               btrim(exception_type)::SMALLINT
        FROM staging_calendar_dates
    """,
)

SHAPES = GtfsFile(
    name="shapes.txt",
    table="shapes",
    required=False,
    required_columns=("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"),
    optional_columns=("shape_dist_traveled",),
    insert_sql="""
        INSERT INTO {table} (
            feed_id, shape_id, shape_pt_sequence, shape_pt_lat, shape_pt_lon,
            shape_dist_traveled
        )
        SELECT %(feed_id)s, btrim(shape_id), btrim(shape_pt_sequence)::INTEGER,
               btrim(shape_pt_lat)::DOUBLE PRECISION, btrim(shape_pt_lon)::DOUBLE PRECISION,
               NULLIF(btrim(shape_dist_traveled), '')::DOUBLE PRECISION
        FROM staging_shapes
    """,
)

TRIPS = GtfsFile(
    name="trips.txt",
    table="trips",
    required_columns=("route_id", "service_id", "trip_id"),
    optional_columns=("trip_headsign", "direction_id", "shape_id"),
    insert_sql="""
        INSERT INTO {table} (
            feed_id, trip_id, route_id, service_id, trip_headsign, direction_id, shape_id
        )
        SELECT %(feed_id)s, btrim(trip_id), btrim(route_id), btrim(service_id),
               NULLIF(btrim(trip_headsign), ''),
               NULLIF(btrim(direction_id), '')::SMALLINT,
               NULLIF(btrim(shape_id), '')
        FROM staging_trips
    """,
)

STOP_TIMES = GtfsFile(
    name="stop_times.txt",
    table="stop_times",
    required_columns=("trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"),
    optional_columns=("stop_headsign", "pickup_type", "drop_off_type", "timepoint"),
    insert_sql="""
        INSERT INTO {table} (
            feed_id, trip_id, stop_sequence, stop_id, arrival_offset_seconds,
            departure_offset_seconds, stop_headsign, pickup_type, drop_off_type, timepoint
        )
        SELECT %(feed_id)s, btrim(trip_id), btrim(stop_sequence)::INTEGER, btrim(stop_id),
               gtfs.time_to_seconds(NULLIF(btrim(arrival_time), '')),
               gtfs.time_to_seconds(NULLIF(btrim(departure_time), '')),
               NULLIF(btrim(stop_headsign), ''),
               NULLIF(btrim(pickup_type), '')::SMALLINT,
               NULLIF(btrim(drop_off_type), '')::SMALLINT,
               NULLIF(btrim(timepoint), '')::SMALLINT
        FROM staging_stop_times
    """,
)

FILES: tuple[GtfsFile, ...] = (
    FEED_INFO,
    AGENCY,
    ROUTES,
    STOPS,
    CALENDAR,
    CALENDAR_DATES,
    SHAPES,
    TRIPS,
    STOP_TIMES,
)
