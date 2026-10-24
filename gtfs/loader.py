"""Load a GTFS zip into the ``gtfs`` schema as a new feed version.

A load runs in a single transaction: stage every file with COPY, validate, insert the feed
version and its rows, expand service dates, and finally make it the current feed. A failure
at any step leaves the previous current feed untouched. Loading a zip whose checksum is
already known does not copy any data.
"""

from __future__ import annotations

import csv
import enum
import io
import logging
import re
import time
import zipfile
from dataclasses import dataclass, field
from datetime import date
from typing import IO

import psycopg
from psycopg import sql

from gtfs.download import FeedFile
from gtfs.files import FILES, STOP_TIMES, GtfsFile

logger = logging.getLogger(__name__)

COPY_CHUNK_SIZE = 1 << 20
LOAD_LOCK_KEY = 7_013_001

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
_BUILD_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}")


class InvalidFeedError(Exception):
    """The zip is not a GTFS feed this loader can accept."""


class LoadStatus(enum.StrEnum):
    LOADED = "loaded"
    UNCHANGED = "unchanged"
    REACTIVATED = "reactivated"


@dataclass(frozen=True)
class LoadResult:
    status: LoadStatus
    feed_id: int
    valid_from: date | None = None
    valid_to: date | None = None
    row_counts: dict[str, int] = field(default_factory=dict)
    duration_seconds: float = 0.0


def load_feed(conn: psycopg.Connection, feed: FeedFile) -> LoadResult:
    """Load ``feed`` and make it current, or reuse the version with the same checksum."""
    started = time.perf_counter()
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (LOAD_LOCK_KEY,))
        known = conn.execute(
            "SELECT feed_id, is_current FROM gtfs.feed_versions WHERE sha256 = %s",
            (feed.sha256,),
        ).fetchone()
        if known is not None:
            feed_id, is_current = known
            if not is_current:
                switch_current(conn, feed_id)
            status = LoadStatus.UNCHANGED if is_current else LoadStatus.REACTIVATED
            logger.info("event=feed_skipped status=%s feed_id=%d", status, feed_id)
            return LoadResult(status, feed_id, duration_seconds=time.perf_counter() - started)

        with zipfile.ZipFile(feed.path) as archive:
            for gtfs_file in FILES:
                stage_file(conn, archive, gtfs_file)
            built_on = build_date(conn, archive)
        valid_from, valid_to = staged_validity(conn)
        feed_id = insert_feed_version(conn, feed, valid_from, valid_to, built_on)
        create_stop_times_table(conn, feed_id)
        row_counts = publish_rows(conn, feed_id)
        attach_stop_times_table(conn, feed_id)
        set_trip_start_offsets(conn, feed_id)
        row_counts["service_dates"] = expand_service_dates(conn, feed_id)
        analyze_tables(conn, feed_id)
        switch_current(conn, feed_id)

    duration = time.perf_counter() - started
    logger.info(
        "event=feed_loaded feed_id=%d valid_from=%s valid_to=%s stop_times=%d duration_s=%.1f",
        feed_id,
        valid_from,
        valid_to,
        row_counts["stop_times"],
        duration,
    )
    return LoadResult(LoadStatus.LOADED, feed_id, valid_from, valid_to, row_counts, duration)


def read_header(stream: IO[bytes], file_name: str) -> list[str]:
    """Column names from the first line, without a byte order mark; rejects unusable names."""
    line = stream.readline().decode("utf-8-sig")
    header = [name.strip() for name in next(csv.reader(io.StringIO(line)), [])]
    invalid = [name for name in header if not _IDENTIFIER.match(name)]
    if not header or invalid:
        raise InvalidFeedError(f"{file_name}: unusable header columns {invalid or header}")
    if len(set(header)) != len(header):
        raise InvalidFeedError(f"{file_name}: duplicate header columns")
    return header


def check_columns(gtfs_file: GtfsFile, header: list[str]) -> None:
    missing = [name for name in gtfs_file.required_columns if name not in header]
    if missing:
        raise InvalidFeedError(f"{gtfs_file.name}: missing required columns {missing}")


def stage_file(conn: psycopg.Connection, archive: zipfile.ZipFile, gtfs_file: GtfsFile) -> None:
    """COPY one file into a text-only temporary table, adding absent optional columns."""
    if gtfs_file.name not in archive.namelist():
        if gtfs_file.required:
            raise InvalidFeedError(f"missing required file {gtfs_file.name}")
        create_staging_table(conn, gtfs_file, list(gtfs_file.columns))
        return

    started = time.perf_counter()
    with archive.open(gtfs_file.name) as stream:
        header = read_header(stream, gtfs_file.name)
        check_columns(gtfs_file, header)
        create_staging_table(conn, gtfs_file, header)
        copy_rows(conn, gtfs_file.staging_table, header, stream)
    logger.info(
        "event=file_staged file=%s duration_s=%.1f",
        gtfs_file.name,
        time.perf_counter() - started,
    )


def create_staging_table(conn: psycopg.Connection, gtfs_file: GtfsFile, header: list[str]) -> None:
    columns = header + [name for name in gtfs_file.columns if name not in header]
    conn.execute(
        sql.SQL("CREATE TEMPORARY TABLE {} ({}) ON COMMIT DROP").format(
            sql.Identifier(gtfs_file.staging_table),
            sql.SQL(", ").join(sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in columns),
        )
    )


def copy_rows(conn: psycopg.Connection, table: str, header: list[str], stream: IO[bytes]) -> None:
    statement = sql.SQL("COPY {} ({}) FROM STDIN (FORMAT csv)").format(
        sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, header))
    )
    with conn.cursor().copy(statement) as copy:
        while chunk := stream.read(COPY_CHUNK_SIZE):
            copy.write(chunk)


def staged_validity(conn: psycopg.Connection) -> tuple[date, date]:
    """First and last day any service runs, from calendar ranges and added dates."""
    first_day, last_day = conn.execute("""
        WITH bounds AS (
            SELECT to_date(btrim(start_date), 'YYYYMMDD') AS first_day,
                   to_date(btrim(end_date), 'YYYYMMDD') AS last_day
            FROM staging_calendar
            UNION ALL
            SELECT to_date(btrim(date), 'YYYYMMDD'), to_date(btrim(date), 'YYYYMMDD')
            FROM staging_calendar_dates
            WHERE btrim(exception_type) = '1'
        )
        SELECT min(first_day), max(last_day) FROM bounds
    """).fetchone()
    if first_day is None:
        raise InvalidFeedError("calendar.txt and calendar_dates.txt define no service days")
    return first_day, last_day


def build_date(conn: psycopg.Connection, archive: zipfile.ZipFile) -> date | None:
    """The day the publisher built the feed.

    HSL writes the build time as ``feed_version`` (``2026-10-24 06:04:45``). Other feeds fall
    back to the newest modification date of the GTFS files in the zip.
    """
    feed_version = staged_feed_version(conn)
    if feed_version and (found := _BUILD_DATE.match(feed_version)):
        return date.fromisoformat(found.group(0))
    names = {gtfs_file.name for gtfs_file in FILES}
    newest = max(
        (info.date_time for info in archive.infolist() if info.filename in names), default=None
    )
    if newest is None or newest[0] <= 1980:
        return None
    return date(*newest[:3])


def staged_feed_version(conn: psycopg.Connection) -> str | None:
    return conn.execute("SELECT max(feed_version) FROM staging_feed_info").fetchone()[0]


def insert_feed_version(
    conn: psycopg.Connection,
    feed: FeedFile,
    valid_from: date,
    valid_to: date,
    built_on: date | None,
) -> int:
    row = conn.execute(
        """
        INSERT INTO gtfs.feed_versions (
            source_url, sha256, size_bytes, feed_version, build_date, downloaded_at,
            valid_from, valid_to
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING feed_id
        """,
        (
            feed.source_url,
            feed.sha256,
            feed.size_bytes,
            staged_feed_version(conn),
            built_on,
            feed.downloaded_at,
            valid_from,
            valid_to,
        ),
    ).fetchone()
    return row[0]


def stop_times_table(feed_id: int) -> sql.Identifier:
    return sql.Identifier("gtfs", f"stop_times_{feed_id}")


def create_stop_times_table(conn: psycopg.Connection, feed_id: int) -> None:
    """A plain table for one feed's stop times, without indexes or foreign keys yet.

    The check on ``feed_id`` lets the later ATTACH skip scanning the rows for the partition
    bound.
    """
    table = stop_times_table(feed_id)
    conn.execute(
        sql.SQL(
            "CREATE TABLE {} (LIKE gtfs.stop_times INCLUDING DEFAULTS INCLUDING CONSTRAINTS)"
        ).format(table)
    )
    conn.execute(
        sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} CHECK (feed_id = {})").format(
            table, sql.Identifier(f"ck_stop_times_{feed_id}_feed"), sql.Literal(feed_id)
        )
    )


def attach_stop_times_table(conn: psycopg.Connection, feed_id: int) -> None:
    """Attach the loaded table as a partition: builds its primary key and checks foreign keys
    with one query per constraint instead of one lookup per row."""
    started = time.perf_counter()
    conn.execute(
        sql.SQL("ALTER TABLE gtfs.stop_times ATTACH PARTITION {} FOR VALUES IN ({})").format(
            stop_times_table(feed_id), sql.Literal(feed_id)
        )
    )
    logger.info(
        "event=partition_attached table=stop_times_%d duration_s=%.1f",
        feed_id,
        time.perf_counter() - started,
    )


def publish_rows(conn: psycopg.Connection, feed_id: int) -> dict[str, int]:
    """Move staged rows into the gtfs tables in foreign key order; returns rows per table."""
    counts: dict[str, int] = {}
    for gtfs_file in FILES:
        if gtfs_file.table is None:
            continue
        started = time.perf_counter()
        target = (
            stop_times_table(feed_id)
            if gtfs_file is STOP_TIMES
            else sql.Identifier("gtfs", gtfs_file.table)
        )
        statement = sql.SQL(gtfs_file.insert_sql).format(table=target)
        cursor = conn.execute(statement, {"feed_id": feed_id})
        counts[gtfs_file.table] = cursor.rowcount
        logger.info(
            "event=table_loaded table=%s rows=%d duration_s=%.1f",
            gtfs_file.table,
            cursor.rowcount,
            time.perf_counter() - started,
        )
    return counts


def set_trip_start_offsets(conn: psycopg.Connection, feed_id: int) -> None:
    """Departure offset of each trip's first stop: the scheduled start real-time events carry."""
    conn.execute(
        """
        UPDATE gtfs.trips AS t
        SET start_offset_seconds = first_stop.departure_offset_seconds
        FROM (
            SELECT DISTINCT ON (trip_id) trip_id, departure_offset_seconds
            FROM gtfs.stop_times
            WHERE feed_id = %(feed_id)s
            ORDER BY trip_id, stop_sequence
        ) AS first_stop
        WHERE t.feed_id = %(feed_id)s AND t.trip_id = first_stop.trip_id
        """,
        {"feed_id": feed_id},
    )


def expand_service_dates(conn: psycopg.Connection, feed_id: int) -> int:
    """One row per day a service runs: weekly pattern, minus removed, plus added dates."""
    cursor = conn.execute(
        """
        INSERT INTO gtfs.service_dates (feed_id, service_date, service_id)
        WITH weekly AS (
            SELECT c.service_id, day::DATE AS service_date
            FROM gtfs.calendar AS c
            CROSS JOIN LATERAL generate_series(c.start_date, c.end_date, INTERVAL '1 day') AS day
            WHERE c.feed_id = %(feed_id)s
              AND (ARRAY[c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday,
                         c.sunday])[extract(ISODOW FROM day)::INTEGER]
        ),
        exceptions AS (
            SELECT service_id, service_date, exception_type
            FROM gtfs.calendar_dates
            WHERE feed_id = %(feed_id)s
        )
        SELECT %(feed_id)s, w.service_date, w.service_id
        FROM weekly AS w
        WHERE NOT EXISTS (
            SELECT 1
            FROM exceptions AS e
            WHERE e.service_id = w.service_id
              AND e.service_date = w.service_date
              AND e.exception_type = 2
        )
        UNION
        SELECT %(feed_id)s, service_date, service_id
        FROM exceptions
        WHERE exception_type = 1
        """,
        {"feed_id": feed_id},
    )
    return cursor.rowcount


def analyze_tables(conn: psycopg.Connection, feed_id: int) -> None:
    """Refresh planner statistics; for stop_times only the new partition is analyzed."""
    tables = [f.table for f in FILES if f.table not in (None, STOP_TIMES.table)]
    tables += ["service_dates", f"stop_times_{feed_id}"]
    for table in tables:
        conn.execute(sql.SQL("ANALYZE {}").format(sql.Identifier("gtfs", table)))


def switch_current(conn: psycopg.Connection, feed_id: int) -> None:
    conn.execute("UPDATE gtfs.feed_versions SET is_current = false WHERE is_current")
    conn.execute("UPDATE gtfs.feed_versions SET is_current = true WHERE feed_id = %s", (feed_id,))
