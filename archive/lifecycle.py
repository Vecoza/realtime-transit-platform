"""Storage lifecycle: move old day partitions and replaced timetable versions to Parquet.

Every object goes through the same steps, each recorded in meta.archive_runs:

1. export to Parquet, written to a temporary file and renamed into place;
2. verify that the file holds exactly the rows PostgreSQL holds;
3. detach and drop the partition in one short transaction (real-time days), or delete the
   version (timetable).

A step that is already recorded for the same row count is not repeated, so the job can be
stopped at any point and run again. Nothing is dropped without a verified export.

Each real-time table has its own retention, by who reads it in PostgreSQL: positions feed the
live map and are kept two days, stop events and minute statistics a week. Partitions no
longer change by then: the sink rejects events more than a day old. Timetable versions are
exported as soon as they are loaded, because the analytical models read the timetable from
Parquet, and dropped once they are no longer needed for matching (docs/design_gtfs.md).
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import psycopg
from psycopg import sql

from transit_platform.config import ArchiveSettings, DatabaseSettings
from transit_platform.logs import configure_logging

logger = logging.getLogger(__name__)

DAY_TABLES = ("vehicle_positions", "stop_events", "route_minute_stats", "stop_minute_stats")
FEED_TABLES = ("stop_times", "trips", "service_dates", "routes", "stops")
FUTURE_DAYS = 7
LOCK_TIMEOUT = "10s"

# A version stays while it was current at some point in the hot window.
DROPPABLE_FEEDS_SQL = """
    SELECT feed_id
    FROM (
        SELECT feed_id, is_current, lead(built_on) OVER (ORDER BY built_on, feed_id) AS replaced_on
        FROM (
            SELECT feed_id, is_current,
                   coalesce(build_date, (loaded_at AT TIME ZONE 'Europe/Helsinki')::date)
                       AS built_on
            FROM gtfs.feed_versions
        ) AS built
    ) AS versions
    WHERE NOT is_current AND replaced_on <= %s
    ORDER BY feed_id
"""


class ArchiveError(RuntimeError):
    """An export did not match PostgreSQL; nothing was dropped."""


@dataclass(frozen=True)
class Step:
    step: str
    row_count: int | None
    path: str | None


@dataclass(frozen=True)
class Exported:
    rows: int
    size_bytes: int
    path: Path


class Lifecycle:
    def __init__(
        self,
        conn: psycopg.Connection,
        duck: duckdb.DuckDBPyConnection,
        settings: ArchiveSettings,
    ) -> None:
        self.conn = conn
        self.duck = duck
        self.settings = settings

    def run(self, today: date) -> bool:
        """One pass over everything that is due; False when another run holds the lock."""
        (locked,) = self.conn.execute(
            "SELECT pg_try_advisory_lock(hashtext('archive.lifecycle'))"
        ).fetchone()
        if not locked:
            logger.warning("event=lifecycle_skipped reason=another_run_active")
            return False
        try:
            self._run(today)
        finally:
            self.conn.execute("SELECT pg_advisory_unlock(hashtext('archive.lifecycle'))")
        return True

    def _run(self, today: date) -> None:
        self.conn.execute(sql.SQL("SET lock_timeout = {}").format(sql.Literal(LOCK_TIMEOUT)))
        self.conn.execute(
            "SELECT rt.ensure_day_partitions(%s, %s)", (today, today + timedelta(FUTURE_DAYS))
        )
        for table in DAY_TABLES:
            cutoff = today - timedelta(days=self.settings.keep_days(table))
            for day in self.day_partitions(table, before=cutoff):
                self.archive_day(table, day)
        for feed_id in self.feed_ids():
            self.export_feed(feed_id)
        for feed_id in self.droppable_feeds(today - timedelta(days=self.settings.stop_events_days)):
            self.drop_feed(feed_id)

    def day_partitions(self, table: str, before: date) -> list[date]:
        pattern = re.compile(rf"^{table}_(\d{{8}})$")
        names = self.conn.execute(
            """
            SELECT child.relname
            FROM pg_inherits
            JOIN pg_class AS child ON child.oid = pg_inherits.inhrelid
            WHERE pg_inherits.inhparent = to_regclass(%s)
            """,
            (f"rt.{table}",),
        ).fetchall()
        days = [
            datetime.strptime(match.group(1), "%Y%m%d").date()
            for (name,) in names
            if (match := pattern.match(name))
        ]
        return sorted(day for day in days if day < before)

    def archive_day(self, table: str, day: date) -> None:
        source = f"rt.{table}"
        key = day.isoformat()
        name = f"{table}_{day:%Y%m%d}"
        partition = sql.Identifier("rt", name)
        last = self.last_step(source, key)
        if last is not None and last.step == "dropped":
            self.record(source, key, "failed", detail="partition recreated after it was archived")
            logger.error("event=partition_recreated table=%s day=%s", table, key)
            return

        rows = self.count(partition)
        target = self.settings.directory / table / f"utc_day={key}" / "part-0.parquet"
        if not self.verified(last, rows, target):
            exported = self.export(f"SELECT * FROM pg.rt.{name}", target, source, key)
            self.verify(source, key, exported, rows)

        with self.conn.transaction():
            self.conn.execute(
                sql.SQL("ALTER TABLE {} DETACH PARTITION {}").format(
                    sql.Identifier("rt", table), partition
                )
            )
            self.record(source, key, "detached", row_count=rows)
            self.conn.execute(sql.SQL("DROP TABLE {}").format(partition))
            self.record(source, key, "dropped", row_count=rows, path=str(target))
        logger.info("event=day_archived table=%s day=%s rows=%d", table, key, rows)

    def export_feed(self, feed_id: int) -> None:
        for table in FEED_TABLES:
            source = f"gtfs.{table}"
            key = str(feed_id)
            name = f"stop_times_{feed_id}" if table == "stop_times" else table
            rows = self.count(sql.Identifier("gtfs", name), feed_id)
            directory = self.settings.directory / "gtfs" / table / f"feed_id={feed_id}"
            target = directory / "part-0.parquet"
            if self.verified(self.last_step(source, key), rows, target):
                continue
            exported = self.export(
                f"SELECT * EXCLUDE (feed_id) FROM pg.gtfs.{name} WHERE feed_id = {feed_id}",
                target,
                source,
                key,
            )
            self.verify(source, key, exported, rows)

    def droppable_feeds(self, cutoff: date) -> list[int]:
        return [row[0] for row in self.conn.execute(DROPPABLE_FEEDS_SQL, (cutoff,))]

    def drop_feed(self, feed_id: int) -> None:
        key = str(feed_id)
        for table in FEED_TABLES:
            last = self.last_step(f"gtfs.{table}", key)
            if last is None or last.step != "verified":
                raise ArchiveError(f"feed {feed_id}: gtfs.{table} has no verified export")
        with self.conn.transaction():
            self.conn.execute(
                sql.SQL("DROP TABLE IF EXISTS {}").format(
                    sql.Identifier("gtfs", f"stop_times_{feed_id}")
                )
            )
            self.conn.execute("DELETE FROM gtfs.feed_versions WHERE feed_id = %s", (feed_id,))
            self.record("gtfs.feed_versions", key, "dropped")
        logger.info("event=feed_dropped feed_id=%d", feed_id)

    def feed_ids(self) -> list[int]:
        return [row[0] for row in self.conn.execute("SELECT feed_id FROM gtfs.feed_versions")]

    def export(self, query: str, target: Path, source: str, key: str) -> Exported:
        started = time.perf_counter()
        target.parent.mkdir(parents=True, exist_ok=True)
        partial = target.with_name(target.name + ".partial")
        (rows,) = self.duck.execute(
            f"COPY ({query}) TO '{partial}' (FORMAT parquet, COMPRESSION zstd)"
        ).fetchone()
        os.replace(partial, target)
        exported = Exported(rows, target.stat().st_size, target)
        self.record(
            source,
            key,
            "exported",
            row_count=rows,
            size_bytes=exported.size_bytes,
            path=str(target),
            duration_ms=round((time.perf_counter() - started) * 1000),
        )
        return exported

    def verify(self, source: str, key: str, exported: Exported, rows: int) -> None:
        (in_file,) = self.duck.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(exported.path)]
        ).fetchone()
        if in_file != rows or exported.rows != rows:
            detail = f"postgres={rows} exported={exported.rows} file={in_file}"
            self.record(source, key, "failed", row_count=in_file, detail=detail)
            raise ArchiveError(f"{source} {key}: {detail}")
        self.record(
            source,
            key,
            "verified",
            row_count=rows,
            size_bytes=exported.size_bytes,
            path=str(exported.path),
        )

    def verified(self, last: Step | None, rows: int, target: Path) -> bool:
        """A verified export of exactly these rows is already in place."""
        if last is None or last.step != "verified" or last.row_count != rows:
            return False
        if not target.exists():
            return False
        (in_file,) = self.duck.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(target)]
        ).fetchone()
        return in_file == rows

    def count(self, relation: sql.Identifier, feed_id: int | None = None) -> int:
        query = sql.SQL("SELECT count(*) FROM {}").format(relation)
        if feed_id is None:
            return self.conn.execute(query).fetchone()[0]
        return self.conn.execute(query + sql.SQL(" WHERE feed_id = %s"), (feed_id,)).fetchone()[0]

    def last_step(self, source: str, key: str) -> Step | None:
        row = self.conn.execute(
            """
            SELECT step, row_count, path FROM meta.archive_runs
            WHERE source_table = %s AND object_key = %s AND step <> 'failed'
            ORDER BY step_id DESC
            LIMIT 1
            """,
            (source, key),
        ).fetchone()
        return None if row is None else Step(*row)

    def record(
        self,
        source: str,
        key: str,
        step: str,
        *,
        row_count: int | None = None,
        size_bytes: int | None = None,
        path: str | None = None,
        detail: str | None = None,
        duration_ms: int | None = None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO meta.archive_runs
                (source_table, object_key, step, row_count, size_bytes, path, detail, duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (source, key, step, row_count, size_bytes, path, detail, duration_ms),
        )


def connect_duckdb(database: DatabaseSettings) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with PostgreSQL attached read-only; the extension is installed at build."""
    duck = duckdb.connect(config={"autoinstall_known_extensions": False})
    duck.execute("LOAD postgres")
    conninfo = database.conninfo.replace("'", "''")
    duck.execute(f"ATTACH '{conninfo}' AS pg (TYPE postgres, READ_ONLY)")
    return duck


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--today", type=date.fromisoformat, default=datetime.now(UTC).date())
    args = parser.parse_args()

    database = DatabaseSettings.from_env()
    settings = ArchiveSettings.from_env()
    started = time.perf_counter()
    with (
        psycopg.connect(database.conninfo, autocommit=True) as conn,
        connect_duckdb(database) as duck,
    ):
        Lifecycle(conn, duck, settings).run(args.today)
    logger.info("event=lifecycle_finished duration_s=%.1f", time.perf_counter() - started)


if __name__ == "__main__":
    main()
