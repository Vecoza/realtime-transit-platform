"""Publish the analytical marts from DuckDB into the PostgreSQL schema ``marts``.

Grafana reads only PostgreSQL. Each mart is copied into a new table next to the current one,
given its primary key, and swapped in with two renames in one short transaction, so a reader
sees either the previous build or this one, never a half-filled table. Privileges for readers
come from the schema's default privileges, so a swapped table is readable at once.

The dbt run itself, read from dbt's ``run_results.json``, is recorded in ``meta.dbt_runs``,
also when it failed. A run with a failed model or test publishes nothing.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb
import psycopg
from psycopg import sql

from transit_platform.config import AnalyticsSettings, DatabaseSettings
from transit_platform.logs import configure_logging

logger = logging.getLogger(__name__)

MARTS = {
    "mart_on_time_performance": ("service_date", "hour_local", "route_id"),
    "mart_stop_punctuality": ("service_date", "hour_local", "stop_id"),
    "mart_bunching": ("service_date", "hour_local", "route_id", "direction_id", "stop_id"),
    "mart_data_completeness": ("service_date", "route_id"),
}
FAILED = {"error", "fail", "runtime error"}
LOCK_TIMEOUT = "10s"


@dataclass(frozen=True)
class DbtRun:
    invocation_id: str
    started_at: datetime
    finished_at: datetime
    elapsed_s: float
    models: int
    tests: int
    failed: int

    @classmethod
    def from_results(cls, path: Path) -> DbtRun:
        results = json.loads(path.read_text())
        statuses = [(r["unique_id"].split(".")[0], r["status"]) for r in results["results"]]
        metadata = results["metadata"]
        return cls(
            invocation_id=metadata["invocation_id"],
            started_at=datetime.fromisoformat(metadata["invocation_started_at"]),
            finished_at=datetime.fromisoformat(metadata["generated_at"]),
            elapsed_s=results["elapsed_time"],
            models=sum(kind == "model" for kind, _ in statuses),
            tests=sum(kind == "test" for kind, _ in statuses),
            failed=sum(status in FAILED for _, status in statuses),
        )


def publish(conn: psycopg.Connection, duck: duckdb.DuckDBPyConnection) -> int:
    """Copy every mart into ``marts`` and swap it in; returns the number of tables."""
    for mart, key in MARTS.items():
        started = time.perf_counter()
        next_table = f"{mart}__next"
        conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier("marts", next_table)))
        duck.execute(f"CREATE TABLE pg.marts.{next_table} AS SELECT * FROM warehouse.{mart}")
        conn.execute(
            sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY ({})").format(
                sql.Identifier("marts", next_table),
                sql.Identifier(f"pk_{mart}__next"),
                sql.SQL(", ").join(map(sql.Identifier, key)),
            )
        )
        conn.execute(sql.SQL("ANALYZE {}").format(sql.Identifier("marts", next_table)))
        with conn.transaction():
            conn.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(LOCK_TIMEOUT)))
            conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier("marts", mart)))
            conn.execute(
                sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                    sql.Identifier("marts", next_table), sql.Identifier(mart)
                )
            )
            conn.execute(
                sql.SQL("ALTER TABLE {} RENAME CONSTRAINT {} TO {}").format(
                    sql.Identifier("marts", mart),
                    sql.Identifier(f"pk_{mart}__next"),
                    sql.Identifier(f"pk_{mart}"),
                )
            )
        (rows,) = conn.execute(
            sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier("marts", mart))
        ).fetchone()
        logger.info(
            "event=mart_published table=%s rows=%d duration_ms=%d",
            mart,
            rows,
            round((time.perf_counter() - started) * 1000),
        )
    return len(MARTS)


def record_run(conn: psycopg.Connection, run: DbtRun, published: int) -> None:
    conn.execute(
        """
        INSERT INTO meta.dbt_runs (invocation_id, started_at, finished_at, elapsed_s, models,
                                   tests, failed, published_tables, published_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CASE WHEN %s > 0 THEN clock_timestamp() END)
        ON CONFLICT (invocation_id) DO UPDATE
        SET published_tables = excluded.published_tables, published_at = excluded.published_at
        """,
        (
            run.invocation_id,
            run.started_at,
            run.finished_at,
            run.elapsed_s,
            run.models,
            run.tests,
            run.failed,
            published,
            published,
        ),
    )


def connect_duckdb(warehouse: Path, database: DatabaseSettings) -> duckdb.DuckDBPyConnection:
    duck = duckdb.connect(config={"autoinstall_known_extensions": False})
    duck.execute("LOAD postgres")
    duck.execute(f"ATTACH '{warehouse}' AS warehouse (READ_ONLY)")
    conninfo = database.conninfo.replace("'", "''")
    duck.execute(f"ATTACH '{conninfo}' AS pg (TYPE postgres)")
    return duck


def run(database: DatabaseSettings, settings: AnalyticsSettings, results: Path) -> int:
    dbt_run = DbtRun.from_results(results)
    with psycopg.connect(database.conninfo, autocommit=True) as conn:
        published = 0
        if dbt_run.failed == 0:
            with connect_duckdb(settings.duckdb_path, database) as duck:
                published = publish(conn, duck)
        else:
            logger.error("event=marts_not_published failed=%d", dbt_run.failed)
        record_run(conn, dbt_run, published)
    return published


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run-results", type=Path, default=Path("dbt/target/run_results.json"))
    args = parser.parse_args()
    run(DatabaseSettings.from_env(), AnalyticsSettings.from_env(), args.run_results)


if __name__ == "__main__":
    main()
