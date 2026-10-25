"""Set the password of the dashboards' read-only role from the environment."""

from __future__ import annotations

import psycopg
from psycopg import sql

from transit_platform.config import DashboardSettings, DatabaseSettings


def set_reader_password(conn: psycopg.Connection, settings: DashboardSettings) -> None:
    conn.execute(
        sql.SQL("ALTER ROLE {} PASSWORD {}").format(
            sql.Identifier(settings.reader_user), sql.Literal(settings.reader_password)
        )
    )


def main() -> None:
    with psycopg.connect(DatabaseSettings.from_env().conninfo, autocommit=True) as conn:
        set_reader_password(conn, DashboardSettings.from_env())
    print("password set for the dashboards' read-only role")


if __name__ == "__main__":
    main()
