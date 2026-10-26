"""Every provisioned dashboard and alert query runs as the dashboards' read-only role."""

import json
import re
import shutil
from datetime import date
from pathlib import Path

import psycopg
import pytest
import yaml

from analytics.publish import run as publish_marts
from archive.lifecycle import Lifecycle
from tests.conftest import REPO_ROOT, load_build
from tests.test_marts import (  # noqa: F401  (fixtures)
    DBT_DIR,
    FIXTURES,
    build_marts,
    lifecycle,
    store_saturday,
    warehouse,
)
from transit_platform.config import AnalyticsSettings, DatabaseSettings

GRAFANA = REPO_ROOT / "grafana"
MACROS = (
    (re.compile(r"\$__timeFilter\(([^)]+)\)"), r"\1 BETWEEN now() - interval '1 day' AND now()"),
    (re.compile(r"\$__timeGroupAlias\(([^,]+), '1m'\)"), r"date_trunc('minute', \1) AS time"),
    (re.compile(r"\$day"), "2026-10-24"),
    (re.compile(r"\$mode"), "'bus', 'tram'"),
    (re.compile(r"\$min_(journeys|arrivals|trips|pairs)"), "5"),
)


def dashboard_queries() -> list[tuple[str, str]]:
    queries = []
    for path in sorted((GRAFANA / "dashboards").glob("*.json")):
        board = json.loads(path.read_text())
        for variable in board["templating"]["list"]:
            if variable["type"] == "query":
                queries.append((f"{path.stem}: ${variable['name']}", variable["query"]))
        for panel in board["panels"]:
            for target in panel.get("targets", []):
                queries.append((f"{path.stem}: {panel['title']}", target["rawSql"]))
    rules = yaml.safe_load((GRAFANA / "provisioning" / "alerting" / "rules.yaml").read_text())
    for group in rules["groups"]:
        for rule in group["rules"]:
            for step in rule["data"]:
                if step["datasourceUid"] != "__expr__":
                    queries.append((f"alert: {rule['title']}", step["model"]["rawSql"]))
    return queries


def expand(sql: str) -> str:
    for pattern, replacement in MACROS:
        sql = pattern.sub(replacement, sql)
    return sql


@pytest.fixture
def published(
    conn: psycopg.Connection,
    migrated: DatabaseSettings,
    lifecycle: Lifecycle,  # noqa: F811
    warehouse: Path,  # noqa: F811
    tmp_path: Path,
) -> None:
    feed_dir = tmp_path / "gtfs"
    shutil.copytree(FIXTURES / "gtfs_saturday_routes", feed_dir)
    load_build(conn, feed_dir, tmp_path, "2026-10-24")
    conn.commit()
    store_saturday(conn)
    lifecycle.run(date(2026, 10, 25))
    build_marts(warehouse)
    publish_marts(migrated, AnalyticsSettings(warehouse), DBT_DIR / "target" / "run_results.json")


def test_dashboards_and_alerts_only_query_what_the_reader_role_may_read(
    conn: psycopg.Connection, published: None
) -> None:
    queries = dashboard_queries()
    assert len(queries) > 30
    conn.execute("SET ROLE grafana_reader")
    returned_rows = 0
    for name, sql in queries:
        assert "$" not in expand(sql), name
        with conn.transaction():
            rows = conn.execute(expand(sql)).fetchall()
        returned_rows += bool(rows)
    conn.execute("RESET ROLE")
    assert returned_rows > len(queries) / 2
