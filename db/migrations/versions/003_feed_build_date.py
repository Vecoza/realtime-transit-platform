"""Feed build date: choose the feed for an operating day by when HSL built it.

Revision ID: 003
Revises: 002
"""

from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE gtfs.feed_versions ADD COLUMN build_date DATE;

        UPDATE gtfs.feed_versions
        SET build_date = to_date(left(feed_version, 10), 'YYYY-MM-DD')
        WHERE feed_version ~ '^\\d{4}-\\d{2}-\\d{2}';

        CREATE OR REPLACE FUNCTION gtfs.feed_for_service_date(service_date DATE) RETURNS BIGINT
        LANGUAGE sql STABLE STRICT PARALLEL SAFE AS $$
            WITH candidates AS (
                SELECT feed_id,
                       coalesce(build_date, (loaded_at AT TIME ZONE 'Europe/Helsinki')::date)
                           AS built_on,
                       feed_version,
                       loaded_at
                FROM gtfs.feed_versions
                WHERE service_date BETWEEN valid_from AND valid_to
            )
            SELECT feed_id
            FROM candidates
            ORDER BY built_on <= service_date DESC,
                     CASE WHEN built_on <= service_date THEN built_on END DESC,
                     built_on,
                     feed_version DESC NULLS LAST,
                     loaded_at DESC
            LIMIT 1
        $$;
    """)


def downgrade() -> None:
    op.execute("""
        CREATE OR REPLACE FUNCTION gtfs.feed_for_service_date(service_date DATE) RETURNS BIGINT
        LANGUAGE sql STABLE STRICT PARALLEL SAFE AS $$
            SELECT feed_id
            FROM gtfs.feed_versions
            WHERE service_date BETWEEN valid_from AND valid_to
            ORDER BY loaded_at < service_date + 1 DESC,
                     CASE WHEN loaded_at < service_date + 1 THEN loaded_at END DESC,
                     loaded_at
            LIMIT 1
        $$;

        ALTER TABLE gtfs.feed_versions DROP COLUMN build_date;
    """)
