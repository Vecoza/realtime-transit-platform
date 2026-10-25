"""Partition creation skips days that were archived.

A replay of old data would otherwise create an empty partition for a day that already lives
in Parquet. Rows for such a day now land in the table's default partition, where they stay
out of the analytical models and show up in the partition sizes.

Revision ID: 011
Revises: 010
"""

from alembic import op

revision = "011"
down_revision = "010"
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
        FOREACH parent IN ARRAY ARRAY[
            'vehicle_positions', 'stop_events', 'route_minute_stats', 'stop_minute_stats'
        ] LOOP
            FOR day IN SELECT generate_series(first_day, last_day, INTERVAL '1 day')::DATE
            LOOP
                partition_name := format('%s_%s', parent, to_char(day, 'YYYYMMDD'));
                IF to_regclass(format('rt.%I', partition_name)) IS NULL
                   {archived_check}THEN
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
ARCHIVED_CHECK = """AND NOT EXISTS (
                       SELECT 1 FROM meta.archive_runs
                       WHERE source_table = 'rt.' || parent
                         AND object_key = day::TEXT
                         AND step = 'dropped'
                   )
                   """


def upgrade() -> None:
    op.execute(ENSURE_DAY_PARTITIONS.format(archived_check=ARCHIVED_CHECK))


def downgrade() -> None:
    op.execute(ENSURE_DAY_PARTITIONS.format(archived_check=""))
