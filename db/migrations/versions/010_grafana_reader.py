"""Read-only role for the dashboards: rt, marts and meta, including tables created later.

The role is created without a password; ``python -m transit_platform.reader_role`` sets it
from the environment. Roles belong to the whole server, so the role may already exist from
another database on it.

Revision ID: 010
Revises: 009
"""

from alembic import op

revision = "010"
down_revision = "009"
branch_labels = None
depends_on = None

SCHEMAS = ("rt", "marts", "meta")


def upgrade() -> None:
    op.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_reader') THEN
                CREATE ROLE grafana_reader LOGIN;
            END IF;
        END
        $$;
    """)
    op.execute("ALTER ROLE grafana_reader SET default_transaction_read_only = on")
    op.execute("ALTER ROLE grafana_reader SET statement_timeout = '30s'")
    op.execute("""
        DO $$
        BEGIN
            EXECUTE format('GRANT CONNECT ON DATABASE %I TO grafana_reader', current_database());
        END
        $$;
    """)
    for schema in SCHEMAS:
        op.execute(f"""
            GRANT USAGE ON SCHEMA {schema} TO grafana_reader;
            GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO grafana_reader;
            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO grafana_reader;
        """)


def downgrade() -> None:
    for schema in SCHEMAS:
        op.execute(f"""
            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} REVOKE SELECT ON TABLES FROM grafana_reader;
            REVOKE SELECT ON ALL TABLES IN SCHEMA {schema} FROM grafana_reader;
            REVOKE USAGE ON SCHEMA {schema} FROM grafana_reader;
        """)
    op.execute("""
        DO $$
        BEGIN
            EXECUTE format('REVOKE CONNECT ON DATABASE %I FROM grafana_reader', current_database());
            DROP ROLE grafana_reader;
        EXCEPTION WHEN dependent_objects_still_exist THEN
            RAISE NOTICE 'grafana_reader is still used by another database and is kept';
        END
        $$;
    """)
