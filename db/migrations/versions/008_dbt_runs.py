"""dbt runs: each hourly build of the analytical models and whether its marts were published.

Revision ID: 008
Revises: 007
"""

from alembic import op

revision = "008"
down_revision = "007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE meta.dbt_runs (
            invocation_id     UUID NOT NULL,
            started_at        TIMESTAMPTZ NOT NULL,
            finished_at       TIMESTAMPTZ NOT NULL,
            elapsed_s         DOUBLE PRECISION NOT NULL,
            models            INTEGER NOT NULL,
            tests             INTEGER NOT NULL,
            failed            INTEGER NOT NULL,
            published_tables  INTEGER NOT NULL,
            published_at      TIMESTAMPTZ,
            CONSTRAINT pk_dbt_runs PRIMARY KEY (invocation_id),
            CONSTRAINT ck_dbt_runs_published
                CHECK ((published_tables > 0) = (published_at IS NOT NULL)),
            CONSTRAINT ck_dbt_runs_failed_not_published
                CHECK (failed = 0 OR published_tables = 0)
        );

        CREATE INDEX ix_dbt_runs_finished_at ON meta.dbt_runs (finished_at);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE meta.dbt_runs")
