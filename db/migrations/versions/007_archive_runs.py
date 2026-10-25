"""Archive log: every export, verification and drop of a day partition or a feed version.

Revision ID: 007
Revises: 006
"""

from alembic import op

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE meta.archive_runs (
            step_id       BIGINT GENERATED ALWAYS AS IDENTITY,
            source_table  TEXT NOT NULL,
            object_key    TEXT NOT NULL,
            step          TEXT NOT NULL,
            row_count     BIGINT,
            size_bytes    BIGINT,
            path          TEXT,
            detail        TEXT,
            duration_ms   INTEGER,
            recorded_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT pk_archive_runs PRIMARY KEY (step_id),
            CONSTRAINT ck_archive_runs_step
                CHECK (step IN ('exported', 'verified', 'detached', 'dropped', 'failed')),
            CONSTRAINT ck_archive_runs_counted
                CHECK (step NOT IN ('exported', 'verified') OR row_count IS NOT NULL)
        );

        CREATE INDEX ix_archive_runs_object
            ON meta.archive_runs (source_table, object_key, step_id);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE meta.archive_runs")
