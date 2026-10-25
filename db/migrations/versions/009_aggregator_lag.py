"""Aggregator lag: messages behind the end of the topic after each batch, as for the sink.

Revision ID: 009
Revises: 008
"""

from alembic import op

revision = "009"
down_revision = "008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE meta.aggregator_metrics ADD COLUMN consumer_lag BIGINT;
        CREATE INDEX ix_aggregator_metrics_committed_at
            ON meta.aggregator_metrics (group_id, committed_at);
    """)


def downgrade() -> None:
    op.execute("""
        DROP INDEX meta.ix_aggregator_metrics_committed_at;
        ALTER TABLE meta.aggregator_metrics DROP COLUMN consumer_lag;
    """)
