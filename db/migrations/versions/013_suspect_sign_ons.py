"""Suspect sign-ons: a vehicle still reporting a journey whose planned end is long past.

Revision ID: 013
Revises: 012
"""

from alembic import op

revision = "013"
down_revision = "012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE rt.vehicle_status ADD COLUMN suspect BOOLEAN NOT NULL DEFAULT false")


def downgrade() -> None:
    op.execute("ALTER TABLE rt.vehicle_status DROP COLUMN suspect")
