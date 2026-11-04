"""The exploration log refuses TRUNCATE too (ADR-D55)

``exploration_event_append_only`` is a row trigger, BEFORE UPDATE OR DELETE,
FOR EACH ROW (tgtype 27 on the deployed database). A row trigger does not fire
on TRUNCATE, which empties a table in one statement, so the log refused an
UPDATE and a DELETE and let the whole table go. Postgres needs a statement
trigger for TRUNCATE; this adds one on the same function, whose message names
the operation it refuses.

The server session found it reading the trigger after the deployment of
2026-11-04. Additive, so downgrade drops the one trigger.

Revision ID: c5e9a1d3f720
Revises: b2f6c3d8e154
Create Date: 2026-11-04 17:00:00.000000

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c5e9a1d3f720"
down_revision: str | Sequence[str] | None = "b2f6c3d8e154"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

NO_TRUNCATE_TRIGGER = """
CREATE TRIGGER exploration_event_no_truncate
    BEFORE TRUNCATE ON exploration_event
    FOR EACH STATEMENT EXECUTE FUNCTION exploration_event_is_append_only()
"""


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(NO_TRUNCATE_TRIGGER)


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("DROP TRIGGER IF EXISTS exploration_event_no_truncate ON exploration_event")
