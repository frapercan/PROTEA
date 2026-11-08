"""A window records which code built it

The three rule columns say what the delta IS by declaration. They do not say
it by implementation, and the two are not the same quantity: on 2026-11-07 a
fix to the synonym map's anchoring changed the delta of every window whose
corpus era is not its closing graph, with all eight identity fields
untouched. Before this column the only way to ask which code built a window
was to match its ``created_at`` against the dated blocks of
``agent-farm/plans/DECLARED-REVISION.txt``.

THE BACKFILL ONLY COPIES WHAT IS WRITTEN DOWN. ``job.started`` has carried a
``code_revision`` field since the revision stamp landed in the worker, so the
rows whose producing job is newer than that can be filled exactly. The older
ones are left NULL rather than inferred from the date: a date maps to a
declaration, not to a process, and the fleet runs out of a working tree that
can be edited between runs. On the database this was written against, 3 of
133 rows could be filled and 130 could not, among them the long validation
window and the prospective band.

NULL therefore means "ask the dated declaration", and never "the same as this
other row". ``protea.core.code_revision.revisions_conflict`` is the only
correct comparison: it refuses to conclude anything from an unidentifying
value instead of reporting agreement.

Additive and nullable, so downgrade drops the one column.

Revision ID: a7d3e9b1c604
Revises: c5e9a1d3f720
Create Date: 2026-11-07 20:50:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a7d3e9b1c604"
down_revision: str | Sequence[str] | None = "c5e9a1d3f720"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: Only from the event the worker writes, and only when it is there. The
#: join is on ``job_id``, which is nullable on ``evaluation_set``, so a row
#: whose job was deleted keeps its NULL.
BACKFILL = """
UPDATE evaluation_set AS es
   SET code_revision = e.fields ->> 'code_revision'
  FROM job_event AS e
 WHERE e.job_id = es.job_id
   AND e.event = 'job.started'
   AND e.fields ->> 'code_revision' IS NOT NULL
   AND es.code_revision IS NULL
"""


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "evaluation_set",
        sa.Column("code_revision", sa.String(length=64), nullable=True),
    )
    op.execute(BACKFILL)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("evaluation_set", "code_revision")
