"""The server stamps the event, not the machine that emits it

WHY. job_event.ts, job.created_at and job_comment.created_at were stamped by the
Python process through a column default, so every row carried the clock of
whichever machine wrote it. This stream is shared between a server and a compute
node, and a node with a wrong clock therefore writes fiction into the common
history rather than a local mistake.

It is not hypothetical and it is not rare. On 2026-08-29 a compute node booted
with its real-time clock holding local time read as UTC; chrony stepped it back
by 7,199.5 seconds twenty seconds later, and the three events it stamped in
between made two readers reconstruct a 110-minute stall that never happened,
blame the wrong machine twice, and then clear a machine that had in fact written
193,303 rows.

Swept before this migration, while the two clocks still disagreed and the
inconsistency was still visible. The first reading of that sweep was wrong and
is recorded here because the correction is the useful part: 207,802 events
across 53 jobs sit after their own job's finished_at, and that is NOT clock
skew. Asked for the shape of the excess within each job, only 4 of the 53 show
the constant offset a wrong clock produces. The other 49 spread smoothly from
sixty seconds to hours, which is the signature of a job marked finished while
its workers kept emitting: encode_residue_sparse alone contributes 113,448
events spanning twenty hours. None of the 53 carries error_code lease_expired,
so the stale-job reaper does not explain them either.

So the measured extent of clock skew on this table is small and the extent of
premature closure is large. Both are real. This migration addresses only the
first, and it addresses it because a shared history stamped by whoever writes
to it cannot be audited at all: the 2026-08-29 events were found by reading a
log file on the other machine, not by anything the database could say.

EXISTING ROWS ARE NOT REPAIRED. Nothing can repair them: the true time was
never written down.

THE PYTHON DEFAULT IS REMOVED, NOT ACCOMPANIED. SQLAlchemy resolves default= at
flush and includes the column in the INSERT, so a server_default declared beside
one never fires. The pair would apply cleanly, pass review and change nothing,
which is the shape of failure this project keeps meeting.

Revision ID: c7e4a91b3d08
Revises: a4f8d2b91c73
"""

import sqlalchemy as sa

from alembic import op

revision = "c7e4a91b3d08"
down_revision = "a4f8d2b91c73"
branch_labels = None
depends_on = None

_COLUMNS = (
    ("job_event", "ts"),
    ("job", "created_at"),
    ("job_comment", "created_at"),
)


def upgrade() -> None:
    for table, column in _COLUMNS:
        op.alter_column(table, column, server_default=sa.text("now()"))


def downgrade() -> None:
    for table, column in _COLUMNS:
        op.alter_column(table, column, server_default=None)
