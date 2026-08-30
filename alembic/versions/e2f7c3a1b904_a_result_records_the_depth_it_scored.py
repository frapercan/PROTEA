"""A result records the depth it scored

WHY. A depth cut left no trace on the result. The three reading surfaces
rendered depth from prediction_set.limit_per_entry, which is the RETRIEVAL
depth and is 30 for every cut of a k=30 set, so the five point depth series run
on 2026-08-30 appeared as five results at depth 30 with different numbers and
no visible reason for the difference.

The only witness was job.payload, reached through evaluation_result.job_id,
which is declared ON DELETE SET NULL. Deleting a job would have erased the one
field saying what its result measured, silently and with no foreign key error.

THE SEAL CANNOT SUBSTITUTE FOR THIS. Depth is a level, not a frame: two depths
of one retrieval belong under one digest and differ in their level. That is why
the five correct results and five results that had scored no cut at all shared
the digest f-1c245d41f26ff70c3b0a9247 and could not be told apart by it.

BACKFILLED FROM job.payload WHILE THE JOBS STILL EXIST. Every result whose job
still carries max_sequence_rank or max_k_position gets it copied here. Results
whose job is already gone keep NULL, which is honest: nothing in the record can
say what they measured, and inventing 30 for them would assert the very thing
this migration exists to stop asserting.

Revision ID: e2f7c3a1b904
Revises: c7e4a91b3d08
"""

import sqlalchemy as sa

from alembic import op

revision = "e2f7c3a1b904"
down_revision = "c7e4a91b3d08"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "evaluation_result",
        sa.Column("max_sequence_rank", sa.Integer(), nullable=True),
    )
    op.add_column(
        "evaluation_result",
        sa.Column("max_k_position", sa.Integer(), nullable=True),
    )
    # Backfill from the payload of the job that produced each result. The cast
    # is guarded: a payload key that is present but not a number would abort
    # the whole migration, and one bad row is not a reason to lose the rest.
    op.execute(
        """
        UPDATE evaluation_result er
           SET max_sequence_rank =
                 CASE WHEN j.payload ->> 'max_sequence_rank' ~ '^[0-9]+$'
                      THEN (j.payload ->> 'max_sequence_rank')::int END,
               max_k_position =
                 CASE WHEN j.payload ->> 'max_k_position' ~ '^[0-9]+$'
                      THEN (j.payload ->> 'max_k_position')::int END
          FROM job j
         WHERE j.id = er.job_id
        """
    )


def downgrade() -> None:
    op.drop_column("evaluation_result", "max_k_position")
    op.drop_column("evaluation_result", "max_sequence_rank")
