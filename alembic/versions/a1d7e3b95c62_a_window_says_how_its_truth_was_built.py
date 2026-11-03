"""A window says how its truth was built

``evaluation_set`` recorded the five ids a delta is computed from and nothing
about how. That was fine while there was one way. There are now two:

* ``as_closed``, what every existing row did. A t1 term the pivot cannot name
  is dropped by the ``closure & pivot_go_ids`` intersection, and the gain
  disappears with it.
* ``projected``, the lab's rule. Such a term is replaced by its first live
  ancestors at t0, same aspect, before the binding rule and the closure.

On the 211 -> 227 validation window the two differ by 646 proteins and 2,610
entries, which is the whole of the remaining difference between PROTEA's truth
and the lab's.

WHY IT GOES IN THE UNIQUE KEY. The identity constraint already holds the five
ids, for the reason its own comment gives: the same pair of corpora under a
different propagation graph is a different measurement. A different TRUTH is
more than that. And without the column in the key the two could not coexist
at all: the five ids are identical, so the projected window would be refused
as a duplicate of the closed one.

NOT NULL with a server default rather than nullable, because in Postgres a
NULL is distinct from every other NULL inside a unique constraint: two
windows that declared no profile would both be accepted and the key would
stop being one. Every existing row gets ``as_closed``, which is what it did.

Both changes are catalog-only on a three-row table.

Revision ID: a1d7e3b95c62
Revises: c5f1a9d3e8b7
Create Date: 2026-10-27 13:40:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1d7e3b95c62"
down_revision: str | Sequence[str] | None = "c5f1a9d3e8b7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_IDENTITY = (
    "old_annotation_set_id",
    "new_annotation_set_id",
    "pivot_snapshot_id",
    "old_native_snapshot_id",
    "new_native_snapshot_id",
)


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "evaluation_set",
        sa.Column(
            "truth_profile",
            sa.String(length=16),
            nullable=False,
            server_default="as_closed",
        ),
    )
    op.create_check_constraint(
        "ck_evaluation_set_truth_profile",
        "evaluation_set",
        "truth_profile IN ('as_closed', 'projected')",
    )
    op.drop_constraint("uq_evaluation_set_identity", "evaluation_set", type_="unique")
    op.create_unique_constraint(
        "uq_evaluation_set_identity",
        "evaluation_set",
        [*_IDENTITY, "truth_profile"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    # Refuse rather than collapse two truths into one. Dropping the column
    # would make a projected window indistinguishable from the closed window
    # over the same five ids, and the narrower constraint would then refuse
    # to be created at all.
    bind = op.get_bind()
    projected = bind.execute(
        sa.text("select count(*) from evaluation_set where truth_profile <> 'as_closed'")
    ).scalar_one()
    if projected:
        raise RuntimeError(
            f"{projected} evaluation_set rows were built under a non-default truth "
            "profile; dropping the column would make them indistinguishable from "
            "the default ones. Delete those windows first."
        )
    op.drop_constraint("uq_evaluation_set_identity", "evaluation_set", type_="unique")
    op.create_unique_constraint("uq_evaluation_set_identity", "evaluation_set", list(_IDENTITY))
    op.drop_constraint("ck_evaluation_set_truth_profile", "evaluation_set", type_="check")
    op.drop_column("evaluation_set", "truth_profile")
