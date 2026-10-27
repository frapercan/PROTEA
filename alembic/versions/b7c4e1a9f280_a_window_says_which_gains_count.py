"""A window says which gains count

``truth_profile`` said how the t1 side's truth is BUILT. This says which part
of it counts as a gain:

* ``pivot_terms``, what every existing row did: the gain is whatever the pivot
  can name, and the restriction to the terms of interest happens later, when
  cafaeval is told what to score;
* ``live_in_both_ends``, what the lab's frame does: the gain is restricted
  BEFORE the delta is classified, so a protein whose only gain is a term
  obsolete by t1 is not a target at all.

Measured on the 211 -> 227 validation window: one protein, ``Q9FNK4``, whose
four gains were alive in 2022 and are obsolete in 2025. It does not move the
figure, because cafaeval discards a protein with no truth inside the terms of
interest either way. It moves the published POPULATION, which is the number a
paper quotes.

IN THE UNIQUE KEY, AND SEPARATE FROM ``truth_profile``. In the key because it
changes the delta, like the profile and the two graphs before it. Separate
because the two answer different questions -- the projection decides what a
gain CAN be, this decides which gains COUNT -- and there are four
combinations, not two. One field holding both would be the single-field
comparison this campaign has already been bitten by twice.

NOT NULL with a server default, for the same reason as ``truth_profile``: a
NULL is distinct from every other NULL inside a unique constraint, so two
windows that declared nothing would both be accepted and the key would stop
being one.

Catalog-only on a table of 129 rows.

Revision ID: b7c4e1a9f280
Revises: a1d7e3b95c62
Create Date: 2026-10-27 14:25:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b7c4e1a9f280"
down_revision: str | Sequence[str] | None = "a1d7e3b95c62"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_IDENTITY = (
    "old_annotation_set_id",
    "new_annotation_set_id",
    "pivot_snapshot_id",
    "old_native_snapshot_id",
    "new_native_snapshot_id",
    "truth_profile",
)


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "evaluation_set",
        sa.Column(
            "toi_rule",
            sa.String(length=24),
            nullable=False,
            server_default="pivot_terms",
        ),
    )
    op.create_check_constraint(
        "ck_evaluation_set_toi_rule",
        "evaluation_set",
        "toi_rule IN ('pivot_terms', 'live_in_both_ends')",
    )
    op.drop_constraint("uq_evaluation_set_identity", "evaluation_set", type_="unique")
    op.create_unique_constraint(
        "uq_evaluation_set_identity",
        "evaluation_set",
        [*_IDENTITY, "toi_rule"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    restricted = bind.execute(
        sa.text("select count(*) from evaluation_set where toi_rule <> 'pivot_terms'")
    ).scalar_one()
    if restricted:
        raise RuntimeError(
            f"{restricted} evaluation_set rows restricted their gain to the terms of "
            "interest; dropping the column would make them indistinguishable from the "
            "unrestricted ones. Delete those windows first."
        )
    op.drop_constraint("uq_evaluation_set_identity", "evaluation_set", type_="unique")
    op.create_unique_constraint("uq_evaluation_set_identity", "evaluation_set", list(_IDENTITY))
    op.drop_constraint("ck_evaluation_set_toi_rule", "evaluation_set", type_="check")
    op.drop_column("evaluation_set", "toi_rule")
