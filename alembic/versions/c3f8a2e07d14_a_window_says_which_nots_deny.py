"""A window says which NOTs may deny

``truth_profile`` says how the t1 side's truth is BUILT and ``toi_rule`` which
part of it COUNTS. This says which NOT annotations may take it AWAY.

``any`` is what every existing row did, because the loader never asked: a NOT
row removed its term and that term's descendants whatever evidence the NOT
carried, so a ``NOT|IBA`` denied a truth whose positives are experimental
only. ``experimental`` restricts the negation to the thirteen experimental
codes, which is the lab's frame.

Measured on the 211 -> 227 validation window, everything else held fixed --
same remap, same projection, same two graphs:

===================  =======  ============  ======
counter              ``any``  experimental  lab
===================  =======  ============  ======
delta_proteins        40.534        40.531  40.531
nk_proteins           18.268        18.268  18.268
lk_proteins            4.669         4.667   4.667
pk_proteins           19.713        19.713  19.713
projected_entries      2.619         2.610   2.610
projected_pairs        3.830         3.821   3.821
projected_proteins     2.237         2.228   2.228
===================  =======  ============  ======

Three proteins and nine projected entries, and it is the difference between
reproducing the lab's truth exactly and missing it by three. 9.262 of the
17.178 NOT rows across the two ends carry non-experimental evidence, so the
effect is small not because the rows are few but because few of them land on
a delta protein.

IN THE UNIQUE KEY, and separate from the other two, for the third time and
the same reason: the three answer different questions -- what a gain CAN be,
which gains COUNT, which denials BIND -- so there are eight combinations and
one field holding them would be the single-field comparison this campaign has
been bitten by three times.

NOT NULL with a server default of ``any``, so no stored window moves: a NULL
is distinct from every other NULL inside a unique constraint, and two windows
that declared nothing would both be accepted.

Catalog-only on a table of 129 rows.

Revises the alias migration and not the gain one although it needs neither:
both hung off ``b7c4e1a9f280`` while this was being written, and two heads
make ``upgrade head`` ambiguous on a deploy that is meant to be one command.

Revision ID: c3f8a2e07d14
Revises: c8e2b4d7a106
Create Date: 2026-10-27 18:40:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c3f8a2e07d14"
down_revision: str | Sequence[str] | None = "c8e2b4d7a106"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_IDENTITY = (
    "old_annotation_set_id",
    "new_annotation_set_id",
    "pivot_snapshot_id",
    "old_native_snapshot_id",
    "new_native_snapshot_id",
    "truth_profile",
    "toi_rule",
)


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "evaluation_set",
        sa.Column(
            "not_codes",
            sa.String(length=16),
            nullable=False,
            server_default="any",
        ),
    )
    op.create_check_constraint(
        "ck_evaluation_set_not_codes",
        "evaluation_set",
        "not_codes IN ('any', 'experimental')",
    )
    op.drop_constraint("uq_evaluation_set_identity", "evaluation_set", type_="unique")
    op.create_unique_constraint(
        "uq_evaluation_set_identity",
        "evaluation_set",
        [*_IDENTITY, "not_codes"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    restricted = bind.execute(
        sa.text("select count(*) from evaluation_set where not_codes <> 'any'")
    ).scalar_one()
    if restricted:
        raise RuntimeError(
            f"{restricted} evaluation_set rows restricted their NOT side to the "
            "experimental codes; dropping the column would make them "
            "indistinguishable from the unrestricted ones. Delete those windows first."
        )
    op.drop_constraint("uq_evaluation_set_identity", "evaluation_set", type_="unique")
    op.create_unique_constraint("uq_evaluation_set_identity", "evaluation_set", list(_IDENTITY))
    op.drop_constraint("ck_evaluation_set_not_codes", "evaluation_set", type_="check")
    op.drop_column("evaluation_set", "not_codes")
