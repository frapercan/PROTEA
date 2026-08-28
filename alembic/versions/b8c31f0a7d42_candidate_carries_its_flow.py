"""a candidate carries the flow that proposed it

Every column on go_prediction describes ONE flow: ref_protein_accession names a
donor and distance names what found it, which is a complete description of
neighbour transfer and no description of anything else. A candidate proposed by
the classifier has no donor, and so does a candidate nobody proposed. Today
those are the same row, which makes unique reach uncomputable and the affinity
map unmeasurable.

A BITMASK, BECAUSE PROVENANCE IS A SET. Unique reach is "proposed by this flow
and by no other", so collapsing to a single proposer destroys the quantity. One
smallint over 128,191,567 rows is roughly 256 MB and needs no join; a junction
table would need one row per (candidate, flow) and an index to be usable.

BACKFILLED TO NEIGHBOUR_TRANSFER, NOT TO ZERO. Zero must keep meaning "not
recorded", because the column exists precisely so that "no flow proposed this"
becomes sayable, and no historical row is evidence for it. Every existing row
was produced by neighbour transfer by construction, so the backfill is a fact
rather than a default.

NOT NULL WITHOUT A DEFAULT ON THE COLUMN. A default would let a future producer
write a candidate without declaring its flow and have the row look complete.
The producer must say.

Revision ID: b8c31f0a7d42
Revises: d7f21a9c4e08
"""

import sqlalchemy as sa

from alembic import op

revision = "b8c31f0a7d42"
down_revision = "d7f21a9c4e08"
branch_labels = None
depends_on = None

#: protea.core.flows.Flow.NEIGHBOUR_TRANSFER. Duplicated as a literal on
#: purpose: a migration must not import application code, whose meaning can
#: change under a schema that has already run.
NEIGHBOUR_TRANSFER = 1


def upgrade() -> None:
    # Added nullable so the table is not rewritten under a lock, then
    # backfilled, then constrained. On 128 million rows the three-step form is
    # the difference between a brief catalogue change and a full rewrite that
    # holds ACCESS EXCLUSIVE for the duration.
    op.add_column(
        "go_prediction",
        sa.Column(
            "flow_mask",
            sa.SmallInteger(),
            nullable=True,
            comment=(
                "Bitmask of the flows that proposed this candidate "
                "(protea.core.flows.Flow). A set, not a value: unique reach "
                "counts rows whose mask is exactly one flow. Zero means the "
                "provenance was not recorded, which is not the same as no "
                "flow proposing it."
            ),
        ),
    )
    op.execute(f"UPDATE go_prediction SET flow_mask = {NEIGHBOUR_TRANSFER} WHERE flow_mask IS NULL")
    op.alter_column("go_prediction", "flow_mask", nullable=False)

    # Partial index on the single-flow masks, which is what unique reach reads.
    # Full index would be 128 million entries over eight distinct values and
    # would never be chosen.
    op.create_index(
        "ix_go_prediction_flow_mask_single",
        "go_prediction",
        ["prediction_set_id", "flow_mask"],
        postgresql_where=sa.text("flow_mask IN (1,2,4,8,16,32,64,128)"),
    )


def downgrade() -> None:
    op.drop_index("ix_go_prediction_flow_mask_single", table_name="go_prediction")
    op.drop_column("go_prediction", "flow_mask")
