"""A window can score a declared sample of its targets

A window restricted to a declared sample of its targets (a ``window_sample``
query set) is its parent's window over fewer proteins: the same two corpora,
graphs and rules, with the parent's verified truth kept to the sample. The
column ``evaluation_set.target_query_set_id`` names that sample (``RESTRICT``:
a sample a window scores is not one to delete under it), NULL for a window over
its whole population.

The column joins the window's identity: ``uq_evaluation_set_identity`` is made
again over the ten columns it held and this one, under its old name and with
``NULLS NOT DISTINCT``, so a NULL sample is one value, "the whole population":
two unrestricted windows still collide, a restricted window does not collide
with its parent, and two restricted to one sample do. The ten columns before it
are NOT NULL, so no stored pair of windows collides under the new constraint
that did not collide before.

The downgrade refuses while a restricted window exists: without the column it
would be its parent's identity twice.

Revision ID: 532d2c1db500
Revises: a0cc2a412ec0
Create Date: 2026-11-28 20:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "532d2c1db500"
down_revision: str | Sequence[str] | None = "a0cc2a412ec0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The ten columns the identity held before this revision, in their order.
PREVIOUS_IDENTITY = (
    "old_annotation_set_id",
    "new_annotation_set_id",
    "pivot_snapshot_id",
    "old_native_snapshot_id",
    "new_native_snapshot_id",
    "truth_profile",
    "toi_rule",
    "not_codes",
    "target_population",
    "binding_rule",
)
IDENTITY = (*PREVIOUS_IDENTITY, "target_query_set_id")


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "evaluation_set",
        sa.Column("target_query_set_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_evaluation_set_target_query_set_id",
        "evaluation_set",
        "query_set",
        ["target_query_set_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.drop_constraint("uq_evaluation_set_identity", "evaluation_set", type_="unique")
    op.create_unique_constraint(
        "uq_evaluation_set_identity",
        "evaluation_set",
        list(IDENTITY),
        postgresql_nulls_not_distinct=True,
    )


def downgrade() -> None:
    """Downgrade schema."""
    restricted = (
        op.get_bind()
        .execute(
            sa.text("SELECT count(*) FROM evaluation_set WHERE target_query_set_id IS NOT NULL")
        )
        .scalar_one()
    )
    if restricted:
        raise RuntimeError(
            f"{restricted} evaluation_set rows score a declared sample of their targets; "
            "without the column each would hold its parent's identity. Delete those windows "
            "first."
        )
    op.drop_constraint("uq_evaluation_set_identity", "evaluation_set", type_="unique")
    op.create_unique_constraint(
        "uq_evaluation_set_identity", "evaluation_set", list(PREVIOUS_IDENTITY)
    )
    op.drop_constraint(
        "fk_evaluation_set_target_query_set_id", "evaluation_set", type_="foreignkey"
    )
    op.drop_column("evaluation_set", "target_query_set_id")
