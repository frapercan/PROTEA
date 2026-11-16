"""A window says which proteins it scores

The three rule columns decide which TERMS a window's truth holds. This one
decides which PROTEINS are its targets.

``every_protein`` is what every existing row did, because no window ever
asked: any protein that gained an experimental annotation between the two
ends was a target. ``superset_at_t0`` keeps the superset of the photo of the
window's start (ADR-D59), the Swiss-Prot list of the UniProt release paired
with that cut, fixed before the window opens as CAFA fixes its targets. The
author chose it for the four windows of the frame on 2026-11-16 (ADR-D60).

IN THE UNIQUE KEY, because it changes the delta: a window over every protein
and the same window over the superset are two measurements and must coexist.

NOT NULL with a server default of ``every_protein``, so no stored window
moves: a NULL is distinct from every other NULL inside a unique constraint.

Revision ID: e5b1c9d3a7f2
Revises: a6e1e05d3c8b
Create Date: 2026-11-16 21:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e5b1c9d3a7f2"
down_revision: str | Sequence[str] | None = "a6e1e05d3c8b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The model's check, by the same text; a test compares the two.
TARGET_POPULATION_CHECK = "target_population IN ('every_protein', 'superset_at_t0')"

_IDENTITY = (
    "old_annotation_set_id",
    "new_annotation_set_id",
    "pivot_snapshot_id",
    "old_native_snapshot_id",
    "new_native_snapshot_id",
    "truth_profile",
    "toi_rule",
    "not_codes",
)


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "evaluation_set",
        sa.Column(
            "target_population",
            sa.String(length=24),
            nullable=False,
            server_default="every_protein",
        ),
    )
    op.create_check_constraint(
        "ck_evaluation_set_target_population", "evaluation_set", TARGET_POPULATION_CHECK
    )
    op.drop_constraint("uq_evaluation_set_identity", "evaluation_set", type_="unique")
    op.create_unique_constraint(
        "uq_evaluation_set_identity", "evaluation_set", [*_IDENTITY, "target_population"]
    )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    restricted = bind.execute(
        sa.text("select count(*) from evaluation_set where target_population <> 'every_protein'")
    ).scalar_one()
    if restricted:
        raise RuntimeError(
            f"{restricted} evaluation_set rows restricted their targets to a population; "
            "dropping the column would make them indistinguishable from windows over "
            "every protein. Delete those windows first."
        )
    op.drop_constraint("uq_evaluation_set_identity", "evaluation_set", type_="unique")
    op.create_unique_constraint("uq_evaluation_set_identity", "evaluation_set", list(_IDENTITY))
    op.drop_constraint("ck_evaluation_set_target_population", "evaluation_set", type_="check")
    op.drop_column("evaluation_set", "target_population")
