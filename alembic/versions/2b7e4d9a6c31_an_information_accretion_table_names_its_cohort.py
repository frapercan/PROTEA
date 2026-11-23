"""An information accretion table names its cohort

An IA table was identified by three axes: the ontology snapshot, the corpus and
the evidence regime. LAFA computes its information accretion over one more
choice, which proteins of the corpus count: only the Swiss-Prot list of the
release paired with the corpus. The fourth axis, ``cohort``, says it:
``every_protein`` on every stored row, which is what each one did, and
``swissprot`` for LAFA's. It joins the unique key, so the two tables over the
same corpus can coexist.

The downgrade refuses while a row names another cohort: dropping the column
would make it indistinguishable from the table over every protein, and the old
key would collide.

Revision ID: 2b7e4d9a6c31
Revises: 5d3a8c1f7b24
Create Date: 2026-11-20 20:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2b7e4d9a6c31"
down_revision: str | Sequence[str] | None = "5d3a8c1f7b24"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The model's check, by the same text; a test compares them.
COHORT_CHECK = "cohort IN ('every_protein', 'swissprot')"

_THREE = ["ontology_snapshot_id", "annotation_set_id", "evidence_regime"]


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "information_accretion_set",
        sa.Column("cohort", sa.String(length=24), nullable=False, server_default="every_protein"),
    )
    op.create_check_constraint("ck_ia_set_cohort", "information_accretion_set", COHORT_CHECK)
    op.drop_constraint(
        "uq_ia_set_snapshot_corpus_regime", "information_accretion_set", type_="unique"
    )
    op.create_unique_constraint(
        "uq_ia_set_snapshot_corpus_regime_cohort",
        "information_accretion_set",
        [*_THREE, "cohort"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    rows = (
        op.get_bind()
        .execute(
            sa.text(
                "select count(*) from information_accretion_set where cohort <> 'every_protein'"
            )
        )
        .scalar_one()
    )
    if rows:
        raise RuntimeError(
            f"{rows} information_accretion_set rows count another cohort than every "
            "protein; dropping the column would make them look like the table over "
            "every protein. Delete those tables first."
        )
    op.drop_constraint(
        "uq_ia_set_snapshot_corpus_regime_cohort", "information_accretion_set", type_="unique"
    )
    op.create_unique_constraint(
        "uq_ia_set_snapshot_corpus_regime", "information_accretion_set", _THREE
    )
    op.drop_constraint("ck_ia_set_cohort", "information_accretion_set", type_="check")
    op.drop_column("information_accretion_set", "cohort")
