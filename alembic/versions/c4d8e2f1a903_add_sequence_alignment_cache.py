"""add sequence_alignment cache

An alignment is a function of two sequences and nothing else, so it is worth
storing once and reusing across every run that meets the same pair. Measured on
the rung-1 grid: within a model, K=3's pairs are a strict subset of K=30's
(1,239 of 1,239); across models, 1,063 of 1,216 pairs recurred. Alignments are
63% of a batch's wall time.

Keyed by sequence_hash, never by accession: an accession can point at a new
sequence after a UniProt release, and the cache would then answer with an
alignment of something else.

Revision ID: c4d8e2f1a903
Revises: f2b8d1c6a94e
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c4d8e2f1a903"
down_revision: str | Sequence[str] | None = "f2b8d1c6a94e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_METRICS = (
    "identity_nw",
    "similarity_nw",
    "alignment_score_nw",
    "gaps_pct_nw",
    "alignment_length_nw",
    "identity_sw",
    "similarity_sw",
    "alignment_score_sw",
    "gaps_pct_sw",
    "alignment_length_sw",
    "length_query",
    "length_ref",
)


def upgrade() -> None:
    op.create_table(
        "sequence_alignment",
        sa.Column("query_hash", sa.String(length=32), nullable=False),
        sa.Column("ref_hash", sa.String(length=32), nullable=False),
        *[sa.Column(name, sa.Float(), nullable=False) for name in _METRICS],
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("query_hash", "ref_hash"),
    )


def downgrade() -> None:
    op.drop_table("sequence_alignment")
