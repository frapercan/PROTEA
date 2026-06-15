"""Add term_cooccurrence + term_frequency tables (lafa-integrate INT-3).

Revision ID: c5d7e9f1a3b8
Revises: f3a1c2d4e5b6
Create Date: 2026-06-15

Motivation
----------
The cross-aspect association feature needs a per-annotation-set co-occurrence
table. ``term_cooccurrence`` stores, for each ``(known_term, candidate_term)``
pair, the number of distinct proteins carrying both among their propagated
EXPERIMENTAL annotations; ``term_frequency`` stores the per-term distinct
protein count used as the denominator of ``P(t | k) = cooccurrence / freq(k)``.

Both tables are populated offline by the ``build_go_cooccurrence`` operation
and read at predict time behind the ``compute_association`` flag. They carry
no prediction-path writes, so the change is additive and fully reversible.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "c5d7e9f1a3b8"
down_revision = "f3a1c2d4e5b6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "term_cooccurrence",
        sa.Column("annotation_set_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("known_term_id", sa.BigInteger(), nullable=False),
        sa.Column("candidate_term_id", sa.BigInteger(), nullable=False),
        sa.Column("cooccurrence_count", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["annotation_set_id"], ["annotation_set.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["known_term_id"], ["go_term.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["candidate_term_id"], ["go_term.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("annotation_set_id", "known_term_id", "candidate_term_id"),
    )
    op.create_index(
        "ix_term_cooccurrence_set_known",
        "term_cooccurrence",
        ["annotation_set_id", "known_term_id"],
    )
    op.create_table(
        "term_frequency",
        sa.Column("annotation_set_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("term_id", sa.BigInteger(), nullable=False),
        sa.Column("freq", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["annotation_set_id"], ["annotation_set.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["term_id"], ["go_term.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("annotation_set_id", "term_id"),
    )


def downgrade() -> None:
    op.drop_table("term_frequency")
    op.drop_index("ix_term_cooccurrence_set_known", table_name="term_cooccurrence")
    op.drop_table("term_cooccurrence")
