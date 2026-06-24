"""Add snapshot-invariant go_id string columns to the co-occurrence tables.

Revision ID: d1e2f3a4b5c6
Revises: c5d7e9f1a3b8
Create Date: 2026-06-17

Motivation
----------
GO term integer ids in PROTEA are per ``ontology_snapshot_id`` (each
``(go_id, snapshot)`` is a distinct ``GOTerm`` row with a distinct int id).
The original ``term_cooccurrence`` / ``term_frequency`` tables keyed everything
on those per-snapshot integer ids. That broke the association feature across
snapshots: the offline build keys the cooccurrence on the annotation set's OWN
snapshot int ids, but the training export candidates live in the export's
snapshot, so ``candidate_term_id`` (build snapshot) never matched the export
candidate int ids and every association feature was zero for training rows.

Fix: carry the snapshot-INVARIANT ``go_id`` STRING alongside each integer id so
the loader and scorer can match on strings regardless of which snapshot the t0
set and the candidate live in. ``known_go_id`` / ``candidate_go_id`` are added
to ``term_cooccurrence`` and ``go_id`` to ``term_frequency``. The integer FK
columns are kept (nullable) so existing rows stay readable and FK integrity to
``go_term`` is preserved; new builds populate both. The lookup indexes move to
the string columns since matching is now string-keyed.

Backward read: pre-existing rows keep their integer ids; the new string columns
are NULL until a fresh ``build_go_cooccurrence`` run repopulates the set, which
the predict path requires anyway (string-keyed scoring ignores NULL go_id rows).
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "d1e2f3a4b5c6"
down_revision = "c5d7e9f1a3b8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # term_cooccurrence: add the snapshot-invariant string sides.
    op.add_column(
        "term_cooccurrence",
        sa.Column("known_go_id", sa.String(length=15), nullable=True),
    )
    op.add_column(
        "term_cooccurrence",
        sa.Column("candidate_go_id", sa.String(length=15), nullable=True),
    )
    op.create_index(
        "ix_term_cooccurrence_set_known_go",
        "term_cooccurrence",
        ["annotation_set_id", "known_go_id"],
    )

    # term_frequency: add the snapshot-invariant string key.
    op.add_column(
        "term_frequency",
        sa.Column("go_id", sa.String(length=15), nullable=True),
    )
    op.create_index(
        "ix_term_frequency_set_go",
        "term_frequency",
        ["annotation_set_id", "go_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_term_frequency_set_go", table_name="term_frequency")
    op.drop_column("term_frequency", "go_id")
    op.drop_index("ix_term_cooccurrence_set_known_go", table_name="term_cooccurrence")
    op.drop_column("term_cooccurrence", "candidate_go_id")
    op.drop_column("term_cooccurrence", "known_go_id")
