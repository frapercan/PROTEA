"""t1_6_add_schema_sha_v2_columns

Revision ID: a3c1d8e2f4b6
Revises: 9d2fb5a07e4c
Create Date: 2026-05-12 14:00:00.000000

T1.6 of master plan v3 (ADR D10). Adds a parallel ``schema_sha_v2``
column to ``dataset`` and ``reranker_model`` so legacy rows that
fingerprinted the feature schema with the PROTEA-side
``json.dumps(..., sort_keys=True)`` formula can coexist with rows
written by ``protea_contracts.compute_schema_sha`` (the canonical
``"|".join(sorted(columns))`` digest).

The column is nullable and unindexed-by-FK. A b-tree index supports
inference lookups (``WHERE schema_sha_v2 = :live_sha``). The forward
migration is strictly additive (two ``ADD COLUMN`` + two ``CREATE
INDEX`` statements). The downgrade drops the indexes then the columns
in reverse order so the migration is safe to roll back mid-deploy.

Backfill happens out-of-band via
``scripts/backfill_schema_sha_v2.py``; the runbook at
``docs/source/runbooks/schema-sha-v2-rollout.rst`` walks the operator
through pre-flight, apply, verify, and rollback.

Type choice
-----------

``schema_sha`` on ``dataset`` is ``String(16)``; the v2 column uses
``Text`` because future digest widenings (e.g. full SHA-256 instead of
the 12-hex truncation) must not require another type migration. The
current ``protea_contracts.compute_schema_sha`` returns 12 hex chars,
so production payloads will fit comfortably.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a3c1d8e2f4b6"
down_revision: str | Sequence[str] | None = "9d2fb5a07e4c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add ``schema_sha_v2`` to ``dataset`` and ``reranker_model``.

    Both columns are nullable and indexed. No FKs, no defaults; the
    backfill script writes them post-deploy.
    """
    op.add_column(
        "dataset",
        sa.Column("schema_sha_v2", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_dataset_schema_sha_v2",
        "dataset",
        ["schema_sha_v2"],
    )

    op.add_column(
        "reranker_model",
        sa.Column("schema_sha_v2", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_reranker_model_schema_sha_v2",
        "reranker_model",
        ["schema_sha_v2"],
    )


def downgrade() -> None:
    """Drop the indexes then the columns (reverse of :func:`upgrade`)."""
    op.drop_index("ix_reranker_model_schema_sha_v2", table_name="reranker_model")
    op.drop_column("reranker_model", "schema_sha_v2")

    op.drop_index("ix_dataset_schema_sha_v2", table_name="dataset")
    op.drop_column("dataset", "schema_sha_v2")
