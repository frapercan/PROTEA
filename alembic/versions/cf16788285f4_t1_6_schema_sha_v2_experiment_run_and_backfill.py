"""t1_6_schema_sha_v2_experiment_run_and_backfill

Revision ID: cf16788285f4
Revises: ccc0494d22f5
Create Date: 2026-05-17 09:00:00.000000

T1.6 follow-up of master plan v3 (ADR D10). The first slice
(``a3c1d8e2f4b6_t1_6_add_schema_sha_v2_columns``) added the parallel
``schema_sha_v2`` column to ``dataset`` and ``reranker_model`` but left
``experiment_run`` out of scope (the axis-tuple migration
``e1c4a7b2d8f3`` had not yet shipped). This revision finishes the
scaffolding so every row that fingerprints a feature schema can carry
the canonical ``protea_contracts.compute_schema_sha`` digest alongside
the legacy value.

Changes
-------

* ``experiment_run.schema_sha_v2 TEXT NULL`` plus an index
  ``ix_experiment_run_schema_sha_v2`` for cell-catalog lookups.
* Idempotent backfill ``UPDATE`` statements that copy ``schema_sha``
  (or ``feature_schema_sha`` on ``reranker_model`` / ``experiment_run``)
  into the new column when it is still NULL. The values are byte-equal
  to v1 today; the canonical helper produces the same digest. A future
  slice may rewrite the helper, at which point the dedicated backfill
  script ``scripts/backfill_schema_sha_v2.py`` is the right place to
  recompute the digests.
* Downgrade drops the index and column from ``experiment_run``. The
  backfill writes are NOT rolled back: ``schema_sha_v2`` on ``dataset``
  and ``reranker_model`` survives the downgrade because the columns
  themselves were added by the prior revision. Operators that need a
  full revert chain through both revisions.

The migration is strictly additive and idempotent. The backfill
``UPDATE`` clauses scope to ``WHERE schema_sha_v2 IS NULL`` so a second
``alembic upgrade head`` is a no-op even if rows were touched by the
out-of-band ``scripts/backfill_schema_sha_v2.py``.

This revision ships **scaffolding only**. The live-DB
``alembic upgrade head`` against production is a separate
human-coordinated window.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "cf16788285f4"
down_revision: str | Sequence[str] | None = "ccc0494d22f5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add ``schema_sha_v2`` to ``experiment_run`` + backfill all three tables."""
    op.add_column(
        "experiment_run",
        sa.Column("schema_sha_v2", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_experiment_run_schema_sha_v2",
        "experiment_run",
        ["schema_sha_v2"],
    )

    # Idempotent backfill. v2 == v1 string-wise today; if a later slice
    # ever changes the digest formula, the production rollout uses
    # ``scripts/backfill_schema_sha_v2.py`` (which can recompute) rather
    # than this migration.
    op.execute(
        sa.text(
            "UPDATE dataset SET schema_sha_v2 = schema_sha "
            "WHERE schema_sha_v2 IS NULL AND schema_sha IS NOT NULL"
        )
    )
    op.execute(
        sa.text(
            "UPDATE reranker_model SET schema_sha_v2 = feature_schema_sha "
            "WHERE schema_sha_v2 IS NULL AND feature_schema_sha IS NOT NULL"
        )
    )
    op.execute(
        sa.text(
            "UPDATE experiment_run SET schema_sha_v2 = feature_schema_sha "
            "WHERE schema_sha_v2 IS NULL AND feature_schema_sha IS NOT NULL"
        )
    )


def downgrade() -> None:
    """Drop the new column + index from ``experiment_run``.

    The dataset / reranker_model backfill is intentionally left in
    place: those columns belong to the prior revision
    ``a3c1d8e2f4b6_t1_6_add_schema_sha_v2_columns`` and dropping their
    values here would create a hole the operator likely does not want.
    Roll all the way back through ``a3c1d8e2f4b6`` for a clean slate.
    """
    op.drop_index(
        "ix_experiment_run_schema_sha_v2",
        table_name="experiment_run",
    )
    op.drop_column("experiment_run", "schema_sha_v2")
