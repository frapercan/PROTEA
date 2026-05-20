"""farm_exp_12_rename_lineage_dataset_to_prostt5

Revision ID: d8e2f4b6a3c1
Revises: cf16788285f4
Create Date: 2026-05-20 09:00:00.000000

FARM-EXP.12 (ADR D36): the legacy ``bench-v1-K5-v226-lineage``
``Dataset`` row was built against ProstT5 embeddings
(``embedding_config_id=c0ae5b69-d6dc-41cf-a711-1739d3d2e170``) but the
PLM choice was not encoded in the dataset name. With the multi-PLM
v226 sweep (FARM-EXP.13) about to land seven sibling builds, the
implicit PLM axis must become explicit at dataset-name level.

This migration is data-only: ``Dataset.name`` is renamed from
``bench-v1-K5-v226-lineage`` to ``bench-v1-K5-v226-lineage-prostt5``
in a single atomic ``UPDATE`` (the ``name`` column carries a UNIQUE
constraint, so the swap goes through without a temporary value). The
legacy name is recorded in the row's ``meta`` JSONB under
``alias_names`` so any caller still resolving by the old name has a
traceable fallback handle for one release cycle. The artifact-store
``key_prefix`` is intentionally left unchanged (no parquet movement
on disk or S3).

The upgrade is idempotent: a second ``alembic upgrade head`` finds no
row matching the legacy name and exits as a no-op. The downgrade
reverts the rename but leaves the ``alias_names`` history in
``meta`` so audit consumers can still see the migration trace.

Schema impact: none. No columns added or dropped.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d8e2f4b6a3c1"
down_revision: str | Sequence[str] | None = "cf16788285f4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

LEGACY_NAME = "bench-v1-K5-v226-lineage"
CANONICAL_NAME = "bench-v1-K5-v226-lineage-prostt5"


def upgrade() -> None:
    """Rename the legacy lineage Dataset row + record the alias in meta."""
    bind = op.get_bind()
    # If a prior run (or a manual fix) already renamed the row, exit
    # cleanly. We never raise on missing row; the rename can be a
    # no-op when the legacy bench was wiped or never created (e.g. a
    # fresh staging DB).
    legacy_row = bind.execute(
        sa.text(
            "SELECT id, meta FROM dataset WHERE name = :legacy"
        ),
        {"legacy": LEGACY_NAME},
    ).fetchone()
    if legacy_row is None:
        return

    # Refuse to clobber an existing canonical row. If both names exist
    # we stop and let a human resolve the conflict.
    canonical_exists = bind.execute(
        sa.text("SELECT 1 FROM dataset WHERE name = :canonical"),
        {"canonical": CANONICAL_NAME},
    ).fetchone()
    if canonical_exists is not None:
        raise RuntimeError(
            f"Both {LEGACY_NAME!r} and {CANONICAL_NAME!r} exist in dataset; "
            "manual resolution required before this migration can run."
        )

    # Append the legacy name to meta.alias_names (JSONB list).
    bind.execute(
        sa.text(
            """
            UPDATE dataset
            SET meta = COALESCE(meta, '{}'::jsonb)
                   || jsonb_build_object(
                          'alias_names',
                          COALESCE(meta->'alias_names', '[]'::jsonb)
                              || to_jsonb(:legacy::text)
                      )
            WHERE name = :legacy
              AND NOT (
                  COALESCE(meta->'alias_names', '[]'::jsonb) @> to_jsonb(:legacy::text)
              )
            """
        ),
        {"legacy": LEGACY_NAME},
    )

    # Atomic rename.
    bind.execute(
        sa.text("UPDATE dataset SET name = :canonical WHERE name = :legacy"),
        {"canonical": CANONICAL_NAME, "legacy": LEGACY_NAME},
    )


def downgrade() -> None:
    """Revert the rename. The alias_names trace stays in meta."""
    bind = op.get_bind()
    bind.execute(
        sa.text("UPDATE dataset SET name = :legacy WHERE name = :canonical"),
        {"legacy": LEGACY_NAME, "canonical": CANONICAL_NAME},
    )
