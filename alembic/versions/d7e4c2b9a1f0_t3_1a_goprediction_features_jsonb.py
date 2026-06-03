"""t3.1a_goprediction_features_jsonb

Revision ID: d7e4c2b9a1f0
Revises: 1e4aee32e677
Create Date: 2026-05-11 22:00:00.000000

T3.1a of the executor plan (F4). Adds a nullable JSONB ``features``
column to ``go_prediction`` so the predict / store pipeline can
dual-write every typed feature value into a single blob. Old typed
columns stay (readers still depend on them); the cut-over lands in
T3.1b. Backfill of pre-existing rows is intentionally out of scope:
the column is nullable so legacy rows simply read as ``NULL`` and
existing readers (still on the typed columns) are unaffected.

Strictly additive. Safe to apply on a live DB without a maintenance
window: ``ADD COLUMN ... jsonb NULL`` is metadata-only on Postgres
(no table rewrite, no row scan).

SQL preview (for reviewer):
    ALTER TABLE go_prediction ADD COLUMN features jsonb;
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d7e4c2b9a1f0"
down_revision: str | Sequence[str] | None = "1e4aee32e677"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "go_prediction",
        sa.Column("features", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("go_prediction", "features")
