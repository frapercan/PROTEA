"""t3.1_goprediction_predictions_jsonb

Revision ID: ccc0494d22f5
Revises: e1c4a7b2d8f3
Create Date: 2026-05-17 09:00:00.000000

T3.1 of the executor plan (phase 4). Adds a nullable JSONB
``predictions_jsonb`` column to ``go_prediction`` plus a GIN index so
the predict / store pipeline can dual-write the prediction tuple
(``go_term_id``, score, evidence code) into a single compact blob.

This is the prediction-tuple sibling of the T3.1a ``features`` JSONB
column already on the table (mirrors per-row feature signals). The
two blobs answer different questions: ``features`` is the input to the
reranker; ``predictions_jsonb`` is the per-row prediction identity
plus its score, useful for compact downstream consumers that do not
need the full feature vector.

T3.1 ships the column + index only. T3.2 will backfill historical
rows; T3.3 cuts read paths over; T3.4 retires the typed prediction
columns. Live DB migration is a separate human-coordinated window
(see PR body).

The column is nullable and the GIN index is unconditional. Old rows
predate the dual-write and stay ``NULL`` (GIN indexes skip NULL
values by default, so the index cost is bounded by the dual-write
population).

SQL preview (for reviewer):

    ALTER TABLE go_prediction ADD COLUMN predictions_jsonb jsonb;
    CREATE INDEX ix_go_prediction_jsonb_gin
        ON go_prediction USING GIN (predictions_jsonb);

Strictly additive. ``ADD COLUMN ... jsonb NULL`` is metadata-only on
Postgres (no table rewrite). The GIN ``CREATE INDEX`` does scan the
table, but at insert time the column is NULL so the build is
effectively a no-op until backfill / dual-write populates rows.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ccc0494d22f5"
down_revision: str | Sequence[str] | None = "e1c4a7b2d8f3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add ``predictions_jsonb`` column + GIN index to ``go_prediction``.

    Both are nullable / unconditional. No backfill, no defaults; the
    dual-write writer site (gated by
    ``PROTEA_GO_PREDICTION_JSONB_WRITE_ENABLED``) populates new rows,
    and a future slice handles the historical backfill.
    """
    op.add_column(
        "go_prediction",
        sa.Column("predictions_jsonb", JSONB(), nullable=True),
    )
    op.create_index(
        "ix_go_prediction_jsonb_gin",
        "go_prediction",
        ["predictions_jsonb"],
        postgresql_using="gin",
    )


def downgrade() -> None:
    """Drop the GIN index then the column (reverse of :func:`upgrade`)."""
    op.drop_index("ix_go_prediction_jsonb_gin", table_name="go_prediction")
    op.drop_column("go_prediction", "predictions_jsonb")
