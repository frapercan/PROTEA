"""add reranker v6 features to go_prediction

Adds 25 nullable Float columns used by the v6 reranker:

- 6 Anc2Vec semantic-coherence features (neighbor + query-known).
- 3 tax_voters consensus features (computed over the subset of neighbors that
  voted for each candidate term).
- 16 emb_pca_query_* features (per-query projection onto the precomputed
  principal components of the reference embedding pool).

All columns are nullable because older prediction_sets predate these features
and older reranker versions do not read them.

Revision ID: 7a2c9e1d0b33
Revises: 651358a5a2c8
Create Date: 2026-04-19 12:00:00.000000
"""
from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision: str = "7a2c9e1d0b33"
down_revision: str = "651358a5a2c8"
branch_labels = None
depends_on = None


_ANC2VEC_COLS = (
    "anc2vec_neighbor_cos",
    "anc2vec_neighbor_maxcos",
    "anc2vec_has_emb",
    "anc2vec_query_known_cos",
    "anc2vec_query_known_maxcos",
    "anc2vec_query_known_count",
)

_TAX_VOTERS_COLS = (
    "tax_voters_same_frac",
    "tax_voters_close_frac",
    "tax_voters_mean_common_ancestors",
)

_EMB_PCA_COLS = tuple(f"emb_pca_query_{i}" for i in range(16))


def upgrade() -> None:
    for col in (*_ANC2VEC_COLS, *_TAX_VOTERS_COLS, *_EMB_PCA_COLS):
        op.add_column("go_prediction", sa.Column(col, sa.Float(), nullable=True))


def downgrade() -> None:
    for col in reversed((*_ANC2VEC_COLS, *_TAX_VOTERS_COLS, *_EMB_PCA_COLS)):
        op.drop_column("go_prediction", col)
