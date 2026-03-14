"""add feature engineering columns to go_prediction

Revision ID: a7b8c9d0e1f2
Revises: f1a2b3c4d5e6
Create Date: 2026-03-11 00:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "a7b8c9d0e1f2"
down_revision: Union[str, Sequence[str], None] = "f1a2b3c4d5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Alignment — Needleman–Wunsch (global)
    op.add_column("go_prediction", sa.Column("identity_nw", sa.Float(), nullable=True))
    op.add_column("go_prediction", sa.Column("similarity_nw", sa.Float(), nullable=True))
    op.add_column("go_prediction", sa.Column("alignment_score_nw", sa.Float(), nullable=True))
    op.add_column("go_prediction", sa.Column("gaps_pct_nw", sa.Float(), nullable=True))
    op.add_column("go_prediction", sa.Column("alignment_length_nw", sa.Float(), nullable=True))

    # Alignment — Smith–Waterman (local)
    op.add_column("go_prediction", sa.Column("identity_sw", sa.Float(), nullable=True))
    op.add_column("go_prediction", sa.Column("similarity_sw", sa.Float(), nullable=True))
    op.add_column("go_prediction", sa.Column("alignment_score_sw", sa.Float(), nullable=True))
    op.add_column("go_prediction", sa.Column("gaps_pct_sw", sa.Float(), nullable=True))
    op.add_column("go_prediction", sa.Column("alignment_length_sw", sa.Float(), nullable=True))

    # Sequence lengths
    op.add_column("go_prediction", sa.Column("length_query", sa.Integer(), nullable=True))
    op.add_column("go_prediction", sa.Column("length_ref", sa.Integer(), nullable=True))

    # Taxonomy
    op.add_column("go_prediction", sa.Column("query_taxonomy_id", sa.Integer(), nullable=True))
    op.add_column("go_prediction", sa.Column("ref_taxonomy_id", sa.Integer(), nullable=True))
    op.add_column("go_prediction", sa.Column("taxonomic_lca", sa.Integer(), nullable=True))
    op.add_column("go_prediction", sa.Column("taxonomic_distance", sa.Integer(), nullable=True))
    op.add_column("go_prediction", sa.Column("taxonomic_common_ancestors", sa.Integer(), nullable=True))
    op.add_column("go_prediction", sa.Column("taxonomic_relation", sa.String(length=20), nullable=True))


def downgrade() -> None:
    op.drop_column("go_prediction", "taxonomic_relation")
    op.drop_column("go_prediction", "taxonomic_common_ancestors")
    op.drop_column("go_prediction", "taxonomic_distance")
    op.drop_column("go_prediction", "taxonomic_lca")
    op.drop_column("go_prediction", "ref_taxonomy_id")
    op.drop_column("go_prediction", "query_taxonomy_id")
    op.drop_column("go_prediction", "length_ref")
    op.drop_column("go_prediction", "length_query")
    op.drop_column("go_prediction", "alignment_length_sw")
    op.drop_column("go_prediction", "gaps_pct_sw")
    op.drop_column("go_prediction", "alignment_score_sw")
    op.drop_column("go_prediction", "similarity_sw")
    op.drop_column("go_prediction", "identity_sw")
    op.drop_column("go_prediction", "alignment_length_nw")
    op.drop_column("go_prediction", "gaps_pct_nw")
    op.drop_column("go_prediction", "alignment_score_nw")
    op.drop_column("go_prediction", "similarity_nw")
    op.drop_column("go_prediction", "identity_nw")
