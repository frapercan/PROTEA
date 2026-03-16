"""Add evidence_weights column to scoring_config.

Revision ID: c1d2e3f4a5b6
Revises: 7c19ca08d5d4
Create Date: 2026-03-16

Motivation
----------
``ScoringConfig`` previously hard-coded the per-evidence-code quality
weights inside the Python scoring engine, making them invisible to users
and impossible to customise without a code change.

This migration adds an optional ``evidence_weights`` JSONB column that
stores per-code overrides at the config level.  Existing rows receive
``NULL``, which is interpreted by the engine as "use system defaults"
(:data:`protea.infrastructure.orm.models.embedding.scoring_config.DEFAULT_EVIDENCE_WEIGHTS`).
The change is therefore fully backwards-compatible with all existing
``ScoringConfig`` rows.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "c1d2e3f4a5b6"
down_revision = "7c19ca08d5d4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "scoring_config",
        sa.Column(
            "evidence_weights",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            comment=(
                "Optional per-GO-evidence-code quality multipliers in [0, 1]. "
                "NULL means use the system defaults defined in DEFAULT_EVIDENCE_WEIGHTS. "
                "Partial dicts are allowed; absent codes fall back to the system table."
            ),
        ),
    )


def downgrade() -> None:
    op.drop_column("scoring_config", "evidence_weights")
