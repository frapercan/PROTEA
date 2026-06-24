"""Add params column to scoring_config (A-SCORE advanced knobs).

Revision ID: f3a1c2d4e5b6
Revises: c3d5e7f9a1b2
Create Date: 2026-06-11

Motivation
----------
The A-SCORE study needs to ablate / tune richer scoring axes that the
linear scorer could not previously express: a term-specificity (IA) prior,
a soft-vote temperature, and a post-score calibration hook.

This migration adds an optional ``params`` JSONB column holding those
advanced knobs. Existing rows receive ``NULL``, which the scoring engine
interprets as "all advanced knobs off" and reproduces the pre-A-SCORE
behaviour byte for byte. The change is fully backwards-compatible.
See ``protea.infrastructure.orm.models.embedding.scoring_config.DEFAULT_PARAMS``
for the schema and ``protea.core.scoring`` for the formulas.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "f3a1c2d4e5b6"
down_revision = "c3d5e7f9a1b2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "scoring_config",
        sa.Column(
            "params",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            comment=(
                "Optional advanced scoring knobs (ia_prior, soft_vote, "
                "calibration). NULL reproduces the legacy behaviour exactly. "
                "See DEFAULT_PARAMS for the schema."
            ),
        ),
    )


def downgrade() -> None:
    op.drop_column("scoring_config", "params")
