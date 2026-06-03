"""add_job_narrative_fields

Revision ID: fe74cbd2f871
Revises: 76cafcb8d9be
Create Date: 2026-05-09 09:30:00.000000

T3.9 of master plan v3.2 §24 Fase 4. Implements decision D11
(narrativa en Jobs): adds three additive columns to the ``job``
table so any UI / API surface can carry the run's narrative.

* ``description`` — pre-execution intent text supplied by the
  user / cron / orchestrator. Free-form, nullable, no default.
* ``findings`` — post-completion summary written by a worker
  or curator. Free-form, nullable.
* ``tags`` — lightweight grouping tokens (e.g. ``"ablation"``,
  ``"benchmark-v1"``). Postgres ``ARRAY(Text)`` so the index +
  filter side stays cheap; defaults to ``[]`` so existing rows
  retain a usable empty list rather than NULL.

All three are additive — no rewrites, no NOT NULL on existing
rows. Safe to apply on a live DB without a maintenance window.
T3.10 (JobComment ORM + endpoints) builds on top in a follow-up
slice.
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "fe74cbd2f871"
down_revision: str | Sequence[str] | None = "76cafcb8d9be"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("job", sa.Column("description", sa.Text(), nullable=True))
    op.add_column("job", sa.Column("findings", sa.Text(), nullable=True))
    op.add_column(
        "job",
        sa.Column(
            "tags",
            ARRAY(sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::text[]"),
        ),
    )


def downgrade() -> None:
    op.drop_column("job", "tags")
    op.drop_column("job", "findings")
    op.drop_column("job", "description")
