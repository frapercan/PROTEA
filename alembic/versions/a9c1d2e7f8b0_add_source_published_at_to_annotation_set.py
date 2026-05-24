"""add_source_published_at_to_annotation_set

Revision ID: a9c1d2e7f8b0
Revises: fe74cbd2f871
Create Date: 2026-05-24 02:00:00.000000

Adds a nullable, timezone-aware ``source_published_at`` column to
``annotation_set``. The intent is to back the UI's GOA-release timeline
with the official EBI publication date (scraped from
``ftp.ebi.ac.uk/pub/databases/GO/goa/old/UNIPROT/`` by the
``refresh_goa_release_dates`` operation).

This migration is purely additive. It does not backfill: existing rows
remain NULL until the refresh operation runs, at which point the UI
falls back to a greyed-out indicator with a hint to dispatch the
operation.
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a9c1d2e7f8b0"
down_revision: str | Sequence[str] | None = "fe74cbd2f871"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "annotation_set",
        sa.Column(
            "source_published_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("annotation_set", "source_published_at")
