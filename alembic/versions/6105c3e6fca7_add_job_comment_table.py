"""add_job_comment_table

Revision ID: 6105c3e6fca7
Revises: fe74cbd2f871
Create Date: 2026-05-09 09:50:00.000000

T3.10 of master plan v3.2 §24 Fase 4. Implements decision D11
(narrativa en Jobs): adds the ``job_comment`` table so curators
and operators can attach a chronological thread of timestamped
notes to any Job row.

Schema
------
* ``id``         BigInt PK (autoincrement) — comment identity.
* ``job_id``     UUID FK to ``job(id)`` ON DELETE CASCADE — every
                 comment is owned by its parent job.
* ``author``     Text NULL — free-form actor tag (user, bot, cron).
* ``body``       Text NOT NULL — the comment payload (markdown allowed).
* ``created_at`` Timestamptz NOT NULL DEFAULT now-tz — chronological
                 ordering anchor.

Single supporting index ``ix_job_comment_job_id_created_at`` covers
the natural read pattern (``WHERE job_id = ... ORDER BY created_at``).

Strictly additive — no rewrites, no NOT NULL on existing rows. Safe
to apply on a live DB without a maintenance window. T3.10 endpoints
(``POST /jobs/{id}/comments``, ``GET /jobs/{id}/comments``) ship in
the same PR.
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "6105c3e6fca7"
down_revision: str | Sequence[str] | None = "fe74cbd2f871"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "job_comment",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("job_id", sa.UUID(), nullable=False),
        sa.Column("author", sa.Text(), nullable=True),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["job_id"], ["job.id"], ondelete="CASCADE", name="fk_job_comment_job_id"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_job_comment_job_id_created_at",
        "job_comment",
        ["job_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_job_comment_job_id_created_at", table_name="job_comment")
    op.drop_table("job_comment")
