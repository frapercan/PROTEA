"""f_ops_jobs_1a_dedup_key_leased_until

Revision ID: a7b3c8d2e1f4
Revises: b8dc06ee4a98
Create Date: 2026-05-26 10:00:00.000000

F-OPS-JOBS.1a -- Job synchronisation hardening (dedup + lease)

Adds two new columns to the ``job`` table:

dedup_key (VARCHAR, nullable)
    A 16-hex-char prefix of SHA-256(canonical(operation, payload)).
    NULL for legacy rows. A partial unique index enforces uniqueness
    only among active jobs (queued/running) so the same op+payload can
    be re-submitted once the previous instance reaches a terminal state.

leased_until (TIMESTAMPTZ, nullable)
    Set by BaseWorker on claim and renewed every
    ``job_heartbeat_interval_seconds`` (default 30s). The stale-job
    reaper uses this field instead of ``started_at`` alone so a live
    heartbeating job is never reaped. NULL for legacy rows (reaper
    falls back to the event-activity heuristic for those).

Both columns are additive and fully backward-compatible: legacy rows
keep NULL in both fields and existing queries are unaffected.

Reversible: ``downgrade()`` drops the index and columns cleanly.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a7b3c8d2e1f4"
down_revision: str | Sequence[str] | None = "b8dc06ee4a98"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. Add dedup_key column (nullable, no default required for legacy rows).
    op.add_column("job", sa.Column("dedup_key", sa.Text(), nullable=True))

    # 2. Add leased_until column (nullable TIMESTAMPTZ).
    op.add_column(
        "job",
        sa.Column("leased_until", sa.DateTime(timezone=True), nullable=True),
    )

    # 3. Partial unique index: dedup_key must be unique among active jobs
    #    (status IN ('QUEUED', 'RUNNING')) where dedup_key IS NOT NULL.
    #    The literals match the Postgres ``job_status`` enum, whose members
    #    are stored as uppercase identifiers (QUEUED, RUNNING, ...). Using
    #    lowercase here triggers ``InvalidTextRepresentation`` at index
    #    creation time because PG resolves the IN literals against the
    #    declared enum type. Partial indexes exclude NULLs from uniqueness
    #    enforcement, but the explicit IS NOT NULL predicate makes the
    #    intent unmistakable.
    op.create_index(
        "uq_job_dedup_key_active",
        "job",
        ["dedup_key"],
        unique=True,
        postgresql_where=sa.text(
            "status IN ('QUEUED', 'RUNNING') AND dedup_key IS NOT NULL"
        ),
    )


def downgrade() -> None:
    op.drop_index("uq_job_dedup_key_active", table_name="job")
    op.drop_column("job", "leased_until")
    op.drop_column("job", "dedup_key")
