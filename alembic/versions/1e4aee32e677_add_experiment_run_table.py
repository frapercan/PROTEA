"""add_experiment_run_table

Revision ID: 1e4aee32e677
Revises: 6105c3e6fca7
Create Date: 2026-05-09 10:10:00.000000

T3.8 of master plan v3.2 §24 Fase 4. Adds the ``experiment_run``
table — the narrative anchor that F-EXP campaigns (T-EXP.1-7)
and the F8b Experiments UI hang their per-run metadata off of.

Schema
------
* ``id``           UUID PK — default gen_random_uuid().
* ``name``         Text NOT NULL UNIQUE — slug shown in the UI;
                   used as the natural key in scripts / CLI lookups.
* ``description``  Text NULL — free-form research-question text
                   (decision D11 mirrors the Job narrative trio).
* ``hypothesis``   Text NULL — what the run is trying to prove.
* ``findings``     Text NULL — post-completion summary written by
                   a curator after the run closes.
* ``status``       experiment_run_status enum NOT NULL DEFAULT
                   'planned' — planned / running / done / abandoned.
* ``config``       JSONB NOT NULL DEFAULT '{}' — pinned parameters
                   (cohort, embedding ids, K values, ...).
* ``provenance``   JSONB NOT NULL DEFAULT '{}' — output of
                   ``protea.core.provenance.capture_provenance``.
* ``tags``         ARRAY(Text) NOT NULL DEFAULT '{}' — light grouping
                   tokens (mirrors Job.tags from T3.9).
* ``created_at``   Timestamptz NOT NULL DEFAULT now().
* ``started_at``   Timestamptz NULL — flipped when status -> running.
* ``finished_at``  Timestamptz NULL — flipped when status -> done /
                   abandoned.

Indexes
-------
* ``ix_experiment_run_name`` (UNIQUE) — natural key lookup.
* ``ix_experiment_run_status_created_at`` — list-by-status with
  newest-first ordering, mirroring Job's status+created_at index.

Strictly additive — no rewrites, no NOT NULL on existing rows
(the table is brand-new). Safe to apply on a live DB without a
maintenance window. Endpoints (T4.7-T4.9 narrative endpoints)
land in a follow-up slice.
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "1e4aee32e677"
down_revision: str | Sequence[str] | None = "6105c3e6fca7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_STATUS_VALUES: tuple[str, ...] = ("planned", "running", "done", "abandoned")


def upgrade() -> None:
    # ``sa.Enum`` emits ``CREATE TYPE`` automatically as part of the
    # ``CREATE TABLE`` it gets attached to; passing ``create_type=False``
    # would skip that, but here we want it created in line with the table
    # (the table is brand-new so the type can never pre-exist on a fresh
    # DB; on a re-run the migration script is idempotent only via Alembic
    # versioning, not psql-level checks, so we simply let Alembic decide).
    status_enum = sa.Enum(*_STATUS_VALUES, name="experiment_run_status")
    op.create_table(
        "experiment_run",
        sa.Column(
            "id",
            sa.UUID(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("hypothesis", sa.Text(), nullable=True),
        sa.Column("findings", sa.Text(), nullable=True),
        sa.Column(
            "status",
            status_enum,
            nullable=False,
            server_default=sa.text("'planned'::experiment_run_status"),
        ),
        sa.Column(
            "config",
            JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "provenance",
            JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "tags",
            ARRAY(sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::text[]"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_experiment_run_name",
        "experiment_run",
        ["name"],
        unique=True,
    )
    op.create_index(
        "ix_experiment_run_status_created_at",
        "experiment_run",
        ["status", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_experiment_run_status_created_at", table_name="experiment_run")
    op.drop_index("ix_experiment_run_name", table_name="experiment_run")
    op.drop_table("experiment_run")
    sa.Enum(name="experiment_run_status").drop(op.get_bind(), checkfirst=True)
