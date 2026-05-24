"""farm_auth_11_email_token

Revision ID: b2e4f1a9c3d7
Revises: a1f2b3c4d5e6
Create Date: 2026-05-24 22:00:00.000000

FARM-AUTH.11 -- one-time email token table for magic-link and
password-reset flows.

Creates the ``email_token`` table that backs the optional SMTP auth
flows introduced in this slice. Tokens are hashed (SHA-256) before
storage so a DB breach does not let an attacker replay undelivered
tokens.

Schema
------

* ``id``           UUID PK (gen_random_uuid() default).
* ``user_id``      UUID FK -> user(id) ON DELETE CASCADE.
* ``token_hash``   SHA-256 hex digest of the raw token; unique + indexed.
* ``kind``         Text: 'magic_link' or 'password_reset'.
* ``created_at``   TIMESTAMPTZ NOT NULL DEFAULT now().
* ``expires_at``   TIMESTAMPTZ NOT NULL (15-minute TTL set at insert).
* ``consumed_at``  TIMESTAMPTZ NULL; set on first successful consume.

Indexes
-------

* ``ix_email_token_token_hash`` -- unique; primary lookup path.
* ``ix_email_token_user_id``    -- list/purge tokens for a given user.
* ``ix_email_token_expires_at`` -- expired-token purge jobs.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "b2e4f1a9c3d7"
down_revision: str | Sequence[str] | None = "a1f2b3c4d5e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "email_token",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("user.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("token_hash", sa.Text, nullable=False, unique=True),
        sa.Column("kind", sa.Text, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
    )
    # IF NOT EXISTS keeps the upgrade idempotent on the shared CI pg
    # service container, which can carry leftover indexes from previous
    # PR runs even after the alembic_version row is reset.
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_email_token_token_hash "
        "ON email_token (token_hash)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_email_token_user_id "
        "ON email_token (user_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_email_token_expires_at "
        "ON email_token (expires_at)"
    )


def downgrade() -> None:
    op.drop_index("ix_email_token_expires_at", table_name="email_token")
    op.drop_index("ix_email_token_user_id", table_name="email_token")
    op.drop_index("ix_email_token_token_hash", table_name="email_token")
    op.drop_table("email_token")
