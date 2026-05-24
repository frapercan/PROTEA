"""farm_auth_8_session_revocation_table

Revision ID: e8a3f1b2c4d5
Revises: d6e5a7b8c9f1
Create Date: 2026-05-24 20:00:00.000000

FARM-AUTH.8 of the F-AUTH phase (ADR D37). Adds the ``user_session``
table that replaces the in-memory ``_SESSION_STORE`` dict from
FARM-AUTH.3. Provides durable session revocation and an admin
bulk-revoke path.

Schema
------
* ``id``             UUID PK — default ``gen_random_uuid()``.
* ``user_id``        UUID FK -> user.id ON DELETE CASCADE.
* ``token_hash``     Text NOT NULL UNIQUE — SHA-256 hex of the JWT
                     cookie value; O(1) lookup per request.
* ``created_at``     Timestamptz NOT NULL DEFAULT now().
* ``expires_at``     Timestamptz NOT NULL — mirrors the JWT ``exp``
                     claim; used for row pruning without token decoding.
* ``last_seen_at``   Timestamptz NOT NULL DEFAULT now() — updated on
                     every authenticated ``/me`` call.
* ``revoked_at``     Timestamptz NULL — set on logout or admin revoke.
* ``user_agent``     Text NULL — raw User-Agent header at login.
* ``client_ip_hash`` Text NULL — SHA-256 hex of client IP (privacy).

Indexes
-------
* ``ix_user_session_token_hash`` (UNIQUE) — primary per-request lookup.
* ``ix_user_session_user_id``             — admin bulk-revoke scan.

Strictly additive. Safe to apply on a live database without a
maintenance window.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e8a3f1b2c4d5"
down_revision: str | Sequence[str] | None = "d6e5a7b8c9f1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "user_session",
        sa.Column(
            "id",
            sa.UUID(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column(
            "user_id",
            sa.UUID(),
            sa.ForeignKey("user.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("token_hash", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column("client_ip_hash", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_user_session_token_hash",
        "user_session",
        ["token_hash"],
        unique=True,
    )
    op.create_index(
        "ix_user_session_user_id",
        "user_session",
        ["user_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_user_session_user_id", table_name="user_session")
    op.drop_index("ix_user_session_token_hash", table_name="user_session")
    op.drop_table("user_session")
