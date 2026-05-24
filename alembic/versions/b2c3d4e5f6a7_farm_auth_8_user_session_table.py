"""farm_auth_8_user_session_table

Revision ID: b2c3d4e5f6a7
Revises: a1f2b3c4d5e6
Create Date: 2026-05-24 19:00:00.000000

FARM-AUTH.8 - persistent user_session table.

Replaces the in-process ``_SESSION_STORE`` dict (AUTH.3) with a durable
``user_session`` table so that logout invalidates sessions server-wide
and an admin can revoke all sessions for a target user.

Schema
------

* ``id``             UUID PK (gen_random_uuid()).
* ``user_id``        UUID FK to user(id) ON DELETE CASCADE; indexed.
* ``token_hash``     SHA-256 hex of the raw cookie JWT; UNIQUE; indexed.
* ``created_at``     TIMESTAMPTZ; set at row creation.
* ``expires_at``     TIMESTAMPTZ; mirrors the JWT exp claim; indexed.
* ``last_seen_at``   TIMESTAMPTZ; updated on every /auth/me hit.
* ``revoked_at``     TIMESTAMPTZ nullable; non-null once invalidated.
* ``user_agent``     Text nullable; raw User-Agent for forensics.
* ``client_ip_hash`` Text nullable; SHA-256 of client IP (no raw PII).

Note: no ::user_role-style cast in server_default; all defaults are
plain SQL expressions (now(), gen_random_uuid()) to avoid the
silent-rollback bug documented in PR #489 commit 39273e4.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b2c3d4e5f6a7"
down_revision: str | Sequence[str] | None = "a1f2b3c4d5e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "user_session",
        sa.Column(
            "id",
            PG_UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            primary_key=True,
        ),
        sa.Column(
            "user_id",
            PG_UUID(as_uuid=True),
            sa.ForeignKey("user.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("token_hash", sa.Text, nullable=False),
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
        sa.Column("user_agent", sa.Text, nullable=True),
        sa.Column("client_ip_hash", sa.Text, nullable=True),
    )
    op.create_index(
        "uq_user_session_token_hash", "user_session", ["token_hash"], unique=True
    )
    op.create_index("ix_user_session_user_id", "user_session", ["user_id"])
    op.create_index("ix_user_session_expires_at", "user_session", ["expires_at"])


def downgrade() -> None:
    op.drop_index("ix_user_session_expires_at", table_name="user_session")
    op.drop_index("ix_user_session_user_id", table_name="user_session")
    op.drop_index("uq_user_session_token_hash", table_name="user_session")
    op.drop_table("user_session")
