"""farm_auth_9_audit_log

Revision ID: a1f2b3c4d5e6
Revises: d6e5a7b8c9f1, b8e3f1a7c2d9
Create Date: 2026-05-24 18:00:00.000000

FARM-AUTH.9 — append-only audit log table.

Creates the ``auth_audit`` table that records every security-relevant
event in a PROTEA deployment (login, logout, signup, admin approval,
admin revocation, role change, etc.).  The table is append-only by
convention; the helper in ``protea.api.auth.audit`` never issues UPDATE
or DELETE against it.

Schema
------

* ``id``             TIMESTAMPTZ PK with ``gen_random_uuid()`` default.
* ``occurred_at``    TIMESTAMPTZ NOT NULL DEFAULT now(); indexed.
* ``event_type``     Text NOT NULL; indexed.
* ``actor_user_id``  UUID FK -> user(id) ON DELETE SET NULL; nullable.
* ``target_user_id`` UUID FK -> user(id) ON DELETE SET NULL; nullable.
* ``client_ip_hash`` Text; SHA-256 of raw client IP (forensic but no PII).
* ``details``        JSONB; free-form supplementary payload.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1f2b3c4d5e6"
down_revision: str | Sequence[str] | None = ("d6e5a7b8c9f1", "b8e3f1a7c2d9")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "auth_audit",
        sa.Column(
            "id",
            PG_UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            primary_key=True,
        ),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("event_type", sa.Text, nullable=False),
        sa.Column(
            "actor_user_id",
            PG_UUID(as_uuid=True),
            sa.ForeignKey("user.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "target_user_id",
            PG_UUID(as_uuid=True),
            sa.ForeignKey("user.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("client_ip_hash", sa.Text, nullable=True),
        sa.Column("details", JSONB, nullable=True),
    )
    op.create_index("ix_auth_audit_occurred_at", "auth_audit", ["occurred_at"])
    op.create_index("ix_auth_audit_event_type", "auth_audit", ["event_type"])
    op.create_index("ix_auth_audit_actor_user_id", "auth_audit", ["actor_user_id"])
    op.create_index("ix_auth_audit_target_user_id", "auth_audit", ["target_user_id"])


def downgrade() -> None:
    op.drop_index("ix_auth_audit_target_user_id", table_name="auth_audit")
    op.drop_index("ix_auth_audit_actor_user_id", table_name="auth_audit")
    op.drop_index("ix_auth_audit_event_type", table_name="auth_audit")
    op.drop_index("ix_auth_audit_occurred_at", table_name="auth_audit")
    op.drop_table("auth_audit")
