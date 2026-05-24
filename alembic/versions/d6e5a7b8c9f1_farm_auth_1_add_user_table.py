"""farm_auth_1_add_user_table

Revision ID: d6e5a7b8c9f1
Revises: a9c1d2e7f8b0
Create Date: 2026-05-24 17:00:00.000000

FARM-AUTH.1 of the F-AUTH phase (ADR D37). Brings up the ``user``
table and the two Postgres enum types it depends on (``user_role``
and ``user_status``).

Schema
------
* ``id``              UUID PK — default ``gen_random_uuid()``.
* ``email``           Text NOT NULL UNIQUE — primary login handle.
* ``username``        Text NOT NULL UNIQUE — display handle in UI.
* ``display_name``    Text NULL — optional long-form name.
* ``password_hash``   Text NOT NULL — Argon2id PHC string (the salt
                      and parameters are encoded inside the value, so
                      no separate salt column is needed).
* ``role``            user_role enum NOT NULL DEFAULT ``researcher``.
* ``status``          user_status enum NOT NULL DEFAULT ``pending``
                      so signups land in the manual-approval queue
                      until an admin promotes them.
* ``intended_use``    Text NULL — what the user wants the account
                      for; surfaced on the admin approval screen.
* ``created_at``      Timestamptz NOT NULL DEFAULT ``now()``.
* ``last_login_at``   Timestamptz NULL — last successful login.
* ``deactivated_at``  Timestamptz NULL — set when an admin disables
                      the account; row is retained for audit-log FK
                      integrity.

Indexes
-------
* ``uq_user_email`` (UNIQUE) — login lookup.
* ``uq_user_username`` (UNIQUE) — display-handle lookup.
* ``ix_user_status_created_at`` — list-pending-users dashboard query.

Strictly additive — the table is brand-new. Safe to apply on a live
DB without a maintenance window. Subsequent F-AUTH slices land their
own migrations on top (bootstrap admin, sessions, audit log).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d6e5a7b8c9f1"
down_revision: str | Sequence[str] | None = "a9c1d2e7f8b0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_ROLE_VALUES: tuple[str, ...] = ("guest", "researcher", "operator", "admin")
_STATUS_VALUES: tuple[str, ...] = ("pending", "active", "deactivated")


def upgrade() -> None:
    role_enum = sa.Enum(*_ROLE_VALUES, name="user_role")
    status_enum = sa.Enum(*_STATUS_VALUES, name="user_status")
    op.create_table(
        "user",
        sa.Column(
            "id",
            sa.UUID(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("email", sa.Text(), nullable=False),
        sa.Column("username", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=True),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column(
            "role",
            role_enum,
            nullable=False,
            server_default=sa.text("'researcher'::user_role"),
        ),
        sa.Column(
            "status",
            status_enum,
            nullable=False,
            server_default=sa.text("'pending'::user_status"),
        ),
        sa.Column("intended_use", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("uq_user_email", "user", ["email"], unique=True)
    op.create_index("uq_user_username", "user", ["username"], unique=True)
    op.create_index(
        "ix_user_status_created_at",
        "user",
        ["status", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_user_status_created_at", table_name="user")
    op.drop_index("uq_user_username", table_name="user")
    op.drop_index("uq_user_email", table_name="user")
    op.drop_table("user")
    sa.Enum(name="user_status").drop(op.get_bind(), checkfirst=True)
    sa.Enum(name="user_role").drop(op.get_bind(), checkfirst=True)
