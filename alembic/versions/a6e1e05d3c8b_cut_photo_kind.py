"""A photo's kind: cut_photo.kind, with the UniRef columns empty on a truth-only photo

ADR-D59, amended 2026-11-16 with the author: a cut that only ends a window (205,
the end of 177->205) is built as a truth-only photo, its members and their GOA rows
and its Swiss-Prot list, with no UniRef neighbours and no TrEMBL sequences, since
no method predicts from it. ``cut_photo`` gains ``kind`` (``full`` for every
photo loaded before this revision), and its three UniRef columns become nullable,
held by a check: a full photo names its UniRef release, a truth-only one names none.

Downgrading refuses while a truth-only photo exists.

Revision ID: a6e1e05d3c8b
Revises: 384568a09847
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "a6e1e05d3c8b"
down_revision = "384568a09847"
branch_labels = None
depends_on = None

#: The same text the ORM model declares; a test compares the two.
KIND_CHECK = (
    "(kind = 'full' AND uniref_release IS NOT NULL AND uniref_published_at IS NOT NULL "
    "AND uniref_clusters_sha256 IS NOT NULL) OR (kind = 'truth_only' AND uniref_release IS NULL "
    "AND uniref_published_at IS NULL AND uniref_clusters_sha256 IS NULL)"
)
_UNIREF_COLUMNS = (
    ("uniref_release", sa.String(16)),
    ("uniref_published_at", sa.Date()),
    ("uniref_clusters_sha256", sa.String(64)),
)


def upgrade() -> None:
    op.add_column(
        "cut_photo", sa.Column("kind", sa.String(16), nullable=False, server_default="full")
    )
    for name, kind in _UNIREF_COLUMNS:
        op.alter_column("cut_photo", name, existing_type=kind, nullable=True)
    op.create_check_constraint("ck_cut_photo_kind", "cut_photo", KIND_CHECK)


def downgrade() -> None:
    truth_only = (
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM cut_photo WHERE kind <> 'full'"))
        .scalar_one()
    )
    if truth_only:
        raise RuntimeError(f"{truth_only} truth-only photos exist and name no UniRef release")
    op.drop_constraint("ck_cut_photo_kind", "cut_photo", type_="check")
    for name, kind in _UNIREF_COLUMNS:
        op.alter_column("cut_photo", name, existing_type=kind, nullable=False)
    op.drop_column("cut_photo", "kind")
