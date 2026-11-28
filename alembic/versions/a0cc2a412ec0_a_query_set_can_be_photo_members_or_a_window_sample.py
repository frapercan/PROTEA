"""A query set can be a photo's members by flag, or a stratified sample of a window's targets

Two new kinds of declared population, for the donor pool's breadth and the toy's
targets:

- ``photo_members``: the members of one cut photo chosen by their admission flags
  (``truth``, ``superset``, ``curated``, ``neighbour``), optionally a hash-ordered
  sample of them, written by ``declare_photo_member_set``. The new column
  ``cut_photo_id`` names the photo (``RESTRICT``: a photo cannot vanish while a set
  derives from it). A set of this kind is declared like the corpus and the exam
  populations (a photo, a uri with its sha, a description and a job, and no window),
  and its idempotence is the partial unique index on ``(cut_photo_id,
  content_sha256)``: ``uq_query_set_window_content`` cannot catch it, because its
  ``evaluation_set_id`` is NULL and two NULLs do not collide.
- ``window_sample``: a stratified sample of a window's targets, written by
  ``declare_window_sample``. It derives from its window (``evaluation_set_id``), so
  ``uq_query_set_window_content`` already makes one window plus one set of bytes one
  row.

A photo is named only by a set of photo members, so a photo on any other kind is
refused as well.

The downgrade refuses while a row of either kind exists (the recreated vocabulary
check would fail on it), then drops what the upgrade made.

Revision ID: a0cc2a412ec0
Revises: f2e0fc2717f7
Create Date: 2026-11-28 18:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a0cc2a412ec0"
down_revision: str | Sequence[str] | None = "f2e0fc2717f7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The vocabulary at this revision, frozen here, and the one it replaces.
SOURCES = (
    "evaluation_targets",
    "fasta_upload",
    "quick_annotation",
    "corpus_population",
    "exam_population",
    "photo_members",
    "window_sample",
    "legacy_undeclared",
)
PREVIOUS = tuple(v for v in SOURCES if v not in {"photo_members", "window_sample"})

#: The model's checks and index predicate as they stand at this revision, frozen here.
PHOTO_MEMBERS_DECLARED_CHECK = (
    "source_kind <> 'photo_members' OR ("
    "cut_photo_id IS NOT NULL AND artifact_uri IS NOT NULL AND content_sha256 IS NOT NULL "
    "AND description IS NOT NULL AND job_id IS NOT NULL AND evaluation_set_id IS NULL)"
)
WINDOW_SAMPLE_DECLARED_CHECK = (
    "source_kind <> 'window_sample' OR ("
    "evaluation_set_id IS NOT NULL AND artifact_uri IS NOT NULL "
    "AND content_sha256 IS NOT NULL AND description IS NOT NULL AND job_id IS NOT NULL)"
)
PHOTO_ON_MEMBER_SETS_CHECK = "cut_photo_id IS NULL OR source_kind = 'photo_members'"
PHOTO_MEMBERS_PREDICATE = "source_kind = 'photo_members'"


def _source_kind_check(values: tuple[str, ...]) -> str:
    listed = ", ".join(f"'{v}'" for v in values)
    return f"source_kind IN ({listed})"


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "query_set",
        sa.Column("cut_photo_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_query_set_cut_photo_id",
        "query_set",
        "cut_photo",
        ["cut_photo_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.drop_constraint("ck_query_set_source_kind", "query_set", type_="check")
    op.create_check_constraint("ck_query_set_source_kind", "query_set", _source_kind_check(SOURCES))
    op.create_check_constraint(
        "ck_query_set_photo_members_are_declared", "query_set", PHOTO_MEMBERS_DECLARED_CHECK
    )
    op.create_check_constraint(
        "ck_query_set_window_sample_is_declared", "query_set", WINDOW_SAMPLE_DECLARED_CHECK
    )
    op.create_check_constraint(
        "ck_query_set_photo_on_member_sets", "query_set", PHOTO_ON_MEMBER_SETS_CHECK
    )
    op.create_index(
        "uq_query_set_photo_members_content",
        "query_set",
        ["cut_photo_id", "content_sha256"],
        unique=True,
        postgresql_where=sa.text(PHOTO_MEMBERS_PREDICATE),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("uq_query_set_photo_members_content", table_name="query_set")
    op.drop_constraint("ck_query_set_photo_on_member_sets", "query_set", type_="check")
    op.drop_constraint("ck_query_set_window_sample_is_declared", "query_set", type_="check")
    op.drop_constraint("ck_query_set_photo_members_are_declared", "query_set", type_="check")
    op.drop_constraint("ck_query_set_source_kind", "query_set", type_="check")
    # A row of either kind makes this fail, which is correct: dropping the values while
    # rows use them leaves kinds the check forbids and the data asserts.
    op.create_check_constraint(
        "ck_query_set_source_kind", "query_set", _source_kind_check(PREVIOUS)
    )
    op.drop_constraint("fk_query_set_cut_photo_id", "query_set", type_="foreignkey")
    op.drop_column("query_set", "cut_photo_id")
