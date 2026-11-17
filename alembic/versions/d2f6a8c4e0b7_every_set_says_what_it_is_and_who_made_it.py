"""Every set says what it is and who made it

The sets gate (``scripts/check_sets.py``) asks four things of every set table:
what it is derived from, a sentence saying what it IS, a sha256 for every URI it
points at, and the job and code revision that made it. Five tables answered only
some of them. This adds the columns, nullable so no stored row moves; the writers
fill them from now on, and a NULL on an older row reads as "nothing says".

- ``annotation_set``: ``description``, ``code_revision``.
- ``evaluation_set``: ``description``.
- ``information_accretion_set``: ``description``, ``code_revision``.
- ``prediction_set``: ``description``, ``job_id`` (to ``job``, SET NULL),
  ``producer_git_sha`` (not ``code_revision``: ``meta["code_revision"]`` is the
  revision that identifies the method, and one name for two fields would make
  the method seal's partition ambiguous).
- ``dataset``: ``description``, ``train_sha256``, ``eval_sha256``.

Catalog-only on every table.

Revision ID: d2f6a8c4e0b7
Revises: c7d2e4f9a1b3
Create Date: 2026-11-17 10:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d2f6a8c4e0b7"
down_revision: str | Sequence[str] | None = "c7d2e4f9a1b3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: (table, column, type) in the order they are added; downgrade drops them reversed.
COLUMNS: tuple[tuple[str, str, sa.types.TypeEngine], ...] = (
    ("annotation_set", "description", sa.Text()),
    ("annotation_set", "code_revision", sa.String(length=64)),
    ("evaluation_set", "description", sa.Text()),
    ("information_accretion_set", "description", sa.Text()),
    ("information_accretion_set", "code_revision", sa.String(length=64)),
    ("prediction_set", "description", sa.Text()),
    ("prediction_set", "producer_git_sha", sa.String(length=64)),
    ("dataset", "description", sa.Text()),
    ("dataset", "train_sha256", sa.String(length=64)),
    ("dataset", "eval_sha256", sa.String(length=64)),
)


def upgrade() -> None:
    """Upgrade schema."""
    for table, column, kind in COLUMNS:
        op.add_column(table, sa.Column(column, kind, nullable=True))
    op.add_column(
        "prediction_set",
        sa.Column(
            "job_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job.id", ondelete="SET NULL", name="fk_prediction_set_job"),
            nullable=True,
        ),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint("fk_prediction_set_job", "prediction_set", type_="foreignkey")
    op.drop_column("prediction_set", "job_id")
    for table, column, _ in reversed(COLUMNS):
        op.drop_column(table, column)
