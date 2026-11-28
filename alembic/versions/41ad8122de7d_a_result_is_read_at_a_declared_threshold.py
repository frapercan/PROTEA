"""A result is read at a declared threshold

The literature ladder reads every arm at a threshold declared before the read,
per evaluator cell, and records the reading: this adds the table that holds it.

- ``evaluation_operating_point``: one row per (evaluation result, declared
  threshold), written by ``read_operating_point``. The result and the weights
  whose artifact declares the per-cell thresholds are ``RESTRICT`` foreign keys
  (what a reading derives from cannot vanish under it); ``cells`` holds per cell
  the threshold and the numbers read at it; ``t0_category_disagreement`` the
  t0 category against the evaluator's over the window's benchmark pairs (NULL
  when the window's frame states no t0 category); the per-stratum parquet by
  URI and sha256; the description its writer derives, the job and the code
  revision. Unique on (result, threshold).

The downgrade drops the table; the strata parquets stay in the store.

Revision ID: 41ad8122de7d
Revises: 532d2c1db500
Create Date: 2026-11-28 18:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "41ad8122de7d"
down_revision: str | Sequence[str] | None = "532d2c1db500"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The model's checks as they stand at this revision, frozen here.
DESCRIPTIONS_CHECK = "length(description) > 0"
DIGESTS_CHECK = "length(strata_sha256) = 64"


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "evaluation_operating_point",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "evaluation_result_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("evaluation_result.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "declared_threshold_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("model_weights.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("cells", postgresql.JSONB(), nullable=False),
        sa.Column("t0_category_disagreement", postgresql.JSONB(), nullable=True),
        sa.Column("strata_uri", sa.String(length=512), nullable=False),
        sa.Column("strata_sha256", sa.String(length=64), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column(
            "job_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("code_revision", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint(
            "evaluation_result_id",
            "declared_threshold_id",
            name="uq_evaluation_operating_point_result_threshold",
        ),
        sa.CheckConstraint(DESCRIPTIONS_CHECK, name="ck_evaluation_operating_point_descriptions"),
        sa.CheckConstraint(DIGESTS_CHECK, name="ck_evaluation_operating_point_digests"),
    )
    op.create_index(
        "ix_evaluation_operating_point_declared_threshold_id",
        "evaluation_operating_point",
        ["declared_threshold_id"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ix_evaluation_operating_point_declared_threshold_id",
        table_name="evaluation_operating_point",
    )
    op.drop_table("evaluation_operating_point")
