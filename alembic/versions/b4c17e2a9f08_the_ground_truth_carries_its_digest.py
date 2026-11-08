"""The ground truth carries its digest

``evaluation_set`` had ``groundtruth_uri`` and no sha256, and
``load_evaluation_data_for_set`` fetched the parquet and verified nothing --
not because it verified badly, but because there was nothing to verify
against, so the read looked exactly like a verified one. Five other artefacts
in this project do carry their digest (``obo_sha256``,
``information_accretion_set.content_sha256``), and this was the odd one out.

NO BACKFILL, AND THAT IS NOT LAZINESS. Filling it would mean fetching 133
parquets out of the object store inside a migration: slow, dependent on MinIO
being reachable from wherever alembic runs, and failing halfway would leave
some rows claiming a digest and others not with nothing saying which pass
produced what. The rows stay NULL, and
``protea.core._artifact_identity.fetch_and_verify`` treats NULL as "nothing
can say whether these are the right bytes" -- reported, never passed in
silence. A separate operation can seal them later, through the platform,
leaving a job behind.

Additive and nullable, so downgrade drops the one column.

Revision ID: b4c17e2a9f08
Revises: a7d3e9b1c604
Create Date: 2026-11-08 02:10:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b4c17e2a9f08"
down_revision: str | Sequence[str] | None = "a7d3e9b1c604"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "evaluation_set",
        sa.Column("groundtruth_sha256", sa.String(length=64), nullable=True),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("evaluation_set", "groundtruth_sha256")
