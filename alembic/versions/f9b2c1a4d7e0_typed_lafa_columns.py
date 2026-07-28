"""ADR-D45 storage: promote the six LAFA scalars to typed columns

The ``classifier_*`` / ``self_prior_score`` / ``association_*`` families
were the last features living only in the ``go_prediction.features`` JSONB
blob, outside the typed-column space. This adds them as nullable Float
columns so the blob becomes fully redundant (54 of its 60 keys already
duplicate typed columns), which lets a later reviewed step drop the blob
and reclaim roughly 75 GB.

Additive and instant: ``ADD COLUMN ... NULL`` is a metadata-only change,
no table rewrite. The columns are NULLABLE, not zero-defaulted: a NULL
means the producer did not run for that row, which is the missing-value
convention ADR-D45 established (the export emits NaN, never 0.0). The
backfill from the blob and the eventual blob drop are SEPARATE steps.

Revision ID: f9b2c1a4d7e0
Revises: a8b1c2d3e4f5
Create Date: 2026-07-11 02:00:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f9b2c1a4d7e0"
down_revision: str | Sequence[str] | None = "a8b1c2d3e4f5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_COLUMNS = (
    "classifier_score",
    "classifier_present",
    "self_prior_score",
    "association_total",
    "association_cross",
    "association_present",
)


def upgrade() -> None:
    """Upgrade schema."""
    for name in _COLUMNS:
        op.add_column(
            "go_prediction",
            sa.Column(name, sa.Float(), nullable=True),
        )


def downgrade() -> None:
    """Downgrade schema."""
    for name in reversed(_COLUMNS):
        op.drop_column("go_prediction", name)
