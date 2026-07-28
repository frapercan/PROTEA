"""ADR-D45 signal-store: promote the IA scalar to a typed column

Migration f9b2c1a4d7e0 promoted the six LAFA scalars (``classifier_*`` /
``self_prior_score`` / ``association_*``) to typed ``go_prediction`` columns.
The seventh per-candidate scalar that still lived only in the ``features``
JSONB blob is ``IA`` (information accretion, the cafaeval ``f_micro_w``
weighting value stamped by ``apply_ia``). The signal-store code-switch moves
every blob-only scalar to a typed column so the ``features`` blob becomes
fully redundant and a later reviewed step can drop it (roughly 74 GB reclaim).

The predict dict stamps this value under the upper-case ``IA`` key; the typed
column is the lower-case ``ia`` (SQL identifier convention). IA is NOT a
reranker feature (absent from ``protea_contracts.feature_schema.NUMERIC_FEATURES``),
so this column does not enter ``feature_schema_sha`` and no reranker is
invalidated.

Additive and instant: ``ADD COLUMN ... NULL`` is a metadata-only change, no
table rewrite. The column is NULLABLE, not zero-defaulted: a NULL means the IA
producer did not run for that row, which is the missing-value convention
ADR-D45 established. The eventual blob drop is a SEPARATE step.

Revision ID: d7f3a9c1e5b2
Revises: f9b2c1a4d7e0
Create Date: 2026-07-11 03:30:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d7f3a9c1e5b2"
down_revision: str | Sequence[str] | None = "f9b2c1a4d7e0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "go_prediction",
        sa.Column("ia", sa.Float(), nullable=True),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("go_prediction", "ia")
