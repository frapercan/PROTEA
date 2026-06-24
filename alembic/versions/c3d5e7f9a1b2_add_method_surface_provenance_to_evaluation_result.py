"""F-METHOD-EVAL-SURFACE: add method-surface provenance to evaluation_result

Adds four nullable provenance markers to ``evaluation_result`` so every
benchmark number on ``/benchmark`` and ``/evaluation`` is self-describing:

- ``frame`` (``String(8)``): ``'lafa'`` | ``'internal'`` | ``NULL``,
  guarded by a closed-vocabulary CHECK constraint.
- ``temporal_window`` (``String(32)``): free-text rolling-origin window
  label, e.g. ``'SELECT_220_227'`` / ``'FINAL_227_230'`` / ``'other'``.
- ``arms_enabled`` (``JSONB``): flag dict of the method arms that
  contributed (``knn`` / ``reranker`` / ``mlp_tower`` / ``interpro``).
- ``leakage_role`` (``String(8)``): ``'select'`` | ``'test'`` | ``'probe'``
  | ``NULL`` (ADR D40 leakage-hygiene role), guarded by a CHECK.

All columns are nullable with no server default; legacy rows read back as
``NULL`` and the UI renders an "unknown" empty state. Metadata only: this
migration does not change how any metric is computed.

Revision ID: c3d5e7f9a1b2
Revises: f2a4c6e8b0d1
Create Date: 2026-06-10 00:00:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'c3d5e7f9a1b2'
down_revision: str | Sequence[str] | None = 'f2a4c6e8b0d1'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'evaluation_result',
        sa.Column('frame', sa.String(length=8), nullable=True),
    )
    op.add_column(
        'evaluation_result',
        sa.Column('temporal_window', sa.String(length=32), nullable=True),
    )
    op.add_column(
        'evaluation_result',
        sa.Column('arms_enabled', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        'evaluation_result',
        sa.Column('leakage_role', sa.String(length=8), nullable=True),
    )
    op.create_check_constraint(
        'ck_evaluation_result_frame',
        'evaluation_result',
        "frame IS NULL OR frame IN ('lafa', 'internal')",
    )
    op.create_check_constraint(
        'ck_evaluation_result_leakage_role',
        'evaluation_result',
        "leakage_role IS NULL OR leakage_role IN ('select', 'test', 'probe')",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        'ck_evaluation_result_leakage_role', 'evaluation_result', type_='check'
    )
    op.drop_constraint(
        'ck_evaluation_result_frame', 'evaluation_result', type_='check'
    )
    op.drop_column('evaluation_result', 'leakage_role')
    op.drop_column('evaluation_result', 'arms_enabled')
    op.drop_column('evaluation_result', 'temporal_window')
    op.drop_column('evaluation_result', 'frame')
