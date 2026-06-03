"""farm_exp_1_experiment_run_axis

Revision ID: e1c4a7b2d8f3
Revises: d9f7a1c3b2e5
Create Date: 2026-05-16 19:35:00.000000

FARM-EXP.1: adds 9 axis columns + a partial UNIQUE index to
``experiment_run`` so every benchmark cell is addressable by its axis
tuple shortid (see ``protea.core.axis_tuple`` + the
``protea_contracts.axis_tuple_shortid`` helper shipped on the companion
protea-contracts PR).

Columns added (all nullable; rows pre-FARM-EXP.1 have NULL until the
backfill at ``scripts/backfill_experiment_run_axis.py`` populates them):

* ``plm``                     ``Text``
* ``k``                       ``Integer``
* ``reranker_spec_id``        ``Text``
* ``feature_schema_sha``      ``Text``
* ``eval_set_name``           ``Text``
* ``eval_set_manifest_sha``   ``Text``
* ``propagation``             ``Text``
* ``ensemble_spec``           ``JSONB``
* ``axis_tuple_shortid``      ``Text``

Constraint: partial-unique index
``uq_experiment_run_axis_tuple_shortid`` over
``(axis_tuple_shortid) WHERE axis_tuple_shortid IS NOT NULL``. The
partial filter lets legacy / partially-backfilled rows coexist (NULL
shortid is not yet identifying) while the cell-catalog membership
contract holds once the shortid is set.

Strictly additive; no data migration. Downgrade drops the unique
index then the columns in reverse order.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e1c4a7b2d8f3"
down_revision: str | Sequence[str] | None = "d9f7a1c3b2e5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_NEW_COLUMNS: tuple[tuple[str, sa.types.TypeEngine[object]], ...] = (
    ("plm", sa.Text()),
    ("k", sa.Integer()),
    ("reranker_spec_id", sa.Text()),
    ("feature_schema_sha", sa.Text()),
    ("eval_set_name", sa.Text()),
    ("eval_set_manifest_sha", sa.Text()),
    ("propagation", sa.Text()),
    ("ensemble_spec", JSONB()),
    ("axis_tuple_shortid", sa.Text()),
)


def upgrade() -> None:
    """Add the 9 axis columns + partial-unique index on shortid."""
    for name, coltype in _NEW_COLUMNS:
        op.add_column(
            "experiment_run",
            sa.Column(name, coltype, nullable=True),
        )
    op.create_index(
        "uq_experiment_run_axis_tuple_shortid",
        "experiment_run",
        ["axis_tuple_shortid"],
        unique=True,
        postgresql_where=sa.text("axis_tuple_shortid IS NOT NULL"),
    )


def downgrade() -> None:
    """Drop the index then the columns in reverse declaration order."""
    op.drop_index(
        "uq_experiment_run_axis_tuple_shortid",
        table_name="experiment_run",
    )
    for name, _ in reversed(_NEW_COLUMNS):
        op.drop_column("experiment_run", name)
