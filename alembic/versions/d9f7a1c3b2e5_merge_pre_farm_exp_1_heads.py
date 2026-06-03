"""merge_pre_farm_exp_1_heads

Revision ID: d9f7a1c3b2e5
Revises: 2a5e9b3f1c4d, 47de89cf6fec, b1c2d3e4f5a6, b8e3f1a7c2d9, d7e4c2b9a1f0
Create Date: 2026-05-16 19:30:00.000000

Pre-existing repo state: the alembic revision graph carries five
disjoint heads after a sequence of parallel feature branches landed
without re-rooting on each other:

* ``2a5e9b3f1c4d`` (T5.6a API key)
* ``47de89cf6fec`` (add evaluation_result)
* ``b1c2d3e4f5a6`` (add scoring_config)
* ``b8e3f1a7c2d9`` (LB.1 evaluation_set pair uniqueness)
* ``d7e4c2b9a1f0`` (T3.1a goprediction features JSONB)

``alembic upgrade head`` requires a single head, so the FARM-EXP.1
ExperimentRun-axis migration (the slice's actual work) cannot land on
top of any one of them in isolation. This empty merge migration
restores a single head; its only operation is the alembic-standard
multi-parent declaration.

No DDL, no DML. Strictly a graph-topology unblocker. The downgrade
restores the prior five-head state by removing this merge node.
"""

from __future__ import annotations

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "d9f7a1c3b2e5"
down_revision: str | Sequence[str] | None = (
    "2a5e9b3f1c4d",
    "47de89cf6fec",
    "b1c2d3e4f5a6",
    "b8e3f1a7c2d9",
    "d7e4c2b9a1f0",
)
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """No-op: merge migration only consolidates the revision graph."""


def downgrade() -> None:
    """No-op: alembic re-fanouts to the five parent heads on downgrade."""
