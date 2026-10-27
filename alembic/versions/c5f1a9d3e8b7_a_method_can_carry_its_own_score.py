"""A prediction can carry the score its method produced

Every score PROTEA has ever published is DERIVED at evaluation time: a
``ScoringConfig`` takes a weighted average of retrieval signals off the
``go_prediction`` row, or a re-ranker booster reads them. ``_pred_base_cache``
states the consequence in one line -- "only the score column differs" across
the seven configs of a grid cell -- and it is the right design for a method
that is neighbour transfer plus features.

It cannot express a method that produces its own number. The lab's readers
emit a probability per (protein, term); there is no donor to measure a
distance to and no feature to weight. Until now the only way to get that
number through the evaluator was to write it into a column that means
something else (``distance = 2 * (1 - p)`` reproduces it exactly through
``embedding_similarity = 1 - d / 2``), which is the overload this project
keeps paying for elsewhere.

So two changes, both metadata-only and instant:

* ``method_score`` NULL: the number the method produced, in [0, 1]. NULL
  means the row was not produced by a method that scores its own candidates,
  which is every row written so far. It is read only by the ``method_score``
  formula, so no stored result moves.
* ``distance`` becomes nullable. It was NOT NULL because every writer was a
  retrieval, and the one method that is not (``predict_go_terms_from_interpro``)
  had to invent ``1 / vote_count`` to fill it. A method with no neighbour has
  no distance, and NULL is the only truthful value; ``scoring.py`` already
  treats a missing distance as a signal that drops out of the average rather
  than a zero, so nothing downstream changes for the rows that have one.

Revision ID: c5f1a9d3e8b7
Revises: b4e82f17c9a3
Create Date: 2026-10-27 12:05:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c5f1a9d3e8b7"
down_revision: str | Sequence[str] | None = "b4e82f17c9a3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "go_prediction",
        sa.Column("method_score", sa.Float(), nullable=True),
    )
    op.alter_column(
        "go_prediction",
        "distance",
        existing_type=sa.Float(),
        nullable=True,
    )


def downgrade() -> None:
    """Downgrade schema."""
    # Refuse rather than corrupt: restoring NOT NULL would have to invent a
    # distance for every method row, which is the lie the column was made
    # nullable to avoid.
    bind = op.get_bind()
    orphans = bind.execute(
        sa.text("select count(*) from go_prediction where distance is null")
    ).scalar_one()
    if orphans:
        raise RuntimeError(
            f"{orphans} go_prediction rows have no distance; restoring NOT NULL "
            "would require inventing one. Delete those prediction sets first."
        )
    op.alter_column(
        "go_prediction",
        "distance",
        existing_type=sa.Float(),
        nullable=False,
    )
    op.drop_column("go_prediction", "method_score")
