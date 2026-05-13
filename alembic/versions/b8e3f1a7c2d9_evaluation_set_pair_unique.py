"""evaluation_set_pair_unique

Revision ID: b8e3f1a7c2d9
Revises: a3c1d8e2f4b6
Create Date: 2026-05-13 09:00:00.000000

Adds a UNIQUE constraint on
``evaluation_set (old_annotation_set_id, new_annotation_set_id)``.

The pair semantically identifies one evaluation episode (one delta
between two GOA snapshots). Two rows for the same pair are always
duplicates by construction. A pre-existing duplicate was discovered on
2026-05-12 when the LB.1 vanilla dump hit ``MultipleResultsFound`` at
``protea/core/training_dump_helpers.py`` after ~3.5h of compute. The
duplicate was cleaned manually (orphan with no groundtruth_uri); this
migration prevents recurrence at the schema level.

Pre-flight
----------

Before adding the constraint, we count duplicate pairs. If any are
found, the migration aborts with a clear error listing the offending
pairs so the operator can drop the orphans (typically the row with
``groundtruth_uri IS NULL`` and no ``evaluation_result`` children) and
re-run.

Downgrade drops the named constraint.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b8e3f1a7c2d9"
down_revision: str | Sequence[str] | None = "a3c1d8e2f4b6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_CONSTRAINT_NAME = "uq_evaluation_set_old_new_annotation_set"


def upgrade() -> None:
    """Add the UNIQUE constraint after verifying no duplicates exist."""
    bind = op.get_bind()
    dup_rows = bind.exec_driver_sql(
        """
        SELECT old_annotation_set_id, new_annotation_set_id, count(*) AS n
        FROM evaluation_set
        GROUP BY old_annotation_set_id, new_annotation_set_id
        HAVING count(*) > 1
        ORDER BY n DESC
        """
    ).fetchall()
    if dup_rows:
        details = ", ".join(f"({old}->{new}: {n} rows)" for old, new, n in dup_rows)
        raise RuntimeError(
            "Cannot add UNIQUE constraint on evaluation_set "
            "(old_annotation_set_id, new_annotation_set_id): "
            f"{len(dup_rows)} duplicate pair(s) found: {details}. "
            "Clean up the orphan rows (typically those with "
            "groundtruth_uri IS NULL and no evaluation_result children) "
            "before re-running this migration."
        )

    op.create_unique_constraint(
        _CONSTRAINT_NAME,
        "evaluation_set",
        ["old_annotation_set_id", "new_annotation_set_id"],
    )


def downgrade() -> None:
    """Drop the UNIQUE constraint."""
    op.drop_constraint(
        _CONSTRAINT_NAME,
        "evaluation_set",
        type_="unique",
    )
