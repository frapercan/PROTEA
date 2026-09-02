"""An evaluation set is identified by its propagation graphs too.

WHY. ``uq_evaluation_set_old_new_annotation_set`` made ``(old, new)`` the whole
identity of an evaluation episode. It is not. The same pair of annotation sets
produces a different delta under a different propagation graph, and the
difference is not small: measured on GOA 220 -> 227, propagating each side under
its own native DAG rather than both under the pivot moves the PK bucket by 21
percent of its annotations and by 51 percent of its proteins, because a term
whose ancestors were rewired between two ontology releases enters the gain set
without anybody having annotated anything.

So the three snapshots that decide the delta become columns and join the key.
The pivot fixes the term universe both sides are read into; the two natives fix
the DAG each side's ancestors are closed under. Two rows differing only in those
are two different measurements and must be able to coexist. Today the second one
is refused, which is how a level comes to be named by fewer fields than it
varies in.

BACKFILL, AND WHY IT STORES THE RESOLVED VALUE. The columns hold the graph that
was actually used, not the override that was requested. A row generated without
an override resolved to each annotation set's own stored snapshot; a row
generated WITH one recorded it in its producing job's payload. So the sets give
the base value and the payload overrides it, in that order, which reproduces
exactly what the operation did. A row that ends with a NULL cannot be given an
identity it never had, so the migration raises instead of guessing.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "d4b8c2f10a37"
down_revision: str | Sequence[str] | None = "e2f7c3a1b904"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OLD_UNIQUE = "uq_evaluation_set_old_new_annotation_set"
_NEW_UNIQUE = "uq_evaluation_set_identity"
_COLUMNS = ("pivot_snapshot_id", "old_native_snapshot_id", "new_native_snapshot_id")

#: What an unset override resolved to: each side's own stored snapshot, and for
#: the pivot the value the producer already wrote into ``stats``.
_FROM_SETS = sa.text("""
    UPDATE evaluation_set es
       SET pivot_snapshot_id = COALESCE(
               NULLIF(es.stats ->> 'pivot_ontology_snapshot_id', '')::uuid,
               newset.ontology_snapshot_id),
           old_native_snapshot_id = oldset.ontology_snapshot_id,
           new_native_snapshot_id = newset.ontology_snapshot_id
      FROM annotation_set oldset, annotation_set newset
     WHERE oldset.id = es.old_annotation_set_id
       AND newset.id = es.new_annotation_set_id
""")

#: An explicit override wins over the set's own snapshot, because it is what ran.
_FROM_JOB = sa.text("""
    UPDATE evaluation_set es
       SET old_native_snapshot_id = COALESCE(
               NULLIF(j.payload ->> 'old_native_snapshot_id', '')::uuid,
               es.old_native_snapshot_id),
           new_native_snapshot_id = COALESCE(
               NULLIF(j.payload ->> 'new_native_snapshot_id', '')::uuid,
               es.new_native_snapshot_id)
      FROM job j
     WHERE j.id = es.job_id
""")

_UNRESOLVED = sa.text("""
    SELECT count(*) FROM evaluation_set
     WHERE pivot_snapshot_id IS NULL
        OR old_native_snapshot_id IS NULL
        OR new_native_snapshot_id IS NULL
""")


def upgrade() -> None:
    for name in _COLUMNS:
        op.add_column(
            "evaluation_set", sa.Column(name, postgresql.UUID(as_uuid=True), nullable=True)
        )
        op.create_foreign_key(
            f"fk_evaluation_set_{name}",
            "evaluation_set",
            "ontology_snapshot",
            [name],
            ["id"],
            ondelete="RESTRICT",
        )

    conn = op.get_bind()
    conn.execute(_FROM_SETS)
    conn.execute(_FROM_JOB)

    unresolved = conn.execute(_UNRESOLVED).scalar_one()
    if unresolved:
        raise RuntimeError(
            f"{unresolved} evaluation_set rows carry no resolvable propagation graph: "
            "neither their producing job nor their annotation sets name one. Guessing "
            "would give a row an identity it never had, so this migration refuses."
        )

    for name in _COLUMNS:
        op.alter_column("evaluation_set", name, nullable=False)

    op.drop_constraint(_OLD_UNIQUE, "evaluation_set", type_="unique")
    op.create_unique_constraint(
        _NEW_UNIQUE,
        "evaluation_set",
        ["old_annotation_set_id", "new_annotation_set_id", *_COLUMNS],
    )


def downgrade() -> None:
    """Narrow the key back.

    Not always possible, and deliberately not forced: once two sets exist for
    one pair under different graphs, restoring the pair-unique constraint would
    have to delete one of them. Postgres raises on the duplicate and the
    downgrade stops, which is the right outcome — dropping a measurement to fit
    an older schema is not a migration, it is data loss.
    """
    op.drop_constraint(_NEW_UNIQUE, "evaluation_set", type_="unique")
    op.create_unique_constraint(
        _OLD_UNIQUE,
        "evaluation_set",
        ["old_annotation_set_id", "new_annotation_set_id"],
    )
    for name in _COLUMNS:
        op.drop_constraint(f"fk_evaluation_set_{name}", "evaluation_set", type_="foreignkey")
        op.drop_column("evaluation_set", name)
