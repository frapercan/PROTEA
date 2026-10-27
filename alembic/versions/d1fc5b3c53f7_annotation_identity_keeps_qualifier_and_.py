"""annotation identity keeps qualifier and reference

Revision ID: d1fc5b3c53f7
Revises: 3cd5f76d282f
Create Date: 2026-10-05 14:55:47.176323

Replaces ``uq_pga_set_protein_term_evidence`` -- a UNIQUE constraint on
``(annotation_set_id, protein_accession, go_term_id, evidence_code)`` -- with a
unique INDEX that also covers ``qualifier`` and ``db_reference``, over
``coalesce(col, '')``.

What was being lost
-------------------

Two GAF rows differing only in ``qualifier`` or ``db_reference`` collapsed into
one, and ``load_goa_annotations`` uses ``on_conflict_do_nothing``: the FIRST row
in file order won, which is not a curatorial decision. Measured on GOA 156,
reliable rows only:

    filas fiables de proteina                      875.990
    tripletas (set, protein, term, evidence)       620.565
    colapsadas                                     255.425   29,2%

    tripletas con mas de una referencia             74.346
    pares (proteina, termino) que perdian el NOT       358

The references are provenance: how many independent papers support the same
assertion. The 358 are truth: ``_reconcile_not_side`` filters
``qualifier LIKE '%NOT%'`` to propagate a negative to descendants and subtract
them, so a dropped NOT stops being subtracted. Negative results are the scarcest
kind of curated evidence there is.

Why an index over coalesce, and not a wider UNIQUE constraint
-------------------------------------------------------------

In Postgres ``NULL`` never conflicts with ``NULL``. In GOA 156 the qualifier is
empty in 871.484 of the 875.990 reliable rows, and the plugin stores empty as
``None``. A constraint including ``qualifier`` would therefore stop deduplicating
in 99,5% of rows, and every retry of an interrupted load would silently duplicate
the corpus. Verified against this Postgres: two rows with the same key and
``NULL`` coexist under ``UNIQUE``.

A unique index over ``coalesce(col, '')`` does deduplicate, and ``on_conflict``
can target it by its ``index_elements``. That keeps the resumability the loader
depends on -- writing into the same set and skipping what a previous attempt
already wrote -- and the polarity at the same time.

Safety
------

The new identity is strictly MORE permissive than the old one: more columns means
fewer collisions, so any set of rows that satisfied the constraint satisfies the
index. The upgrade cannot fail on existing data, and no pre-flight count is
needed for it.

What the upgrade does NOT do is recover anything. Rows already loaded were
collapsed under the narrow identity and stay collapsed; a set loaded before this
migration and one loaded after are not comparable, and the release has to be
re-loaded to carry its qualifiers and references. Run on 2026-10-05 against an
empty ``protein_go_annotation``, which is why the change costs nothing now and
would have cost a reload of the whole series later.

The downgrade CAN fail, and that is correct: once a set carries two rows that
differ only in qualifier or reference, the narrow constraint has no way to admit
both. It aborts with Postgres' own duplicate-key error naming the pair.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "d1fc5b3c53f7"
down_revision: str | Sequence[str] | None = "3cd5f76d282f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OLD = "uq_pga_set_protein_term_evidence"
_NEW = "uq_pga_annotation_identity"

_COLS = (
    "annotation_set_id, "
    "protein_accession, "
    "go_term_id, "
    "coalesce(evidence_code, ''), "
    "coalesce(qualifier, ''), "
    "coalesce(db_reference, '')"
)


def upgrade() -> None:
    op.execute(f"ALTER TABLE protein_go_annotation DROP CONSTRAINT IF EXISTS {_OLD}")
    op.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {_NEW} ON protein_go_annotation ({_COLS})")


def downgrade() -> None:
    op.execute(f"DROP INDEX IF EXISTS {_NEW}")
    op.execute(
        f"ALTER TABLE protein_go_annotation ADD CONSTRAINT {_OLD} "
        "UNIQUE (annotation_set_id, protein_accession, go_term_id, evidence_code)"
    )
