"""query_set carries an exam population, which has no evaluation window at all

THE SIXTH KIND, AND THE SECOND ONE THAT IS NOT A WINDOW'S DELTA. ADR-D57 puts
the vocabulary exams inside the platform, and its decision 2 says each exam
population enters as a declared set. There are two of them, frozen once by the
laboratory: 3.335 proteins for the site exam and 4.227 for the domain exam, over
Swiss-Prot 2022_03.

WHY NOT ``corpus_population`` AND WHY NOT A WINDOW. An exam population is not
the corpus: it is a frozen sample with a partition into a selection and a
confirmation half, and the confirmation half is spent once, at the end, by the
platform. And it has no evaluation window: these exams sit on Swiss-Prot
2022_03, which has nothing to do with the cut of a transition window. That is
also why they do not fit ADR-D55's exploration unit, whose
``evaluation_set_id`` is NOT NULL.

WHAT MAKES IT VERIFIABLE, which is the half this migration can enforce. The
artifact a row of this kind points at is a MANIFEST, not the population file: an
exam population is several files (the pairs of accession and md5, the split, the
concepts, the instances or the sites, and the laboratory's own manifest with the
scripts that built them), and a ``query_set`` row has one ``artifact_uri`` and
one ``content_sha256``. So the row names the manifest, the manifest names every
file with its own digest, and the declaring operation re-reads and verifies each
one before any row exists. A digest proves nobody altered the bytes; it never
proves they are the right bytes, so the operation also checks each file's row
count against what the manifest claims about it.

Revision ID: b7c3f91d2a04
Revises: a4e91c0d5b83
"""

from __future__ import annotations

from alembic import op

revision = "b7c3f91d2a04"
down_revision = "a4e91c0d5b83"
branch_labels = None
depends_on = None

SOURCES = (
    "evaluation_targets",
    "fasta_upload",
    "quick_annotation",
    "corpus_population",
    "exam_population",
    "legacy_undeclared",
)

PREVIOUS = tuple(v for v in SOURCES if v != "exam_population")


def upgrade() -> None:
    op.drop_constraint("ck_query_set_source_kind", "query_set", type_="check")
    listed = ", ".join(f"'{v}'" for v in SOURCES)
    op.create_check_constraint(
        "ck_query_set_source_kind", "query_set", f"source_kind IN ({listed})"
    )
    # The same four criteria the corpus kind carries, for the same reason: the
    # set guard as a constraint and not as a habit. Described, verifiable (a uri
    # with its sha), produced by a job, and NOT derived from a window. The last
    # clause is the one that carries work here too, and more of it: an exam
    # population could LOOK like it belongs to a window, because the exams will
    # be compared against windows' results, and it does not.
    op.create_check_constraint(
        "ck_query_set_exam_is_declared",
        "query_set",
        "source_kind <> 'exam_population' OR ("
        "artifact_uri IS NOT NULL AND content_sha256 IS NOT NULL "
        "AND description IS NOT NULL AND job_id IS NOT NULL "
        "AND evaluation_set_id IS NULL)",
    )


def downgrade() -> None:
    op.drop_constraint("ck_query_set_exam_is_declared", "query_set", type_="check")
    op.drop_constraint("ck_query_set_source_kind", "query_set", type_="check")
    # Back to the five it had. A row already written as 'exam_population' makes
    # this downgrade fail, which is correct: dropping the value while rows use
    # it leaves a kind the CHECK forbids and the data asserts.
    listed = ", ".join(f"'{v}'" for v in PREVIOUS)
    op.create_check_constraint(
        "ck_query_set_source_kind", "query_set", f"source_kind IN ({listed})"
    )
