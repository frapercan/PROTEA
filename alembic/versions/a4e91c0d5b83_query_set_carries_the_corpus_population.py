"""query_set carries the corpus population, which is not a window's delta

THE KIND THIS TABLE DID NOT HAVE A NAME FOR. ``source_kind`` closed its
vocabulary with four values on 2026-11-08, and none of them describes the one
set everything else is measured over: the corpus's own proteins. It is not an
upload, it is not a quick annotation, it is not a window's delta, and calling
it ``legacy_undeclared`` would say "we looked and it cannot be established"
about the one population that CAN be established exactly.

WHY IT MATTERS, with the measurement that made it concrete. A vector bank is a
RECIPE applied to a POPULATION, and until today only the recipe had an
identity: ``embedding_config``. On 2026-11-08 the bank of ``f3a7fe16`` covered
683.887 of the corpus's 684.048 sequences, and the 161 missing ones were the
longest in the corpus, the ones with no neighbourhood. That is 0,02 per cent:
invisible in any metric, and nothing in the schema could express it, because
"the population this bank covers" had nowhere to live.

AND THE TWO UNITS ARE DIFFERENT, which is the trap this closes. The corpus is
**792.535 accessions over 684.048 distinct sequences**: production's bank holds
one row per ACCESSION, ordered by ``Protein.accession COLLATE "C"``, while a
vector is per SEQUENCE. So a population is counted in accessions and a coverage
in sequences, and a single number quoted without its unit is how three sessions
spent an afternoon saying "the extended population" about a population that had
not changed by one row.

WHAT THIS MIGRATION DOES NOT DO. It does not record a coverage. A coverage is a
MEASURE of (recipe, population) at a date, not a set, and the researcher's
decision of 2026-11-08 was to declare the population now and to put the
coverage on ``prediction_set`` when predictions resume, so that a run cites
(recipe, population sha, the coverage it saw). Recording it here would freeze a
number that the next release makes stale.

Revision ID: a4e91c0d5b83
Revises: c5e8d1a30b72
"""

from __future__ import annotations

from alembic import op

revision = "a4e91c0d5b83"
down_revision = "c5e8d1a30b72"
branch_labels = None
depends_on = None

SOURCES = (
    "evaluation_targets",
    "fasta_upload",
    "quick_annotation",
    "corpus_population",
    "legacy_undeclared",
)


def upgrade() -> None:
    op.drop_constraint("ck_query_set_source_kind", "query_set", type_="check")
    listed = ", ".join(f"'{v}'" for v in SOURCES)
    op.create_check_constraint(
        "ck_query_set_source_kind", "query_set", f"source_kind IN ({listed})"
    )
    # The four criteria of the set guard, as a constraint rather than as a
    # habit: described, verifiable (a uri with its sha), produced by a job, and
    # NOT derived from a window. The last clause is the one that carries work:
    # a corpus population is not a window's delta, so naming an evaluation set
    # here would be a category error with a foreign key behind it.
    op.create_check_constraint(
        "ck_query_set_corpus_is_declared",
        "query_set",
        "source_kind <> 'corpus_population' OR ("
        "artifact_uri IS NOT NULL AND content_sha256 IS NOT NULL "
        "AND description IS NOT NULL AND job_id IS NOT NULL "
        "AND evaluation_set_id IS NULL)",
    )


def downgrade() -> None:
    op.drop_constraint("ck_query_set_corpus_is_declared", "query_set", type_="check")
    op.drop_constraint("ck_query_set_source_kind", "query_set", type_="check")
    # Back to the four it had. A row already written as 'corpus_population'
    # would refuse this downgrade, which is correct: dropping the value while
    # rows use it would leave a kind the CHECK forbids and the data asserts.
    previous = ", ".join(
        f"'{v}'" for v in ("evaluation_targets", "fasta_upload", "quick_annotation", "legacy_undeclared")
    )
    op.create_check_constraint(
        "ck_query_set_source_kind", "query_set", f"source_kind IN ({previous})"
    )
