"""Declare the corpus's own population, once, with its digest.

WHAT WAS MISSING. A vector bank is a RECIPE applied to a POPULATION, and until
this operation only the recipe had an identity: ``embedding_config``. So two
banks built from one recipe over different corpora were the same row, and
"which proteins does this bank cover" had no answer a later reader could check.
Measured on 2026-11-08: the bank of ``f3a7fe16`` held 683.887 of the corpus's
684.048 sequences, and the 161 missing ones were the LONGEST in the corpus, the
ones with no neighbourhood at all. That is 0,02 per cent, invisible in any
metric, and nothing in the schema could express it.

THE TWO UNITS ARE DIFFERENT, and this is the trap the operation closes by
reporting both. The corpus is **792.535 accessions over 684.048 distinct
sequences**: production's bank holds one row per ACCESSION, ordered by
``Protein.accession COLLATE "C"``, while a vector is per SEQUENCE. A population
is therefore counted in accessions and a coverage in sequences, and a number
quoted without its unit is how an afternoon went on the phrase "the extended
population" about a population that had not changed by one row. It had not:
zero sequences were created the day its coverage went from 683.887 to 684.048.

THE ORDER IS PRODUCTION'S OWN. The accessions are serialised sorted by
``COLLATE "C"``, which is codepoint order, because that is the order the
reference pool is built in. The default collation of this database is
``en_US.utf8``, which ignores punctuation on a first pass and puts ``A0A18``
before ``A0A1-9``: a digest taken under it would not be reproducible by anybody
sorting the same accessions in Python.

DECLARED AFTER EXTRACTION, never before. The name carries the digest of the
bytes that exist, so the scope cannot be asserted in a payload and contradicted
by the file.

WHAT IT DOES NOT WRITE. A coverage. A coverage is a MEASURE of (recipe,
population) at a date, not a set: it is reported in this operation's event,
naming the population's sha it was measured against, and it belongs on a
prediction run when predictions resume. Storing it here would freeze a number
that the next release makes stale.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path
from typing import Any, NamedTuple

from pydantic import field_validator
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from protea.core.code_revision import code_revision
from protea.core.contracts.operation import (
    EmitFn,
    Operation,
    OperationResult,
    ProteaPayload,
)
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

#: The rule, in one place, because it goes into the description a reader has to
#: be able to act on. Not a label: a predicate they can run.
POPULATION_RULE = (
    "every protein row carrying a sequence, serialised one accession per line "
    'sorted by accession under COLLATE "C" (codepoint order)'
)


def population_key_for(digest: str) -> str:
    """Storage key for a declared population.

    One spelling, derived and never parsed back out of a uri, for the same
    reason ``targets_key_for`` exists: a later reader that needed these bytes
    would otherwise split the saved locator to get at them.
    """
    return f"corpus-population/corpus-{digest[:12]}.txt"


class _Extracted(NamedTuple):
    """The population as read, before anything is declared about it."""

    accessions: list[str]
    #: Accession to the id of the sequence the PROTEIN row already points at,
    #: taken from the row and never re-hashed from the bytes. Re-hashing is how
    #: the hand-registration route could point a set at sequences with no
    #: embedding, which is measured in ``QuerySet``'s own docstring.
    sequence_of: dict[str, int]


class _Published(NamedTuple):
    """The bytes that exist, and what identifies them."""

    raw: bytes
    digest: str
    key: str
    uri: str


class DeclareCorpusPopulationPayload(ProteaPayload, frozen=True):
    """Nothing to choose about the population. Only what to call the row."""

    output_name: str = "corpus"

    @field_validator("output_name")
    @classmethod
    def _a_name_a_reader_can_use(cls, value: str) -> str:
        """Refuse an empty name, and refuse one that is a path.

        The name is half of how a person finds this row again, so an empty one
        fails the "described" half of the set rule before the description is
        even written. And a slash in it reads as a prefix in a store key, which
        is a second spelling of a locator this operation derives.
        """
        limpio = value.strip()
        if not limpio:
            raise ValueError("output_name cannot be empty: it is how the row is found again")
        if "/" in limpio:
            raise ValueError(
                "output_name cannot contain '/': the storage key is derived, not typed"
            )
        return limpio


def _extract(session: Session) -> _Extracted:
    """The corpus's accessions in production's own order, with their sequences."""
    rows = session.execute(
        select(Protein.accession, Protein.sequence_id)
        .where(Protein.sequence_id.isnot(None))
        .order_by(Protein.accession.collate("C"))
    ).all()
    return _Extracted(
        accessions=[r[0] for r in rows],
        sequence_of={r[0]: int(r[1]) for r in rows},
    )


def _serialise(accessions: list[str]) -> bytes:
    """One accession per line, newline terminated, UTF-8. Nothing else.

    A format with no header and no separator ambiguity, so the digest can be
    reproduced by ``sha256sum`` on a file anybody can write from a sorted list.
    """
    return ("\n".join(accessions) + "\n").encode("utf-8")


def _publish(raw: bytes) -> _Published:
    digest = hashlib.sha256(raw).hexdigest()
    key = population_key_for(digest)
    store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
    return _Published(raw=raw, digest=digest, key=key, uri=store.put(key, raw))


def _refuse_a_second_row(session: Session, digest: str) -> None:
    """Refuse a duplicate, because the unique constraint cannot catch this one.

    ``uq_query_set_window_content`` is on ``(evaluation_set_id,
    content_sha256)``, and this kind has ``evaluation_set_id`` NULL by its own
    CHECK. Two NULLs do not collide in Postgres, so that constraint would admit
    a second row with the SAME bytes. The check lives here instead, and says so,
    rather than being assumed to be somewhere else.
    """
    existing = session.execute(
        select(QuerySet.id).where(
            QuerySet.source_kind == "corpus_population",
            QuerySet.content_sha256 == digest,
        )
    ).first()
    if existing is not None:
        raise ValueError(
            f"this population is already declared as QuerySet {existing[0]} with the same "
            f"sha256 {digest}, so declaring it again would make two rows for one set of "
            "bytes. The unique constraint cannot refuse it because its evaluation_set_id "
            "is NULL and two NULLs do not collide"
        )


def _coverage(session: Session, sequences: int) -> list[dict[str, Any]]:
    """Per recipe, how much of THIS population it covers. A measure, not a set.

    Reported with the population's size beside it so the fraction is readable
    without a second query, and emitted rather than stored: the next release
    loaded makes every one of these numbers stale, and a stale coverage in a
    row is worse than no coverage at all.
    """
    rows = session.execute(
        text(
            "SELECT c.id::text, coalesce(c.display_name, c.model_name), count(e.sequence_id) "
            "  FROM embedding_config c "
            "  LEFT JOIN sequence_embedding e ON e.embedding_config_id = c.id "
            " GROUP BY c.id, c.display_name, c.model_name "
            "HAVING count(e.sequence_id) > 0 "
            " ORDER BY count(e.sequence_id) DESC"
        )
    ).all()
    return [
        {
            "embedding_config_id": r[0],
            "name": r[1],
            "sequences_with_a_vector": int(r[2]),
            "of_population_sequences": sequences,
        }
        for r in rows
    ]


def _declare(
    session: Session,
    p: DeclareCorpusPopulationPayload,
    got: _Extracted,
    pub: _Published,
    job_id: uuid.UUID | None,
) -> QuerySet:
    """The row, built from what was actually written, and its entries.

    ``job_id`` IS AN ARGUMENT AND NOT ASSIGNED AFTERWARDS, which is the bug this
    signature fixes. The first version built the row, flushed it, and set
    ``row.job_id`` after: so the INSERT went in without it and
    ``ck_query_set_corpus_is_declared`` refused the row, because that CHECK
    requires the job for this kind. The constraint caught it on the first real
    run, which is the constraint doing its job and a test failing to do mine.
    """
    sequences = len(set(got.sequence_of.values()))
    row = QuerySet(
        name=f"{p.output_name}-{pub.digest[:12]}",
        source_kind="corpus_population",
        description=(
            f"The corpus population: {len(got.accessions)} accessions over {sequences} "
            f"distinct sequences, as of this job. The rule is {POPULATION_RULE}. "
            "It is NOT a window's delta and NOT an upload: it is what every bank, "
            "every reference pool and every coverage figure is measured over, and its "
            "sha256 is what a later run cites to say WHICH population it saw."
        ),
        artifact_uri=pub.uri,
        content_sha256=pub.digest,
        job_id=job_id,
        code_revision=code_revision(),
    )
    session.add(row)
    session.flush()
    session.add_all(
        [
            QuerySetEntry(query_set_id=row.id, accession=a, sequence_id=got.sequence_of[a])
            for a in got.accessions
        ]
    )
    return row


class DeclareCorpusPopulationOperation(Operation):
    name = "declare_corpus_population"
    description = (
        "Declare the corpus's own population as a QuerySet with its digest: every "
        "protein carrying a sequence, serialised in production's own accession order "
        "and published as bytes anybody can hash again. A bank is a recipe applied to "
        "a population, and until this only the recipe had an identity. Reports the "
        "coverage of every vector recipe against THIS population as a measure, with "
        "its sha beside it, and stores none of them."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        nombre = payload.get("output_name") or "corpus"
        return f"declare the corpus population as '{nombre}', with its digest"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = DeclareCorpusPopulationPayload.model_validate(contract_payload(payload))
        got = _extract(session)
        if not got.accessions:
            raise ValueError(
                "no protein row carries a sequence, so there is no population to declare. "
                "An empty set with a digest is a digest of nothing"
            )
        pub = _publish(_serialise(got.accessions))
        _refuse_a_second_row(session, pub.digest)
        row = _declare(session, p, got, pub, job_id_from_payload(payload))
        sequences = len(set(got.sequence_of.values()))
        result = {
            "query_set_id": str(row.id),
            "name": row.name,
            "accessions": len(got.accessions),
            "sequences": sequences,
            "artifact_uri": pub.uri,
            "content_sha256": pub.digest,
            "rule": POPULATION_RULE,
            "coverage": _coverage(session, sequences),
        }
        emit(
            "corpus_population.declared",
            f"{len(got.accessions)} accessions over {sequences} sequences, sha {pub.digest[:12]}",
            result,
            "info",
        )
        return OperationResult(result=result)
