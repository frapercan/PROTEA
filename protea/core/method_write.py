"""How a native method's results enter the platform (ADR-D64, decision 1).

A method fitted and run inside the platform writes three things, and this module
is the one way to write them:

- its ``method`` row, derived from its spec (``Method.from_spec``): the same spec
  is the same row, so a deterministic refit finds the row it made before;
- a ``prediction_set`` that names the method and no embedding configuration;
- its scores, in one of the two forms the evaluator reads: ``go_prediction`` rows
  whose score is ``method_score`` (the scorer reads them under the
  ``method_score`` formula), with one ``prediction_explanation`` per row when its
  spec says it explains; or, for a method whose rows carry nothing but a score,
  one ``prediction_score`` row per protein with its terms and their scores
  (:func:`copy_scores`), about thirty times smaller.

The rows go in by COPY or by one insert per block of proteins: the set is new, its
id was made here, and every (protein, term) pair is written once, so nothing can
conflict.

TWO MORE THINGS A NATIVE WRITER SAYS. The facts behind its rows (a homology vote's
donors, a term's counts, the channels of an own annotation) go to the artifact
store as one parquet named by its sha256 (:func:`store_evidence`), which the set
records as ``evidence_artifact``. And the proteins it was asked about are spelled
one way (:func:`declared_targets`): a window that declares its targets (a toy
window's sample, ``target_query_set_id``; a full window's ``evaluation_targets``
set) names them by that query set, which the set records as its ``query_set_id``;
a window that declares none is asked about its truth's targets, recorded by their
digest (``query_population``) as before.
"""

from __future__ import annotations

import io
import json
import math
import uuid
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import insert, select, text
from sqlalchemy.orm import Session

from protea.core._artifact_identity import sha256_of_bytes
from protea.core.code_revision import code_revision, dependency_revisions
from protea.core.contracts.method import MethodSpec, TrainingWindow
from protea.core.evaluation import load_evaluation_data_for_set
from protea.core.query_population import population_of
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.embedding.prediction_score import PredictionScore
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.method.method import Method
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry

#: Proteins per insert of compact scores: a block of arrays, not one statement per row.
_SCORE_BLOCK = 500
#: Unknown term ids named in a refusal, at most.
_NAMED = 10


class NativeWriteRefused(ValueError):
    """Rows a native method may not write, or targets it cannot read, with the reason."""


def ensure_method(
    session: Session, spec: MethodSpec, windows: Sequence[TrainingWindow] = ()
) -> Method:
    """The method row of ``spec``, created on first use."""
    row = session.get(Method, spec.method_id)
    if row is None:
        row = Method.from_spec(spec, windows)
        session.add(row)
        session.flush()
    return row


@dataclass(frozen=True)
class SetContext:
    """What a method's prediction set says besides its rows."""

    annotation_set_id: uuid.UUID
    ontology_snapshot_id: uuid.UUID
    limit_per_entry: int
    description: str
    meta: dict[str, Any] = field(default_factory=dict)
    job_id: uuid.UUID | None = None
    #: The proteins the method was asked about (protea.core.query_population):
    #: a window's targets for a method that reads a window.
    population: dict[str, Any] | None = None
    #: The declared query set it was asked about instead (:func:`declared_targets`).
    #: A set names its population one way: by this set, or by the digest above.
    query_set_id: uuid.UUID | None = None


def new_prediction_set(session: Session, method: Method, context: SetContext) -> PredictionSet:
    """A prediction set produced by ``method``, flushed so its rows can name it."""
    if context.query_set_id is not None and context.population:
        raise NativeWriteRefused(
            "a set names the proteins it was asked about once: by its query set or by the "
            "digest of the list it was handed, not both"
        )
    row = PredictionSet(
        id=uuid.uuid4(),
        embedding_config_id=None,
        method_id=method.id,
        annotation_set_id=context.annotation_set_id,
        ontology_snapshot_id=context.ontology_snapshot_id,
        query_set_id=context.query_set_id,
        limit_per_entry=context.limit_per_entry,
        description=context.description,
        meta={
            "algorithm": "native_method",
            "method": method.short_name,
            **context.meta,
            # Written last, so a method's own keys cannot stand in for them: the
            # code and libraries that wrote the rows, as a retrieval's receipt
            # records them, and the proteins asked about. A comparison refuses a
            # set that does not say all three (protea.core.method_seal).
            "code_revision": code_revision(),
            "dependency_revisions": dependency_revisions(),
            **({"query_population": context.population} if context.population else {}),
        },
        job_id=context.job_id,
        producer_git_sha=code_revision(),
    )
    session.add(row)
    session.flush()
    return row


def _raw(session: Session) -> Any:
    return session.connection().connection.driver_connection


def copy_predictions(
    session: Session, pset: PredictionSet, rows: Iterable[tuple[str, int, float]]
) -> int:
    """(accession, go_term_id, score) rows into ``go_prediction``; how many went in."""
    name = str(pset.meta["method"])
    count = 0
    with _raw(session).cursor() as cur:
        with cur.copy(
            "COPY go_prediction (prediction_set_id, protein_accession, go_term_id, "
            "ref_protein_accession, method_score) FROM STDIN"
        ) as copy:
            for accession, term_id, score in rows:
                copy.write_row((pset.id, accession, int(term_id), name, float(score)))
                count += 1
    return count


def copy_explanations(
    session: Session, pset: PredictionSet, rows: Iterable[tuple[str, int, dict[str, Any]]]
) -> int:
    """(accession, go_term_id, payload) rows into ``prediction_explanation``."""
    count = 0
    with _raw(session).cursor() as cur:
        with cur.copy(
            "COPY prediction_explanation (prediction_set_id, accession, go_term_id, payload) "
            "FROM STDIN"
        ) as copy:
            for accession, term_id, payload in rows:
                copy.write_row((pset.id, accession, int(term_id), json.dumps(payload)))
                count += 1
    return count


def _by_protein(rows: Iterable[tuple[str, int, float]]) -> dict[str, dict[int, float]]:
    """The rows grouped by protein, refusing a pair given twice or a score that is no probability."""
    grouped: dict[str, dict[int, float]] = defaultdict(dict)
    for accession, term_id, score in rows:
        terms = grouped[accession]
        term, value = int(term_id), float(score)
        if term in terms:
            raise NativeWriteRefused(
                f"term {term} of {accession} is given twice; a set scores a pair once"
            )
        if not math.isfinite(value) or not 0.0 <= value <= 1.0:
            raise NativeWriteRefused(
                f"term {term} of {accession} scores {value!r}; a native method's score is a "
                "finite number between 0 and 1"
            )
        terms[term] = value
    return grouped


def copy_scores(
    session: Session, pset: PredictionSet, rows: Iterable[tuple[str, int, float]]
) -> int:
    """(accession, go_term_id, score) rows as compact ``prediction_score`` rows; how many pairs.

    ``prediction_score`` holds its terms in an array, which cannot carry a foreign
    key, and a native writer has no candidate rows to take its ids from. So every
    term id is checked against the set's ontology snapshot, the window's t0
    snapshot for a native writer (its pivot, which LAFA's frame reads at t0), and a
    set naming an id the snapshot does not hold is refused, with the ids named,
    before a row is written. One row per protein, its terms ascending.
    """
    grouped = _by_protein(rows)
    asked = sorted({term for terms in grouped.values() for term in terms})
    known = set(
        session.execute(
            text("SELECT id FROM go_term WHERE ontology_snapshot_id = :s AND id = ANY(:ids)"),
            {"s": pset.ontology_snapshot_id, "ids": asked},
        ).scalars()
    )
    unknown = [term for term in asked if term not in known]
    if unknown:
        raise NativeWriteRefused(
            f"{len(unknown)} term ids are not terms of the set's ontology snapshot "
            f"{pset.ontology_snapshot_id} (first {unknown[:_NAMED]}); an array cannot carry a "
            "foreign key, so the writer checks them and writes nothing"
        )
    blocks: list[dict[str, Any]] = [
        {
            "prediction_set_id": pset.id,
            "protein_accession": accession,
            "go_term_ids": sorted(terms),
            "scores": [terms[term] for term in sorted(terms)],
        }
        for accession, terms in sorted(grouped.items())
        if terms
    ]
    for start in range(0, len(blocks), _SCORE_BLOCK):
        session.execute(insert(PredictionScore), blocks[start : start + _SCORE_BLOCK])
    return sum(len(terms) for terms in grouped.values())


class DeclaredTargets(NamedTuple):
    """A window's declared target set: the query set and its proteins, sorted."""

    query_set_id: uuid.UUID
    source_kind: str
    accessions: tuple[str, ...]


def _declared_set(session: Session, eset: EvaluationSet) -> QuerySet | None:
    """The query set that declares the window's targets, or None when it declares none."""
    if eset.target_query_set_id is not None:
        return session.get(QuerySet, eset.target_query_set_id)
    found = (
        session.execute(
            select(QuerySet).where(
                QuerySet.evaluation_set_id == eset.id,
                QuerySet.source_kind == "evaluation_targets",
            )
        )
        .scalars()
        .all()
    )
    if len(found) > 1:
        raise NativeWriteRefused(
            f"window {eset.id} has {len(found)} evaluation_targets sets "
            f"({sorted(str(s.id) for s in found)}); a method must know which population "
            "it is asked about"
        )
    return found[0] if found else None


def declared_targets(session: Session, eset: EvaluationSet) -> DeclaredTargets | None:
    """The window's declared target set, or None for a window that declares none.

    A toy window names its sample (``target_query_set_id``), whose entries are
    checked against the set's own sha256, the digest of its sorted accessions. A
    full window names its ``evaluation_targets`` set, when it has exactly one: the
    one the ladder's driver declares before any apply. A window with neither keeps
    today's reading of its truth's targets, and the caller says so by the digest.
    """
    query_set = _declared_set(session, eset)
    if query_set is None:
        return None
    accessions = sorted(
        set(
            session.execute(
                select(QuerySetEntry.accession).where(QuerySetEntry.query_set_id == query_set.id)
            ).scalars()
        )
    )
    if not accessions:
        raise NativeWriteRefused(f"the target set {query_set.id} of window {eset.id} is empty")
    if (
        query_set.source_kind == "window_sample"
        and population_of(accessions)["sha256"] != query_set.content_sha256
    ):
        raise NativeWriteRefused(
            f"the entries of sample {query_set.id} do not hash to its sha256 "
            f"{query_set.content_sha256}: the set does not hold the proteins it declares"
        )
    return DeclaredTargets(query_set.id, str(query_set.source_kind), tuple(accessions))


def window_targets(
    session: Session, eset: EvaluationSet
) -> tuple[list[str], DeclaredTargets | None]:
    """The proteins a native method reads on ``eset``, and the set that declares them.

    The declared target set when the window has one, without reading the window's
    truth; otherwise the proteins its truth scores in any category, as every
    native writer read them before (and their stored rows do not change).
    """
    declared = declared_targets(session, eset)
    if declared is not None:
        return list(declared.accessions), declared
    truth, _ = load_evaluation_data_for_set(session, eset)
    return sorted(set(truth.nk) | set(truth.lk) | set(truth.pk)), None


def population_spelling(targets: Iterable[str], declared: DeclaredTargets | None) -> dict[str, Any]:
    """The ``SetContext`` fields that name the proteins asked about, spelled one way."""
    if declared is not None:
        return {"query_set_id": declared.query_set_id, "population": None}
    return {"query_set_id": None, "population": population_of(targets)}


def store_evidence(
    store: Any, table: pa.Table, *, method_name: str, evaluation_set_id: uuid.UUID
) -> dict[str, Any]:
    """Put the facts behind a set's rows in the artifact store; the record its meta keeps.

    One parquet, named by the sha256 of its bytes under the method and the window,
    so the same facts written again land on the same key. The record is the
    set's ``evidence_artifact``: the uri, the sha256 and the rows.
    """
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    blob = buffer.getvalue()
    digest = sha256_of_bytes(blob)
    key = f"method-evidence/{method_name}/{evaluation_set_id}/{digest}.parquet"
    return {"uri": str(store.put(key, blob)), "sha256": digest, "rows": table.num_rows}


__all__ = [
    "DeclaredTargets",
    "NativeWriteRefused",
    "SetContext",
    "copy_explanations",
    "copy_predictions",
    "copy_scores",
    "declared_targets",
    "ensure_method",
    "new_prediction_set",
    "population_spelling",
    "store_evidence",
    "window_targets",
]
