"""How a native method's results enter the platform (ADR-D64, decision 1).

A method fitted and run inside the platform writes three things, and this module
is the one way to write them:

- its ``method`` row, derived from its spec (``Method.from_spec``): the same spec
  is the same row, so a deterministic refit finds the row it made before;
- a ``prediction_set`` that names the method and no embedding configuration;
- its ``go_prediction`` rows, whose score is ``method_score`` (the scorer reads
  them under the ``method_score`` formula), and, when its spec says it explains,
  one ``prediction_explanation`` per row.

The rows go in by COPY: the set is new, its id was made here, and every (protein,
term) pair is written once, so nothing can conflict.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.orm import Session

from protea.core.code_revision import code_revision, dependency_revisions
from protea.core.contracts.method import MethodSpec, TrainingWindow
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.method.method import Method


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
    #: The query set that declares those proteins, when one does: the set then names
    #: its population by it, as a predicted set does, and records no digest.
    query_set_id: uuid.UUID | None = None


def new_prediction_set(session: Session, method: Method, context: SetContext) -> PredictionSet:
    """A prediction set produced by ``method``, flushed so its rows can name it."""
    row = PredictionSet(
        id=uuid.uuid4(),
        embedding_config_id=None,
        method_id=method.id,
        annotation_set_id=context.annotation_set_id,
        ontology_snapshot_id=context.ontology_snapshot_id,
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
        query_set_id=context.query_set_id,
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


__all__ = [
    "SetContext",
    "copy_explanations",
    "copy_predictions",
    "ensure_method",
    "new_prediction_set",
]
