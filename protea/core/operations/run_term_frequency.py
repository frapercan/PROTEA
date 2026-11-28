"""Score every target of a window by how often each term is held at t0: term frequency.

Rung 0 of the literature ladder (the CAFA Naive), a native method of the
explainable-units family. For the window named by ``evaluation_set_id`` (t0 -> t1),
which must be a window of LAFA's frame:

1. The photo whose annotation set starts the window, and the population counted:
   the photo's members with an experimental row at the cut (``experimental``), or
   those of them in its Swiss-Prot superset (``reviewed_experimental``, LAFA's).
2. The variant (:mod:`protea.core.term_frequency`): ``lafa_direct``, LAFA's literal,
   on the population's direct rows under t0's own ontology, with democafa's
   denominator and its cap at the 500th largest score of an aspect; or
   ``propagated``, the recipe's Naive, on every protein's closed state at t0 as
   LAFA's frame states it, at the evaluator grid's floor.
3. The same vector for every target: the window's declared target set when it has
   one, otherwise the proteins its truth scores. The rows are compact
   (``prediction_score``, one per target) and every term id is checked against the
   window's t0 snapshot before one is written.
4. The count table behind the vector (every counted term, its proteins, its
   aspect's denominator and its score) goes to the store as the set's
   ``evidence_artifact``; what was counted and written is its ``written_counts``.

It reads labels at t0 and nothing later and fits nothing, so it names no training
window; the same method over the same window again is a no-op.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
from pydantic import field_validator, model_validator
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from protea.core import lafa_frame
from protea.core._cut_state import lafa_cut_state
from protea.core._lafa_frame_data import load_end_ontology
from protea.core.contracts.method import Family, InstanceCut, MethodSpec, Pin
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.evidence_codes import ECO_TO_CODE, EXPERIMENTAL
from protea.core.method_write import (
    SetContext,
    copy_scores,
    ensure_method,
    new_prediction_set,
    population_spelling,
    store_evidence,
    window_targets,
)
from protea.core.query_population import population_of
from protea.core.source_guards import SourceGuardError
from protea.core.term_frequency import (
    GRID_FLOOR,
    LAFA_TERMS_PER_ASPECT,
    DirectRow,
    TermScores,
    lafa_direct,
    propagated,
)
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

Population = Literal["reviewed_experimental", "experimental"]
Variant = Literal["lafa_direct", "propagated"]

#: The thirteen experimental codes and the ECO ids that stand for them, as LAFA's
#: frame reads "experimental" (``retrieve_terms -sgc 'Experimental,IC,TAS'``).
EXPERIMENTAL_CODES: tuple[str, ...] = tuple(
    sorted(EXPERIMENTAL | {eco for eco, code in ECO_TO_CODE.items() if code in EXPERIMENTAL})
)

#: Each population in words, as the recipe and the description say it.
POPULATIONS: dict[str, str] = {
    "reviewed_experimental": (
        "the photo's Swiss-Prot members (its superset) with an experimental row at the cut, "
        "LAFA's Naive and BLAST training set"
    ),
    "experimental": "the photo's members with an experimental row at the cut",
}
LAFA_STATE = (
    "named and closed under t0's own ontology as LAFA's frame states an end, each NOT term "
    "of t0 and everything under it taken out first"
)


class TermFrequencyRefused(SourceGuardError):
    """A term frequency the operation will not write, with the reason."""


class RunTermFrequencyPayload(ProteaPayload, frozen=True):
    """The window to read, the population counted and the variant.

    ``lafa_direct`` is LAFA's literal, which is defined on LAFA's population
    (``reviewed_experimental``); ``propagated`` reads either population.
    """

    evaluation_set_id: str
    population: Population
    variant: Variant

    @field_validator("evaluation_set_id", mode="before")
    @classmethod
    def must_be_a_uuid(cls, v: Any) -> str:
        return str(uuid.UUID(str(v)))

    @model_validator(mode="after")
    def the_literal_counts_lafas_population(self) -> RunTermFrequencyPayload:
        if self.variant == "lafa_direct" and self.population != "reviewed_experimental":
            raise ValueError(
                "lafa_direct is LAFA's Naive, which counts the reviewed_experimental "
                "population; the propagated variant reads the experimental one"
            )
        return self


def resolve_lafa_window(
    session: Session, evaluation_set_id: str, refused: type[Exception]
) -> tuple[EvaluationSet, CutPhoto]:
    """The window, which must be of LAFA's frame, and the full photo that starts it."""
    eset = session.get(EvaluationSet, uuid.UUID(evaluation_set_id))
    if eset is None:
        raise refused(f"evaluation set {evaluation_set_id} not found")
    if eset.truth_profile != lafa_frame.PROFILE:
        raise refused(
            f"window {eset.id} is of the {eset.truth_profile!r} frame; this method states t0 "
            "as LAFA's frame states an end and reads only a window of that frame"
        )
    photo = session.execute(
        select(CutPhoto).where(
            CutPhoto.annotation_set_id == eset.old_annotation_set_id, CutPhoto.kind == "full"
        )
    ).scalar_one_or_none()
    if photo is None:
        raise refused("the window does not start at a loaded full photo")
    return eset, photo


def population_accessions(session: Session, photo: CutPhoto, population: str) -> set[str]:
    """The photo's members of the population: truth members, in the superset when reviewed."""
    rows = session.execute(
        text(
            "SELECT accession FROM cut_photo_member WHERE photo_id = :p AND truth "
            "AND (superset OR NOT :reviewed)"
        ),
        {"p": photo.id, "reviewed": population == "reviewed_experimental"},
    )
    return {str(accession) for (accession,) in rows}


def direct_rows(session: Session, photo: CutPhoto, population: str) -> list[DirectRow]:
    """The population's direct experimental rows at the cut, NOT rows dropped, as stored."""
    rows = session.execute(
        text(
            """
            SELECT pga.protein_accession, gt.go_id, gt.aspect
            FROM protein_go_annotation pga
            JOIN go_term gt ON gt.id = pga.go_term_id
            JOIN cut_photo_member m
              ON m.photo_id = :p AND m.accession = pga.protein_accession
            WHERE pga.annotation_set_id = :s
              AND pga.evidence_code = ANY(:codes)
              AND (pga.qualifier IS NULL OR pga.qualifier NOT LIKE '%NOT%')
              AND m.truth AND (m.superset OR NOT :reviewed)
            """
        ),
        {
            "p": photo.id,
            "s": photo.annotation_set_id,
            "codes": list(EXPERIMENTAL_CODES),
            "reviewed": population == "reviewed_experimental",
        },
    )
    return [DirectRow(str(p), str(g), str(a)) for p, g, a in rows]


def count_terms(
    session: Session, eset: EvaluationSet, photo: CutPhoto, p: RunTermFrequencyPayload
) -> TermScores:
    """The variant's vector over the population, read at t0."""
    if p.variant == "lafa_direct":
        ontology = load_end_ontology(session, eset.old_native_snapshot_id)
        return lafa_direct(direct_rows(session, photo, p.population), ontology)
    members = population_accessions(session, photo, p.population)
    state = lafa_cut_state(session, eset)
    return propagated({a: s for a, s in state.positives.items() if a in members})


def method_code(p: RunTermFrequencyPayload) -> str:
    """The method's code: one per variant and population, so each has its own short name."""
    if p.variant == "lafa_direct":
        return "term-frequency-lafa-direct"
    return f"term-frequency-propagated-{p.population.replace('_', '-')}"


def method_spec(photo: CutPhoto, p: RunTermFrequencyPayload) -> MethodSpec:
    """The method a run is: the variant over its population, at the photo's cut."""
    recipe: dict[str, Any] = {
        "source": "term frequency at t0, the same vector for every target",
        "variant": p.variant,
        "population": f"{p.population}: {POPULATIONS[p.population]}",
    }
    if p.variant == "lafa_direct":
        recipe |= {
            "literal": "LAFA's Naive: democafa naive.py --n_terms 1500 on the unpropagated "
            "train_terms.tsv of retrieve_terms -sgc 'Experimental,IC,TAS'",
            "rows": "direct rows of the thirteen codes, NOT rows dropped without negating, named "
            "under t0's own ontology (alternative id to primary, obsolete term to its "
            "replacement, a term not alive dropped), one per protein, term and aspect",
            "score": "N_t / D_aspect: the proteins with a row on the term over the proteins with "
            "any row in its aspect; the roots' direct counts when all three roots carry rows",
            "cap": f"in an aspect with more than {LAFA_TERMS_PER_ASPECT} terms, every score at "
            f"or below the {LAFA_TERMS_PER_ASPECT}th largest is zero",
        }
    else:
        recipe |= {
            "state": LAFA_STATE,
            "score": "N_t / N_root: the proteins whose state holds the term over the proteins "
            "holding a term in its aspect, roots out",
            "floor": f"every term whose four-decimal rendering is at or above {GRID_FLOOR}, the "
            "first element of the evaluator's grid",
        }
    return MethodSpec(
        family=Family.EXPLAINABLE_UNITS,
        code=method_code(p),
        instance=InstanceCut(photo.cut_release, photo.ontology_snapshot_id),
        recipe=recipe,
        pins=(Pin("photo", photo.manifest_sha256, (f"cut_photo:{photo.id}",)),),
        runtime="native",
        explains=False,
    )


def term_ids(session: Session, snapshot_id: uuid.UUID) -> dict[str, int]:
    """Every GO id of a snapshot to its ``go_term`` row's id."""
    rows = session.execute(
        text("SELECT go_id, id FROM go_term WHERE ontology_snapshot_id = :s"), {"s": snapshot_id}
    )
    return {str(go_id): int(term_id) for go_id, term_id in rows}


def pivot_vector(
    session: Session, eset: EvaluationSet, counted: TermScores
) -> list[tuple[int, float]]:
    """The vector as (go_term_id, score) on the window's pivot, refusing a term it lacks."""
    ids = term_ids(session, eset.pivot_snapshot_id)
    missing = sorted(t for t in counted.scores if t not in ids)
    if missing:
        raise TermFrequencyRefused(
            f"{len(missing)} terms of t0's ontology are not terms of the window's pivot "
            f"(first {missing[:10]}); the set is written on the pivot"
        )
    return [(ids[t], s) for t, s in sorted(counted.scores.items())]


def evidence_table(counted: TermScores, p: RunTermFrequencyPayload) -> pa.Table:
    """The count table: every counted term, its proteins, its aspect's denominator, its score."""
    terms = sorted(counted.proteins)
    table = pa.table(
        {
            "go_id": pa.array(terms, pa.string()),
            "aspect": pa.array([counted.aspect_of[t] for t in terms], pa.string()),
            "proteins": pa.array([counted.proteins[t] for t in terms], pa.int64()),
            "denominator": pa.array(
                [counted.denominators[counted.aspect_of[t]] for t in terms], pa.int64()
            ),
            "score": pa.array([counted.scores.get(t) for t in terms], pa.float64()),
        }
    )
    digest = population_of(counted.counted)["sha256"]
    return table.replace_schema_metadata(
        {"variant": p.variant, "population": p.population, "population_sha256": digest}
    )


def _store() -> Any:
    return get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))


class RunTermFrequencyOperation:
    """Score every target of a window by the frequency of each term at t0 (rung 0)."""

    name = "run_term_frequency"
    description = (
        "Score every target of a window of LAFA's frame by how often each term is held at t0 "
        "(term frequency, the CAFA Naive): LAFA's unpropagated literal or the propagated recipe "
        "variant, the same vector for every target, as a native method's compact prediction set "
        "with its count table (explainable-units family)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        window = str(payload.get("evaluation_set_id", ""))[:8]
        return f"term frequency {payload.get('variant', '')} for window {window}"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = RunTermFrequencyPayload.model_validate(contract_payload(payload))
        eset, photo = resolve_lafa_window(session, p.evaluation_set_id, TermFrequencyRefused)
        method = ensure_method(session, method_spec(photo, p))
        done = session.execute(
            select(PredictionSet).where(
                PredictionSet.method_id == method.id,
                PredictionSet.meta["evaluation_set_id"].astext == str(eset.id),
            )
        ).scalar_one_or_none()
        if done is not None:
            emit(
                "run_term_frequency.already_read", None, {"prediction_set_id": str(done.id)}, "info"
            )
            return OperationResult(result={"prediction_set_id": str(done.id), "skipped": True})
        counted = count_terms(session, eset, photo, p)
        targets, declared = window_targets(session, eset)
        emit(
            "run_term_frequency.inputs",
            None,
            {"targets": len(targets), "terms": len(counted.scores), **counted.record},
            "info",
        )
        return self._write(
            session, (eset, photo, method, counted, targets, declared), p, payload, emit
        )

    @staticmethod
    def _write(
        session: Session,
        run: tuple[Any, ...],
        p: RunTermFrequencyPayload,
        payload: dict[str, Any],
        emit: EmitFn,
    ) -> OperationResult:
        eset, photo, method, counted, targets, declared = run
        vector = pivot_vector(session, eset, counted)
        evidence = store_evidence(
            _store(),
            evidence_table(counted, p),
            method_name=method.short_name,
            evaluation_set_id=eset.id,
        )
        written_counts = {
            "targets": len(targets),
            "terms": len(vector),
            "rows": len(targets) * len(vector),
            "denominators": counted.denominators,
            **counted.record,
        }
        pset = new_prediction_set(
            session,
            method,
            SetContext(
                annotation_set_id=photo.annotation_set_id,
                ontology_snapshot_id=eset.pivot_snapshot_id,
                limit_per_entry=max(len(vector), 1),
                description=(
                    f"Term frequency ({p.variant}) over the {p.population} population at cut "
                    f"{photo.cut_release}: {len(vector):,} terms, the same vector for each of the "
                    f"{len(targets):,} targets of window {eset.id}."
                ),
                meta={
                    "evaluation_set_id": str(eset.id),
                    "evidence_artifact": evidence,
                    "written_counts": written_counts,
                },
                job_id=job_id_from_payload(payload),
                **population_spelling(targets, declared),
            ),
        )
        written = copy_scores(
            session, pset, ((target, term, score) for target in targets for term, score in vector)
        )
        result = {
            "prediction_set_id": str(pset.id),
            "method_id": str(method.id),
            "method": method.short_name,
            "rows": written,
            "terms": len(vector),
            "targets": len(targets),
            "evidence_artifact": evidence,
        }
        emit("run_term_frequency.done", f"{written} rows", result, "info")
        return OperationResult(result=result)


__all__ = [
    "RunTermFrequencyOperation",
    "RunTermFrequencyPayload",
    "TermFrequencyRefused",
    "resolve_lafa_window",
]
