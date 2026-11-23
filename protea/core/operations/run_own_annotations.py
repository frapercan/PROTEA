"""Read a window's targets from their own non-experimental annotations at t0.

democafa's ``goa_nonexp`` baseline, run inside the platform as a native method of
the explainable-units family. For the window named by ``evaluation_set_id``
(t0 -> t1):

1. The photo whose annotation set starts the window, and the targets the
   window's truth scores.
2. Every target's own rows in t0's annotation set with a non-experimental
   evidence code (any code but the thirteen of the truth and ``ND``, which only
   says there is no data), not NOT-qualified.
3. Those direct terms reconciled exactly as the evaluator reconciles t0's
   experimental side (``protea.core._cut_state.cut_state`` with ``raw``: the
   remap, the binding rule, the closure under t0's graph, the pivot's universe
   and aspects), minus the target's NOT terms at t0 and their descendants, and
   without the three roots. On a window of LAFA's frame, which that path does
   not build, they are stated as the frame states an end
   (:func:`protea.core.lafa_frame.end_truth`): named and closed under t0's own
   ontology, each NOT term of t0 and everything under it taken out of the
   direct terms first. Only t0's NOT rows count, by the window's ``not_codes``:
   the method reads nothing later than t0.
4. Every term scores 1, as democafa's baseline does: the method has no
   parameter. Its explanation names the evidence codes of the direct
   annotations whose closure holds the term.

It reads labels at t0 and nothing later and fits nothing, so it needs no
training window; the same method over the same window again is a no-op.
"""

from __future__ import annotations

import uuid
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping
from typing import Any

from pydantic import field_validator
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from protea.core import lafa_frame
from protea.core._cut_state import cut_state, window_of
from protea.core._evaluation_graph import _bfs_closure, _load_parents_by_go_id
from protea.core._evaluation_groundtruth import load_evaluation_data_for_set
from protea.core._lafa_frame_data import load_end_ontology, load_end_rows
from protea.core.contracts.method import Family, InstanceCut, MethodSpec, Pin
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.count_map.inputs import ROOTS
from protea.core.evaluation import _EXP_CODES
from protea.core.method_write import (
    SetContext,
    copy_explanations,
    copy_predictions,
    ensure_method,
    new_prediction_set,
)
from protea.core.operations.run_count_map import CountMapRefused, term_ids
from protea.core.query_population import population_of
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet

#: Codes that are not knowledge about the protein: no biological data available.
NOT_KNOWLEDGE = ("ND",)
CODE = "own-annotations-nonexp"
#: The same rule on a window of LAFA's frame is a variant at the same cut, and a
#: variant needs a code of its own (``method_short_name``).
LAFA_CODE = "own-annotations-nonexp-lafa"

#: How a run states the targets' terms, by the window's truth profile. The words
#: are part of the method's recipe, so the method of a LAFA window is another
#: method, and the one of every other window keeps the id it always had.
RECONCILED_STATE = "reconciled as the evaluator reconciles t0, minus t0's NOT terms, roots out"
LAFA_STATE = (
    "named and closed under t0's own ontology as LAFA's frame states an end, each NOT term "
    "of t0 and everything under it taken out first, roots out"
)


class RunOwnAnnotationsPayload(ProteaPayload, frozen=True):
    """The window whose targets are read."""

    evaluation_set_id: str

    @field_validator("evaluation_set_id", mode="before")
    @classmethod
    def must_be_a_uuid(cls, v: Any) -> str:
        return str(uuid.UUID(str(v)))


def own_rows(
    session: Session, annotation_set_id: uuid.UUID, targets: Iterable[str]
) -> dict[str, dict[str, set[str]]]:
    """``{target: {native go_id: {codes}}}``: its own non-NOT, non-experimental rows but ND."""
    rows = session.execute(
        text(
            "SELECT pga.protein_accession, gt.go_id, pga.evidence_code "
            "FROM protein_go_annotation pga JOIN go_term gt ON gt.id = pga.go_term_id "
            "WHERE pga.annotation_set_id = :s AND pga.protein_accession = ANY(:t) "
            "AND NOT (pga.evidence_code = ANY(:skip)) "
            "AND (pga.qualifier IS NULL OR pga.qualifier NOT LIKE '%NOT%')"
        ),
        {"s": annotation_set_id, "t": sorted(set(targets)), "skip": [*_EXP_CODES, *NOT_KNOWLEDGE]},
    )
    out: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for protein, go_id, code in rows:
        out[str(protein)][str(go_id)].add(str(code))
    return {p: dict(terms) for p, terms in out.items()}


def term_codes(
    direct: Mapping[str, set[str]], parents: Mapping[str, set[str]]
) -> dict[str, set[str]]:
    """Every term of the direct terms' closure, with the codes of the direct terms that imply it."""
    out: dict[str, set[str]] = defaultdict(set)
    for go_id, codes in direct.items():
        for term in _bfs_closure({go_id}, parents):
            out[term] |= codes
    return dict(out)


def prediction_rows(
    positives: Mapping[str, Mapping[str, set[str]]],
    negatives: Mapping[str, set[str]],
    codes: Mapping[str, Mapping[str, set[str]]],
    ids: Mapping[str, int],
) -> Iterator[tuple[str, int, float, dict[str, Any]]]:
    """(accession, go_term_id, 1.0, explanation) for every reconciled term but roots and denials."""
    for protein in sorted(positives):
        denied = negatives.get(protein, set())
        mine = codes.get(protein, {})
        every = sorted(set().union(*mine.values())) if mine else []
        for aspect in sorted(positives[protein]):
            for go_id in sorted(positives[protein][aspect]):
                if go_id in ROOTS or go_id in denied or go_id not in ids:
                    continue
                why = sorted(mine[go_id]) if go_id in mine else every
                yield protein, ids[go_id], 1.0, {"own": True, "aspect": aspect, "codes": why}


def method_spec(photo: CutPhoto, truth_profile: str | None = None) -> MethodSpec:
    """The method a run is: the rule, at the photo's cut, pinned to the photo it reads."""
    lafa = truth_profile == lafa_frame.PROFILE
    return MethodSpec(
        family=Family.EXPLAINABLE_UNITS,
        code=LAFA_CODE if lafa else CODE,
        instance=InstanceCut(photo.cut_release, photo.ontology_snapshot_id),
        recipe={
            "source": "the target's own annotations at t0 (democafa goa_nonexp)",
            "evidence": "every code but the thirteen of the truth and ND, not NOT-qualified",
            "state": LAFA_STATE if lafa else RECONCILED_STATE,
            "score": 1.0,
        },
        pins=(Pin("photo", photo.manifest_sha256, (f"cut_photo:{photo.id}",)),),
        runtime="native",
        explains=True,
    )


def resolve(session: Session, evaluation_set_id: str) -> tuple[EvaluationSet, CutPhoto]:
    """The window and the photo that starts it, or a refusal."""
    eset = session.get(EvaluationSet, uuid.UUID(evaluation_set_id))
    if eset is None:
        raise CountMapRefused(f"evaluation set {evaluation_set_id} not found")
    photo = session.execute(
        select(CutPhoto).where(CutPhoto.annotation_set_id == eset.old_annotation_set_id)
    ).scalar_one_or_none()
    if photo is None or photo.kind != "full":
        raise CountMapRefused("the window does not start at a loaded full photo")
    return eset, photo


def read_targets(
    session: Session, eset: EvaluationSet
) -> tuple[list[str], list[tuple[str, int, float, dict[str, Any]]]]:
    """The window's targets and every row of their own reconciled non-experimental terms."""
    truth, _ = load_evaluation_data_for_set(session, eset)
    targets = sorted(set(truth.nk) | set(truth.lk) | set(truth.pk))
    direct = own_rows(session, eset.old_annotation_set_id, targets)
    old = session.get(AnnotationSet, eset.old_annotation_set_id)
    if eset.truth_profile == lafa_frame.PROFILE:
        positives, negatives = lafa_positives(session, eset, old.ontology_snapshot_id, direct), {}
    else:
        raw = {p: set(terms) for p, terms in direct.items()}
        state = cut_state(session, window_of(eset), raw=raw)
        positives, negatives = state.positives, state.negatives
    parents = _load_parents_by_go_id(session, old.ontology_snapshot_id)
    codes = {p: term_codes(terms, parents) for p, terms in direct.items()}
    ids = term_ids(session, eset.pivot_snapshot_id)
    rows = list(prediction_rows(positives, negatives, codes, ids))
    return targets, rows


def lafa_positives(
    session: Session,
    eset: EvaluationSet,
    t0_snapshot_id: uuid.UUID,
    direct: Mapping[str, Mapping[str, set[str]]],
) -> dict[str, dict[str, set[str]]]:
    """The targets' own terms as LAFA's frame states t0, from t0's rows alone.

    Named and closed under t0's own ontology, each NOT term of t0 and everything
    under it taken out of the direct terms before the closure. The NOT rows are
    t0's, by the window's ``not_codes``; t1's are later than the method may read.
    """
    t0 = load_end_ontology(session, t0_snapshot_id)
    _, not_rows = load_end_rows(
        session, eset.old_annotation_set_id, eset.not_codes == "experimental"
    )
    negations = {p: not_rows[p] for p in direct if p in not_rows}
    positives = {p: set(terms) for p, terms in direct.items()}
    return lafa_frame.end_truth(t0, positives, negations, Counter(), "t0")


class RunOwnAnnotationsOperation:
    """Read a window's targets from their own non-experimental annotations at t0."""

    name = "run_own_annotations"
    description = (
        "Read a window's targets from their own non-experimental annotations at t0 (democafa's "
        "goa_nonexp baseline), reconciled as the evaluator reconciles t0, as a native method's "
        "prediction set with explanations (explainable-units family)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return f"own non-experimental annotations for window {str(payload.get('evaluation_set_id', ''))[:8]}"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = RunOwnAnnotationsPayload.model_validate(contract_payload(payload))
        eset, photo = resolve(session, p.evaluation_set_id)
        method = ensure_method(session, method_spec(photo, eset.truth_profile))
        done = session.execute(
            select(PredictionSet).where(
                PredictionSet.method_id == method.id,
                PredictionSet.meta["evaluation_set_id"].astext == str(eset.id),
            )
        ).scalar_one_or_none()
        if done is not None:
            emit(
                "run_own_annotations.already_read",
                None,
                {"prediction_set_id": str(done.id)},
                "info",
            )
            return OperationResult(result={"prediction_set_id": str(done.id), "skipped": True})
        targets, rows = read_targets(session, eset)
        covered = len({r[0] for r in rows})
        emit(
            "run_own_annotations.inputs",
            None,
            {"targets": len(targets), "targets_with_a_term": covered, "rows": len(rows)},
            "info",
        )
        pset = new_prediction_set(
            session,
            method,
            SetContext(
                annotation_set_id=photo.annotation_set_id,
                ontology_snapshot_id=eset.pivot_snapshot_id,
                limit_per_entry=max(Counter(r[0] for r in rows).values(), default=1),
                description=(
                    f"The own non-experimental annotations at cut {photo.cut_release} of the "
                    f"{len(targets):,} targets of window {eset.id}: {covered:,} of them carry one."
                ),
                meta={"evaluation_set_id": str(eset.id), "targets_with_a_term": covered},
                job_id=job_id_from_payload(payload),
                population=population_of(targets),
            ),
        )
        written = copy_predictions(session, pset, ((a, t, s) for a, t, s, _ in rows))
        explained = copy_explanations(session, pset, ((a, t, w) for a, t, _, w in rows))
        result = {
            "prediction_set_id": str(pset.id),
            "method_id": str(method.id),
            "method": method.short_name,
            "rows": written,
            "explanations": explained,
            "targets_with_a_term": covered,
        }
        emit("run_own_annotations.done", f"{written} rows", result, "info")
        return OperationResult(result=result)


__all__ = ["RunOwnAnnotationsOperation", "RunOwnAnnotationsPayload"]
