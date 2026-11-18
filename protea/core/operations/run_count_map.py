"""Fit a count map at a window's start and read the window's targets (ADR-D65).

The explainable-units family's first source, run inside the platform (ADR-D64).
For the window named by ``evaluation_set_id`` (t0 -> t1):

1. The photo whose annotation set starts the window, and the lexicon bank named,
   which must cover that photo.
2. Every protein's state at t0 (``protea.core._cut_state``): the window's own
   rules, with t0's NOT terms alone.
3. The donors are the photo's truth members and the targets the proteins the
   window's truth scores, both by their sequence md5 in the photo. A target the
   photo gives no sequence is read from the prior alone, and counted.
4. Per aspect, m is fitted on the donors alone and the targets are read
   (``protea.core.count_map``).
5. The map is registered as ``count_map`` weights fitted on t0's annotation set;
   the method row is derived from its spec (instance at t0; pins: the lexicon,
   the bank's manifest, the map); one prediction set holds its ``method_score``
   rows and one explanation per row.

It reads GO labels at t0 and nothing later, so its instance is the cut and it
names no training window. The same method over the same window again is a
no-op. Every run needs the author's word (ADR-D64, decision 6).
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import numpy as np
from pydantic import PositiveInt, field_validator
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from protea.core._cut_state import CutState, cut_state, window_of
from protea.core._evaluation_groundtruth import load_evaluation_data_for_set
from protea.core.contracts.method import Family, InstanceCut, MethodSpec, Pin
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.count_map.inputs import aspect_problem, columns_of, member_md5s, theme_units
from protea.core.count_map.run import AspectResult, fit_and_read, map_bytes
from protea.core.method_write import (
    SetContext,
    copy_explanations,
    copy_predictions,
    ensure_method,
    new_prediction_set,
)
from protea.core.source_guards import SourceGuardError
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.embedding.lexicon_bank import LexiconBank
from protea.infrastructure.orm.models.embedding.model_weights import ModelWeights
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store
from protea.services import model_weights_service as weights_service


class CountMapRefused(SourceGuardError):
    """A count map the operation will not fit, with the reason."""


class RunCountMapPayload(ProteaPayload, frozen=True):
    """The window to read, the lexicon bank whose cut gives the units, and the reading's depth."""

    evaluation_set_id: str
    lexicon_bank_id: str
    units: Literal["themes"] = "themes"
    top: PositiveInt = 100

    @field_validator("evaluation_set_id", "lexicon_bank_id", mode="before")
    @classmethod
    def must_be_a_uuid(cls, v: Any) -> str:
        return str(uuid.UUID(str(v)))


@dataclass(frozen=True)
class Sources:
    """The rows a run reads from."""

    eset: EvaluationSet
    photo: CutPhoto
    bank: LexiconBank
    lexicon: ModelWeights


def resolve(session: Session, p: RunCountMapPayload) -> Sources:
    """The window, the photo that starts it, the bank and its lexicon, or a refusal."""
    eset = session.get(EvaluationSet, uuid.UUID(p.evaluation_set_id))
    if eset is None:
        raise CountMapRefused(f"evaluation set {p.evaluation_set_id} not found")
    photo = session.execute(
        select(CutPhoto).where(CutPhoto.annotation_set_id == eset.old_annotation_set_id)
    ).scalar_one_or_none()
    if photo is None or photo.kind != "full":
        raise CountMapRefused("the window does not start at a loaded full photo")
    bank = session.get(LexiconBank, uuid.UUID(p.lexicon_bank_id))
    if bank is None:
        raise CountMapRefused(f"lexicon bank {p.lexicon_bank_id} not found")
    if photo.manifest_sha256 not in {x["manifest_sha256"] for x in bank.photos}:
        raise CountMapRefused(
            f"the lexicon bank {bank.short_name} does not cover photo {photo.cut_release}"
        )
    lexicon = session.get(ModelWeights, bank.model_weights_id)
    if lexicon is None:
        raise CountMapRefused("the bank's lexicon is not registered")
    return Sources(eset, photo, bank, lexicon)


@dataclass(frozen=True)
class Gathered:
    """What the cut says: states, donors, targets, sequences and units."""

    state: CutState
    donors: list[str]
    targets: list[str]
    md5_of: dict[str, str]
    units: tuple[dict[str, np.ndarray], int]
    without_sequence: int


def gather(session: Session, store: Any, src: Sources) -> Gathered:
    """Every input of the fit and the reading, read at t0."""
    state = cut_state(session, window_of(src.eset))
    donor_md5 = member_md5s(session, src.photo.id)
    truth, _ = load_evaluation_data_for_set(session, src.eset)
    targets = sorted(set(truth.nk) | set(truth.lk) | set(truth.pk))
    target_md5 = member_md5s(session, src.photo.id, targets)
    md5_of = {**donor_md5, **target_md5}
    units = theme_units(store, list(src.bank.shards), set(md5_of.values()))
    n_units = 1 + max((int(u.max()) for u in units.values() if len(u)), default=0)
    return Gathered(
        state,
        sorted(donor_md5),
        targets,
        md5_of,
        (units, n_units),
        sum(1 for t in targets if t not in target_md5),
    )


def fit_all(data: Gathered, top: int, emit: EmitFn) -> list[AspectResult]:
    """Each aspect fitted on its donors and read on the targets."""
    results = []
    for aspect, columns in columns_of(data.state.aspect).items():
        if not columns.terms:
            emit(
                "run_count_map.aspect_skipped", f"{aspect}: no terms", {"aspect": aspect}, "warning"
            )
            continue
        problem = aspect_problem(
            columns,
            data.state.positives,
            data.state.negatives,
            data.md5_of,
            data.units,
            (data.donors, data.targets),
        )
        if not problem.donors:
            emit(
                "run_count_map.aspect_skipped",
                f"{aspect}: no donors",
                {"aspect": aspect},
                "warning",
            )
            continue
        result = fit_and_read(columns, problem, top)
        summary = {k: v for k, v in result.record.items() if k != "grid"}
        emit(
            "run_count_map.aspect", f"{aspect}: m {result.record['strength']:.3g}", summary, "info"
        )
        results.append(result)
    return results


def register_map(
    session: Session, store: Any, src: Sources, results: list[AspectResult], name: str
) -> ModelWeights:
    """The map as ``count_map`` weights, fitted on t0's annotation set; found again on a rerun."""
    blob = map_bytes(results)
    sha = hashlib.sha256(blob).hexdigest()
    key = weights_service.content_key("count_map", name, sha, ".npz")
    found = session.execute(
        select(ModelWeights).where(ModelWeights.uri == key)
    ).scalar_one_or_none()
    if found is not None:
        return found
    store.put(key, blob)
    info = weights_service.WeightsInfo(
        kind="count_map",
        name=name,
        description=(
            f"The count map of {src.lexicon.name}'s themes at cut {src.photo.cut_release}: per aspect, "
            "the donors that carry each theme and hold each term, the base rates and the strength "
            "fitted on the donors alone (ADR-D65)."
        ),
        meta={
            "aspects": {
                r.columns.aspect: {k: v for k, v in r.record.items() if k != "grid"}
                for r in results
            }
        },
        trained_on_annotation_set_id=src.photo.annotation_set_id,
    )
    return weights_service.register_by_reference(session, store, info, key, sha)


def method_spec(src: Sources, weights: ModelWeights, p: RunCountMapPayload) -> MethodSpec:
    """The method a run is: the recipe, at the photo's cut, pinned to its three files."""
    return MethodSpec(
        family=Family.EXPLAINABLE_UNITS,
        code=f"count-map-{p.units}-{src.lexicon.name}",
        instance=InstanceCut(src.photo.cut_release, src.photo.ontology_snapshot_id),
        recipe={
            "source": "count map (ADR-D65)",
            "units": p.units,
            "donors": "the photo's truth members",
            "state": "the window's rules at t0, with t0's NOT terms only",
            "exclusion": "the donors that share the target's sequence md5",
            "strength": "per aspect, the leave-group-out bits of the donors: 29 points on log m in [1e-3, 1e4], then 30 golden steps",
            "reading": "the best unit per term, floored at the base rate, NOT terms at t0 and their descendants out, roots out",
            "top": p.top,
        },
        pins=(
            Pin("lexicon", src.lexicon.sha256, (f"model_weights:{src.lexicon.id}",)),
            Pin("lexicon-bank", src.bank.manifest_sha256, (f"lexicon_bank:{src.bank.id}",)),
            Pin("count-map", weights.sha256, (f"model_weights:{weights.id}",)),
        ),
        runtime="native",
        explains=True,
    )


def _rows(
    results: list[AspectResult], targets: list[str], term_ids: dict[str, int]
) -> Iterator[tuple[str, int, float, dict[str, Any]]]:
    """(accession, go_term_id, score, explanation) for every row of every aspect."""
    for result in results:
        terms, rows, cmap = result.columns.terms, result.rows, result.cmap
        for i in range(len(rows)):
            go_id = terms[int(rows.term[i])]
            pi = float(cmap.base_rate[int(rows.term[i])])
            unit = int(rows.unit[i])
            why: dict[str, Any] = {"prior": True, "base_rate": pi}
            if unit >= 0:
                why = {
                    "unit": unit,
                    "k": float(rows.k[i]),
                    "n": float(rows.n[i]),
                    "strength": cmap.strength,
                    "base_rate": pi,
                }
            yield targets[int(rows.target[i])], term_ids[go_id], float(rows.score[i]), why


def term_ids(session: Session, snapshot_id: uuid.UUID) -> dict[str, int]:
    rows = session.execute(
        text("SELECT go_id, id FROM go_term WHERE ontology_snapshot_id = :s"), {"s": snapshot_id}
    )
    return {str(g): int(i) for g, i in rows}


class RunCountMapOperation:
    """Fit a count map at a window's start and read its targets (ADR-D65)."""

    name = "run_count_map"
    description = (
        "Fit the explainable-units family's count map at a window's start (donors, states and "
        "lexicon units at t0) and write its reading of the window's targets as a method's "
        "prediction set with explanations (ADR-D64, ADR-D65)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return f"count map of {payload.get('units', 'themes')} for window {str(payload.get('evaluation_set_id', ''))[:8]}"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = RunCountMapPayload.model_validate(contract_payload(payload))
        src = resolve(session, p)
        store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
        data = gather(session, store, src)
        emit(
            "run_count_map.inputs",
            None,
            {
                "donors": len(data.donors),
                "targets": len(data.targets),
                "targets_without_a_sequence": data.without_sequence,
            },
            "info",
        )
        results = fit_all(data, p.top, emit)
        name = f"count-map-{p.units}-{src.lexicon.name}-{src.photo.cut_release}"
        weights = register_map(session, store, src, results, name)
        method = ensure_method(session, method_spec(src, weights, p))
        done = session.execute(
            select(PredictionSet).where(
                PredictionSet.method_id == method.id,
                PredictionSet.meta["evaluation_set_id"].astext == str(src.eset.id),
            )
        ).scalar_one_or_none()
        if done is not None:
            emit("run_count_map.already_read", None, {"prediction_set_id": str(done.id)}, "info")
            return OperationResult(result={"prediction_set_id": str(done.id), "skipped": True})
        return self._write(session, (src, data, results, method), p, payload, emit)

    @staticmethod
    def _write(
        session: Session,
        run: tuple[Any, ...],
        p: RunCountMapPayload,
        payload: dict[str, Any],
        emit: EmitFn,
    ) -> OperationResult:
        src, data, results, method = run
        pset = new_prediction_set(
            session,
            method,
            SetContext(
                annotation_set_id=src.photo.annotation_set_id,
                ontology_snapshot_id=src.eset.pivot_snapshot_id,
                limit_per_entry=3 * p.top,
                description=(
                    f"The count map of {src.lexicon.name}'s {p.units} at cut {src.photo.cut_release}, "
                    f"read on the {len(data.targets):,} targets of window {src.eset.id}."
                ),
                meta={
                    "evaluation_set_id": str(src.eset.id),
                    "targets_without_a_sequence": data.without_sequence,
                    "aspects": {
                        r.columns.aspect: {k: v for k, v in r.record.items() if k != "grid"}
                        for r in results
                    },
                },
                job_id=job_id_from_payload(payload),
            ),
        )
        ids = term_ids(session, src.eset.pivot_snapshot_id)
        written = copy_predictions(
            session, pset, ((a, t, s) for a, t, s, _ in _rows(results, data.targets, ids))
        )
        explained = copy_explanations(
            session, pset, ((a, t, w) for a, t, _, w in _rows(results, data.targets, ids))
        )
        result = {
            "prediction_set_id": str(pset.id),
            "method_id": str(method.id),
            "method": method.short_name,
            "rows": written,
            "explanations": explained,
        }
        emit("run_count_map.done", f"{written} rows", result, "info")
        return OperationResult(result=result)


__all__ = ["CountMapRefused", "RunCountMapOperation", "RunCountMapPayload"]
