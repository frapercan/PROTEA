"""Optional InterPro2GO BP noisy-OR graft (serve-offline-reconciliation S3).

The offline champion grafts InterPro2GO BP predictions on top of the
reranker output with a noisy-OR combine, worth +0.0179 board-faithful on
LK-BP / PK-BP. This module ports the combine math as a pure, fully tested
function (:func:`noisy_or_graft_bp`) and exposes a gated post-step
(:func:`apply_interpro_bp_graft`) that the predict post-KNN pipeline calls
ONLY when ``serve.interpro_bp_graft`` is enabled (default off, so behaviour
is unchanged until a deploy turns it on).

Scoring formula (matched to the offline graft)
----------------------------------------------
The InterPro BP source reproduces ``interpro2go_test/interpro_lib.py``
exactly:

* Each query protein's CACHED InterPro signatures (``InterProAnnotation``
  rows) are mapped to GO ids through ``InterProGoMapping`` at the active
  ``source_version`` (the EBI InterPro2GO release).
* Each InterPro entry's GO set is propagated up the ontology DAG
  (``is_a`` / ``part_of`` ancestors via :func:`load_parent_map`), then
  restricted to terms present in the active snapshot (drops obsolete /
  out-of-namespace ids, mirroring the offline ``g in ns_map`` filter).
* Per protein the GRADED score of a term ``g`` is
  ``support(g) / n`` where ``support(g)`` is the number of the protein's
  InterPro entries that imply ``g`` (after propagation) and ``n`` is the
  number of the protein's InterPro entries that carry ANY GO mapping
  (all aspects, so the denominator matches the offline build exactly).
* Only biological_process (``aspect == 'P'``) terms are emitted; the
  graded score is the ``prob`` fed to the noisy-OR.

The noisy-OR combine then matches ``apply_and_score.build_blend_rows``:
``final = 1 - (1 - base)(1 - weight * prob)`` per BP candidate, where
``weight`` is the configurable ``serve.interpro_bp_graft_weight`` knob (the
offline tuned per-category BP weights NK 0.05 / LK 0.4 / PK 0.5; serve uses
a single weight because ``/annotate`` does not pre-categorise a query).

Latency + graceful degradation
-------------------------------
``/annotate`` is latency-sensitive, so the source NEVER runs InterProScan
inline. It reads only CACHED ``InterProAnnotation`` signatures: a protein
with no cached signature contributes nothing (its accession is simply
absent from the source), and when no query protein has a cached signature
the whole post-step is a safe no-op that never mutates the reranker output
and never blocks the prediction. The full InterProScan path lives in the
dedicated ``run_interproscan_batch`` operation, which the batch / benchmark
flow runs as a SEPARATE bounded job to populate ``InterProAnnotation``
before predicting; the graft then reuses those cached rows. This keeps the
serving path's added cost to two indexed reads plus an in-memory closure,
never an external binary.
"""

from __future__ import annotations

import uuid
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn

#: GO aspect code for biological_process (``GOTerm.aspect`` wire format).
BP_CODE = "P"


def _noisy_or(base: float, other: float) -> float:
    """Combine two independent probabilities: ``1 - (1 - base)(1 - other)``.

    Inputs are clamped to ``[0, 1]`` so a stray out-of-range score can never
    push the result outside the probability range or flip its sign.
    """
    b = min(1.0, max(0.0, float(base)))
    o = min(1.0, max(0.0, float(other)))
    return 1.0 - (1.0 - b) * (1.0 - o)


def noisy_or_graft_bp(
    prediction_dicts: list[dict[str, Any]],
    interpro_preds: Iterable[Mapping[str, Any]],
    *,
    score_key: str = "reranker_score",
    weight: float = 1.0,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Noisy-OR InterPro BP predictions into the BP candidates (BP-only).

    For each InterPro prediction ``(protein_accession, go_id, go_term_id,
    prob)`` (assumed already filtered to biological_process by the loader)
    the effective contribution is ``weight * prob`` (``weight`` defaults to
    ``1.0`` so callers that pre-multiply, or want the raw probability, are
    unaffected). Then:

    * If a candidate with the same ``(protein_accession, go_id)`` already
      exists, its ``score_key`` becomes ``1 - (1 - base)(1 - weight*prob)``
      where ``base`` is the candidate's current ``score_key`` (0.0 when
      absent).
    * Otherwise a NEW BP candidate row is appended carrying ``weight*prob``
      as its ``score_key`` and ``interpro_graft_present = 1.0``.

    Contributions of ``weight * prob <= 0`` are skipped entirely (no update,
    no new row), matching the offline ``w <= 0 -> comb = base`` short-circuit.

    The function is pure: it mutates / extends ``prediction_dicts`` and
    returns ``(prediction_dicts, stats)``. ``stats`` reports how many
    candidates were updated vs newly added. KNN / reranker candidates are
    never dropped, so this is a strict, monotone enrichment of the BP slice.
    """
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for rec in prediction_dicts:
        go_id = rec.get("go_id")
        if go_id is None:
            continue
        by_key[(rec.get("protein_accession", ""), go_id)] = rec

    updated = 0
    added = 0
    for pred in interpro_preds:
        go_id = pred.get("go_id")
        prob = pred.get("prob")
        if go_id is None or prob is None:
            continue
        eff = weight * float(prob)
        if eff <= 0.0:
            continue
        acc = pred.get("protein_accession", "")
        existing = by_key.get((acc, go_id))
        if existing is not None:
            existing[score_key] = _noisy_or(existing.get(score_key, 0.0), eff)
            existing["interpro_graft_present"] = 1.0
            updated += 1
            continue
        rec = _new_interpro_record(acc, go_id, pred.get("go_term_id"), eff, score_key)
        by_key[(acc, go_id)] = rec
        prediction_dicts.append(rec)
        added += 1

    return prediction_dicts, {"updated": updated, "added": added}


def _new_interpro_record(
    accession: str,
    go_id: str,
    go_term_id: int | None,
    prob: float,
    score_key: str,
) -> dict[str, Any]:
    """Build an InterPro-only BP candidate dict (KNN features zero/default)."""
    rec: dict[str, Any] = {
        "protein_accession": accession,
        "go_term_id": go_term_id,
        "go_id": go_id,
        "aspect": BP_CODE,
        "ref_protein_accession": "interpro",
        "distance": float("nan"),
        "qualifier": "",
        "evidence_code": "IEA",
        "interpro_graft_present": 1.0,
    }
    rec[score_key] = min(1.0, max(0.0, prob))
    return rec


def compute_interpro_bp_preds(
    prot2iprs: Mapping[str, set[str]],
    ipr2go_direct: Mapping[str, set[str]],
    ancestors: Callable[[str], frozenset[str]],
    go_meta: Mapping[str, tuple[int, str]],
) -> list[dict[str, Any]]:
    """Pure graded InterPro2GO BP scorer (no DB), matching the offline build.

    ``prot2iprs`` maps each protein to its cached InterPro accessions;
    ``ipr2go_direct`` maps each InterPro accession to its DIRECT GO ids
    (pre-propagation); ``ancestors`` yields a go_id's DAG closure; ``go_meta``
    maps a go_id present in the active snapshot to ``(go_term_id, aspect)``.

    Returns ``[{protein_accession, go_id, go_term_id, prob}, ...]`` for BP
    terms only, with ``prob = support(g) / n`` (see the module docstring).
    """
    ipr2go_prop = _propagate_ipr_go(ipr2go_direct, ancestors, go_meta)
    out: list[dict[str, Any]] = []
    for acc, iprs in prot2iprs.items():
        iprs_with_go = [ipr for ipr in iprs if ipr in ipr2go_prop]
        n = len(iprs_with_go)
        if n == 0:
            continue
        support: Counter[str] = Counter()
        for ipr in iprs_with_go:
            support.update(ipr2go_prop[ipr])
        for go_id, count in support.items():
            go_term_id, aspect = go_meta[go_id]
            if aspect != BP_CODE:
                continue
            out.append(
                {
                    "protein_accession": acc,
                    "go_id": go_id,
                    "go_term_id": go_term_id,
                    "prob": count / n,
                }
            )
    return out


def _propagate_ipr_go(
    ipr2go_direct: Mapping[str, set[str]],
    ancestors: Callable[[str], frozenset[str]],
    go_meta: Mapping[str, tuple[int, str]],
) -> dict[str, frozenset[str]]:
    """``ipr -> propagated, snapshot-present go_id set`` (keys kept even if empty).

    Every InterPro accession in ``ipr2go_direct`` keeps a key so it counts
    toward the per-protein denominator ``n`` exactly like the offline build,
    even when all of its propagated terms fall outside the active snapshot.
    """
    prop: dict[str, frozenset[str]] = {}
    for ipr, gos in ipr2go_direct.items():
        full: set[str] = set()
        for go_id in gos:
            full.add(go_id)
            full |= ancestors(go_id)
        prop[ipr] = frozenset(g for g in full if g in go_meta)
    return prop


def load_interpro_bp_predictions(
    session: Session,
    snapshot_id: uuid.UUID,
    valid_accessions: list[str],
    *,
    source_version: str | None = None,
) -> list[dict[str, Any]]:
    """Load graded InterPro2GO BP predictions for the query proteins.

    Latency path: reads ONLY cached ``InterProAnnotation`` signatures (never
    runs InterProScan inline). Returns ``[]`` whenever no query protein has a
    cached signature, no InterPro2GO mapping is loaded, or no mapped term
    resolves into the active snapshot, so the graft post-step stays a safe
    no-op until real cached signatures + a loaded mapping are present.
    """
    accessions = [acc for acc in valid_accessions if acc]
    if not accessions:
        return []
    prot2iprs = _load_cached_signatures(session, accessions)
    if not prot2iprs:
        return []
    all_iprs = set().union(*prot2iprs.values())
    sv = source_version or _latest_mapping_version(session)
    if sv is None:
        return []
    ipr2go_direct = _load_ipr_go_direct(session, all_iprs, sv)
    if not ipr2go_direct:
        return []
    ancestors, go_meta = _load_propagation_context(session, snapshot_id, ipr2go_direct)
    if not go_meta:
        return []
    return compute_interpro_bp_preds(prot2iprs, ipr2go_direct, ancestors, go_meta)


def _load_cached_signatures(session: Session, accessions: list[str]) -> dict[str, set[str]]:
    """``protein_accession -> {ipr_accession, ...}`` from cached InterPro rows."""
    from protea.infrastructure.orm.models.annotation.interpro_annotation import (
        InterProAnnotation,
    )

    rows = session.execute(
        select(InterProAnnotation.protein_accession, InterProAnnotation.accession).where(
            InterProAnnotation.protein_accession.in_(accessions)
        )
    ).all()
    prot2iprs: dict[str, set[str]] = {}
    for acc, ipr in rows:
        prot2iprs.setdefault(acc, set()).add(ipr)
    return prot2iprs


def _latest_mapping_version(session: Session) -> str | None:
    """Most recently loaded ``InterProGoMapping.source_version`` (or None)."""
    from protea.infrastructure.orm.models.annotation.interpro_go_mapping import (
        InterProGoMapping,
    )

    return session.execute(
        select(InterProGoMapping.source_version)
        .order_by(InterProGoMapping.created_at.desc())
        .limit(1)
    ).scalar()


def _load_ipr_go_direct(
    session: Session, ipr_accessions: set[str], source_version: str
) -> dict[str, set[str]]:
    """``ipr_accession -> {go_id, ...}`` direct mappings at ``source_version``."""
    from protea.infrastructure.orm.models.annotation.interpro_go_mapping import (
        InterProGoMapping,
    )

    rows = session.execute(
        select(InterProGoMapping.ipr_accession, InterProGoMapping.go_id).where(
            InterProGoMapping.ipr_accession.in_(ipr_accessions),
            InterProGoMapping.source_version == source_version,
        )
    ).all()
    ipr2go: dict[str, set[str]] = {}
    for ipr, go_id in rows:
        ipr2go.setdefault(ipr, set()).add(go_id)
    return ipr2go


def _load_propagation_context(
    session: Session,
    snapshot_id: uuid.UUID,
    ipr2go_direct: Mapping[str, set[str]],
) -> tuple[Callable[[str], frozenset[str]], dict[str, tuple[int, str]]]:
    """Build the DAG closure + ``go_id -> (go_term_id, aspect)`` snapshot map."""
    from protea.core._feature_enricher_helpers import make_ancestor_closure
    from protea.core.feature_enricher import load_parent_map

    ancestors = make_ancestor_closure(load_parent_map(session, snapshot_id))
    candidate_gos: set[str] = set()
    for gos in ipr2go_direct.values():
        for go_id in gos:
            candidate_gos.add(go_id)
            candidate_gos |= ancestors(go_id)
    return ancestors, _resolve_go_meta(session, snapshot_id, candidate_gos)


def _resolve_go_meta(
    session: Session, snapshot_id: uuid.UUID, go_ids: set[str]
) -> dict[str, tuple[int, str]]:
    """``go_id -> (go_term_id, aspect)`` for ids present in the active snapshot."""
    from protea.infrastructure.orm.models.annotation.go_term import GOTerm

    if not go_ids:
        return {}
    rows = session.execute(
        select(GOTerm.go_id, GOTerm.id, GOTerm.aspect).where(
            GOTerm.go_id.in_(go_ids),
            GOTerm.ontology_snapshot_id == snapshot_id,
        )
    ).all()
    return {go_id: (int(gid), aspect or "") for go_id, gid, aspect in rows}


def apply_interpro_bp_graft(
    session: Session,
    snapshot_id: uuid.UUID,
    valid_accessions: list[str],
    prediction_dicts: list[dict[str, Any]],
    emit: EmitFn,
    *,
    score_key: str = "reranker_score",
) -> list[dict[str, Any]]:
    """Gated post-step: graft InterPro2GO BP predictions onto the BP candidates.

    Loads the cached BP predictions (:func:`load_interpro_bp_predictions`),
    applies :func:`noisy_or_graft_bp` with the configured
    ``serve.interpro_bp_graft_weight``, and emits a completion event. Caller
    gates this on ``serve.interpro_bp_graft``; when no cached InterPro
    signature is present the loader yields nothing and ``prediction_dicts`` is
    left unchanged.
    """
    from protea.config.tuning import get_tuning

    serve = get_tuning().serve
    accessions = [acc for acc in valid_accessions if acc]
    interpro_preds = load_interpro_bp_predictions(
        session,
        snapshot_id,
        accessions,
        source_version=serve.interpro_bp_graft_source_version,
    )
    prediction_dicts, stats = noisy_or_graft_bp(
        prediction_dicts,
        interpro_preds,
        score_key=score_key,
        weight=serve.interpro_bp_graft_weight,
    )
    emit(
        "predict_go_terms_batch.interpro_bp_graft_done",
        None,
        {
            "interpro_bp_predictions": len(interpro_preds),
            "candidates_updated": stats["updated"],
            "candidates_added": stats["added"],
        },
        "info",
    )
    return prediction_dicts


__all__ = (
    "BP_CODE",
    "apply_interpro_bp_graft",
    "compute_interpro_bp_preds",
    "load_interpro_bp_predictions",
    "noisy_or_graft_bp",
)
