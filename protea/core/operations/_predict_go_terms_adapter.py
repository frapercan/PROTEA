"""Wire from PROTEA's ``predict_go_terms_batch`` operation to
``protea_method.pipeline.predict``.

The orchestrator (``PredictGOTermsBatchOperation.execute``) used to
duplicate the KNN + vote-accumulation logic inline, then layer
PROTEA-only metadata (``prediction_set_id``, ``ref_protein_accession``,
``qualifier``, ``evidence_code``, ``k_position``,
``go_term_frequency``, ``ref_annotation_density``,
``neighbor_distance_std``, ``neighbor_vote_fraction``) on top of each
prediction record. F2C.5 collapses that inline duplicate into a call to
the pure ``protea_method.pipeline.predict()`` and lets this adapter
re-derive the legacy fields from the ``PredictDiagnostics`` exposed in
``protea-method v0.2.0``.

Design choices
--------------

- ``predict()`` is called with ``compute_v6_features=False``. PROTEA's
  ``enrich_v6_features`` shim still owns the v6 step downstream of the
  adapter (existing behaviour) so per-aspect ``pair_features`` and
  ``pca_state`` flow through PROTEA's already-tested path.
- ``return_diagnostics=True`` exposes ``neighbors_by_aspect`` and
  ``go_map_by_aspect``; both are walked once each to build the per-pair
  alignment / taxonomy bundle the orchestrator forwards to v6, and a
  second pass folds the legacy reranker aggregates onto each prediction.
- The reranker (``booster``) stays out of this call. PROTEA still applies
  it via ``_apply_reranker_if_aligned`` AFTER ancestor expansion, which
  keeps the historical ordering bit-exact.

Smell budget: kept under file 800 / class 500 / method 60 ceilings.
"""

from __future__ import annotations

import math
import uuid
from typing import TYPE_CHECKING, Any, NamedTuple

import numpy as np
from protea_method.pipeline import PredictConfig, PredictDiagnostics
from protea_method.pipeline import predict as pipeline_predict

from protea.core.feature_engineering import compute_alignment, compute_taxonomy

if TYPE_CHECKING:
    from protea.core.operations.predict_go_terms import PredictGOTermsBatchPayload


class AdapterInputs(NamedTuple):
    """Bundle of data inputs the adapter consumes.

    Keeps :func:`call_pipeline_predict` under flake8-bugbear's parameter
    ceiling. ``ref_data`` carries the unified reference pool the
    orchestrator already cached (``embeddings_f32`` / ``embeddings_f32_cos``
    / ``accessions``); the adapter picks the cosine-normalised view when
    the payload's metric is ``"cosine"``.
    """

    p: PredictGOTermsBatchPayload
    valid_accessions: list[str]
    query_embeddings: np.ndarray
    ref_data: dict[str, Any]
    annotations: dict[str, list[dict[str, Any]]]
    go_id_map: dict[int, str]
    go_aspect_map: dict[int, str]
    prediction_set_id: uuid.UUID
    ref_sequences: dict[str, str]
    query_sequences: dict[str, str]
    ref_tax_ids: dict[str, int | None]
    query_tax_ids: dict[str, int | None]


class AdapterResult(NamedTuple):
    """Output bundle: enriched predictions + ``pair_features`` for v6.

    ``pair_features`` is keyed by ``(q_acc, ref_acc)`` and carries the
    alignment / taxonomy columns the v6 enricher (``KnnEnrichmentContext``)
    expects. ``neighbors_by_aspect`` and ``go_map_by_aspect`` are
    re-exported from the diagnostics so the v6 shim can reuse them
    without re-running KNN.
    """

    predictions: list[dict[str, Any]]
    pair_features: dict[tuple[str, str], dict[str, Any]]
    neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]]
    go_map_by_aspect: dict[str, dict[str, list[dict[str, Any]]]]


def _build_predict_config(p: PredictGOTermsBatchPayload, *, use_cos: bool) -> PredictConfig:
    """Translate the PROTEA payload into the ``PredictConfig`` shape."""
    return PredictConfig(
        k=p.limit_per_entry,
        metric=p.metric,
        backend=p.search_backend,
        distance_threshold=p.distance_threshold,
        aspect_separated=False,
        compute_v6_features=False,
        compute_taxonomy=False,
        pre_normalized=use_cos,
    )


def _build_pair_features(
    inputs: AdapterInputs,
    diagnostics: PredictDiagnostics,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Compute alignment / taxonomy features per ``(q_acc, ref_acc)`` pair.

    PROTEA's legacy ``_predict_batch`` populated this dict for every
    neighbour visited (not just voters) so v6 enrichment could read it
    by pair key. The adapter mirrors that semantic by walking
    ``diagnostics.neighbors_by_aspect`` once.
    """
    p = inputs.p
    pair_features: dict[tuple[str, str], dict[str, Any]] = {}
    if not p.compute_alignments and not p.compute_taxonomy:
        return pair_features
    for hits_per_query in diagnostics.neighbors_by_aspect.values():
        for q_acc, hits in zip(inputs.valid_accessions, hits_per_query, strict=False):
            for ref_acc, _distance in hits:
                key = (q_acc, ref_acc)
                if key in pair_features:
                    continue
                features: dict[str, Any] = {}
                if p.compute_alignments:
                    q_seq = inputs.query_sequences.get(q_acc, "")
                    r_seq = inputs.ref_sequences.get(ref_acc, "")
                    if q_seq and r_seq:
                        features.update(compute_alignment(q_seq, r_seq))
                if p.compute_taxonomy:
                    q_tid = inputs.query_tax_ids.get(q_acc)
                    r_tid = inputs.ref_tax_ids.get(ref_acc)
                    features.update(compute_taxonomy(q_tid, r_tid))
                    features["query_taxonomy_id"] = q_tid
                    features["ref_taxonomy_id"] = r_tid
                pair_features[key] = features
    return pair_features


class _LegacyReceipts(NamedTuple):
    """Per-(query, gtid) lookup tables built once from diagnostics.

    Folds the 7 PROTEA-only reranker aggregates the booster ingested at
    training time. Kept tuple-shaped so the per-prediction layering loop
    stays a tight gather.
    """

    first_voter: dict[tuple[str, int], tuple[str, float, int, dict[str, Any]]]
    rr_distance_std: dict[str, float | None]
    rr_vote_min_d: dict[tuple[str, int], float]
    rr_vote_sum_d: dict[tuple[str, int], float]
    rr_vote_count: dict[tuple[str, int], int]
    go_term_freq_by_aspect: dict[str, dict[int, int]]
    ref_ann_density: dict[str, int]


def _compute_per_ref_aggregates(
    diagnostics: PredictDiagnostics,
) -> tuple[dict[str, dict[int, int]], dict[str, int]]:
    """Per-aspect ``go_term -> count`` map + per-ref annotation density."""
    go_term_freq_by_aspect: dict[str, dict[int, int]] = {}
    ref_ann_density: dict[str, int] = {}
    for aspect_key, go_map in diagnostics.go_map_by_aspect.items():
        per_aspect_freq: dict[int, int] = {}
        for ref_acc, anns in go_map.items():
            ref_ann_density[ref_acc] = len(anns)
            for ann in anns:
                gtid = int(ann["go_term_id"])
                per_aspect_freq[gtid] = per_aspect_freq.get(gtid, 0) + 1
        go_term_freq_by_aspect[aspect_key] = per_aspect_freq
    return go_term_freq_by_aspect, ref_ann_density


def _compute_per_query_receipts(
    inputs: AdapterInputs,
    diagnostics: PredictDiagnostics,
) -> tuple[
    dict[tuple[str, int], tuple[str, float, int, dict[str, Any]]],
    dict[str, float | None],
    dict[tuple[str, int], float],
    dict[tuple[str, int], float],
    dict[tuple[str, int], int],
]:
    """Walk neighbours per query to derive vote-side aggregates."""
    first_voter: dict[tuple[str, int], tuple[str, float, int, dict[str, Any]]] = {}
    rr_distance_std: dict[str, float | None] = {}
    rr_vote_min_d: dict[tuple[str, int], float] = {}
    rr_vote_sum_d: dict[tuple[str, int], float] = {}
    rr_vote_count: dict[tuple[str, int], int] = {}
    unified_go_map = diagnostics.go_map_by_aspect.get("", {})
    for hits_per_query in diagnostics.neighbors_by_aspect.values():
        for q_idx, q_acc in enumerate(inputs.valid_accessions):
            if q_idx >= len(hits_per_query):
                continue
            top_refs = hits_per_query[q_idx]
            if top_refs:
                rr_distance_std[q_acc] = (
                    float(np.std([d for _, d in top_refs])) if len(top_refs) > 1 else 0.0
                )
            for k_pos, (ref_acc, dist) in enumerate(top_refs, 1):
                for ann in unified_go_map.get(ref_acc, []):
                    pair = (q_acc, int(ann["go_term_id"]))
                    rr_vote_count[pair] = rr_vote_count.get(pair, 0) + 1
                    if dist < rr_vote_min_d.get(pair, math.inf):
                        rr_vote_min_d[pair] = dist
                    rr_vote_sum_d[pair] = rr_vote_sum_d.get(pair, 0.0) + dist
                    if pair not in first_voter:
                        first_voter[pair] = (ref_acc, dist, k_pos, ann)
    return first_voter, rr_distance_std, rr_vote_min_d, rr_vote_sum_d, rr_vote_count


def _compute_legacy_receipts(
    inputs: AdapterInputs,
    diagnostics: PredictDiagnostics,
) -> _LegacyReceipts:
    """Materialise the per-(query, gtid) lookups the booster expects."""
    go_term_freq_by_aspect, ref_ann_density = _compute_per_ref_aggregates(diagnostics)
    first_voter, rr_distance_std, rr_vote_min_d, rr_vote_sum_d, rr_vote_count = (
        _compute_per_query_receipts(inputs, diagnostics)
    )
    return _LegacyReceipts(
        first_voter=first_voter,
        rr_distance_std=rr_distance_std,
        rr_vote_min_d=rr_vote_min_d,
        rr_vote_sum_d=rr_vote_sum_d,
        rr_vote_count=rr_vote_count,
        go_term_freq_by_aspect=go_term_freq_by_aspect,
        ref_ann_density=ref_ann_density,
    )


_PAIR_FEATURE_KEYS: tuple[str, ...] = (
    "identity_nw",
    "similarity_nw",
    "alignment_score_nw",
    "gaps_pct_nw",
    "alignment_length_nw",
    "identity_sw",
    "similarity_sw",
    "alignment_score_sw",
    "gaps_pct_sw",
    "alignment_length_sw",
    "length_query",
    "length_ref",
    "query_taxonomy_id",
    "ref_taxonomy_id",
    "taxonomic_lca",
    "taxonomic_distance",
    "taxonomic_common_ancestors",
    "taxonomic_relation",
)


def _layer_pair_features(
    pred: dict[str, Any],
    pair_features: dict[tuple[str, str], dict[str, Any]],
    pair_key: tuple[str, str],
) -> None:
    features = pair_features.get(pair_key, {})
    for key in _PAIR_FEATURE_KEYS:
        val = features.get(key)
        if val is not None:
            pred[key] = val


def _layer_legacy_fields(
    pred: dict[str, Any],
    receipts: _LegacyReceipts,
    *,
    p: PredictGOTermsBatchPayload,
    pair_features: dict[tuple[str, str], dict[str, Any]],
    prediction_set_id: uuid.UUID,
) -> None:
    """Mutate ``pred`` in place with PROTEA-only metadata + legacy reranker fields.

    The base ``pred`` carries ``protein_accession``, ``go_term_id``,
    ``vote_count``, ``min_distance``, ``mean_distance``, ``distance``,
    ``aspect`` from ``protea_method.pipeline._accumulate_votes``. This
    helper enriches it with the columns PROTEA's downstream consumers
    (booster training records, score export, GOPrediction storage) read.
    """
    q_acc = pred["protein_accession"]
    gtid = int(pred["go_term_id"])
    pair = (q_acc, gtid)
    voter = receipts.first_voter.get(pair)
    pred["prediction_set_id"] = str(prediction_set_id)
    if voter is not None:
        ref_acc, voter_distance, k_pos, ann = voter
        pred["ref_protein_accession"] = ref_acc
        pred["distance"] = voter_distance
        if ann.get("qualifier"):
            pred["qualifier"] = ann["qualifier"]
        if ann.get("evidence_code"):
            pred["evidence_code"] = ann["evidence_code"]
        _layer_pair_features(pred, pair_features, (q_acc, ref_acc))
    if not p.compute_reranker_features:
        return
    pred["k_position"] = voter[2] if voter else 1
    aspect_key = pred.get("aspect", "")
    freq_map = receipts.go_term_freq_by_aspect.get(aspect_key) or next(
        iter(receipts.go_term_freq_by_aspect.values()),
        {},
    )
    pred["go_term_frequency"] = freq_map.get(gtid, 0)
    if voter is not None:
        pred["ref_annotation_density"] = receipts.ref_ann_density.get(voter[0], 0)
    pred["neighbor_distance_std"] = receipts.rr_distance_std.get(q_acc)
    vc = receipts.rr_vote_count.get(pair, int(pred.get("vote_count", 0)))
    pred["vote_count"] = vc
    pred["neighbor_vote_fraction"] = (
        vc / p.limit_per_entry if p.limit_per_entry else None
    )
    min_d = receipts.rr_vote_min_d.get(pair)
    if min_d is not None:
        pred["neighbor_min_distance"] = min_d
    sum_d = receipts.rr_vote_sum_d.get(pair)
    if sum_d is not None and vc > 0:
        pred["neighbor_mean_distance"] = sum_d / vc


def call_pipeline_predict(inputs: AdapterInputs) -> AdapterResult:
    """Run ``protea_method.pipeline.predict`` with PROTEA-side enrichment.

    Drives the unified KNN path. Returns the prediction dicts shaped
    exactly like the legacy ``_predict_batch`` output (so downstream
    code, including v6 enrichment, ancestor expansion, reranker apply,
    and storage, sees no behaviour change), plus the pair-feature dict
    the v6 enricher wants and the diagnostics' aspect maps for the v6
    context.
    """
    p = inputs.p
    use_cos = p.metric == "cosine"
    ref_embeddings = (
        inputs.ref_data["embeddings_f32_cos"]
        if use_cos
        else inputs.ref_data["embeddings_f32"]
    )
    cfg = _build_predict_config(p, use_cos=use_cos)
    out = pipeline_predict(
        query_accessions=inputs.valid_accessions,
        query_embeddings=inputs.query_embeddings,
        reference_accessions=inputs.ref_data["accessions"],
        reference_embeddings=ref_embeddings,
        annotations=inputs.annotations,
        go_id_map=inputs.go_id_map,
        go_aspect_map=inputs.go_aspect_map,
        config=cfg,
        return_diagnostics=True,
    )
    predictions, diagnostics = out
    pair_features = _build_pair_features(inputs, diagnostics)
    receipts = _compute_legacy_receipts(inputs, diagnostics)
    for pred in predictions:
        _layer_legacy_fields(
            pred,
            receipts,
            p=p,
            pair_features=pair_features,
            prediction_set_id=inputs.prediction_set_id,
        )
    return AdapterResult(
        predictions=predictions,
        pair_features=pair_features,
        neighbors_by_aspect=diagnostics.neighbors_by_aspect,
        go_map_by_aspect=diagnostics.go_map_by_aspect,
    )
