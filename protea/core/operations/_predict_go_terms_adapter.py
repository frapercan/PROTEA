"""Wire from PROTEA's ``predict_go_terms_batch`` operation to
``protea_method.pipeline.predict``.

The orchestrator (``PredictGOTermsBatchOperation.execute``) used to
duplicate the KNN + vote-accumulation logic inline (both the
unified-pool and the aspect-separated paths), then layer PROTEA-only
metadata on top of each prediction record. F2C.5a routed the
unified-pool path through ``protea_method.pipeline.predict``; F2C.5b
adds the aspect-separated wire and drops the legacy receipt code now
that ``protea-method`` 0.3.0 stamps every PROTEA field the orchestrator
needs.

What this adapter still owns
----------------------------

- KNN pre-search to identify unique neighbours (so PROTEA can load
  alignment / taxonomy / annotation inputs for only the refs that
  actually voted on a query). Cheap relative to the DB joins it
  avoids. ``pipeline.predict()`` re-runs the KNN internally; the
  cached numpy arrays make that a no-op even at production sizes.
- Building the per-(query, donor_ref) ``pair_features`` dict the
  v6 enricher consumes downstream, and which ``pipeline.predict()``
  propagates onto each emitted row.

What ``pipeline.predict()`` now owns (delegated)
------------------------------------------------

Vote tally, donor-ref selection, ``vote_count`` / ``k_position`` /
``go_term_frequency`` / ``ref_annotation_density`` /
``neighbor_distance_std`` / ``neighbor_vote_fraction`` /
``neighbor_min_distance`` / ``neighbor_mean_distance``, legacy
``min_distance`` / ``mean_distance`` / ``distance``, ``aspect``,
``qualifier`` / ``evidence_code``, ``prediction_set_id``,
``ref_protein_accession``, pair-feature propagation.

The reranker (``booster``) stays out of this call. PROTEA still
applies it via ``_apply_reranker_if_aligned`` AFTER ancestor
expansion, which keeps the historical ordering bit-exact.

Smell budget: kept under file 800 / class 500 / method 60 ceilings.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, NamedTuple

import numpy as np
from protea_method.pipeline import PredictConfig, PredictDiagnostics
from protea_method.pipeline import predict as pipeline_predict

from protea.core.feature_engineering import compute_alignment, compute_taxonomy
from protea.core.knn_search import search_knn

if TYPE_CHECKING:
    from protea.core.operations.predict_go_terms import PredictGOTermsBatchPayload


class AdapterInputs(NamedTuple):
    """Bundle of data inputs the adapter consumes.

    Keeps the public adapter functions under flake8-bugbear's parameter
    ceiling. ``ref_data`` carries the unified reference pool the
    orchestrator already cached (``embeddings_f32`` /
    ``embeddings_f32_cos`` / ``accessions``); the adapter picks the
    cosine-normalised view when the payload's metric is ``"cosine"``.
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
    alignment / taxonomy columns the v6 enricher
    (``KnnEnrichmentContext``) expects. ``neighbors_by_aspect`` and
    ``go_map_by_aspect`` are re-exported from the diagnostics so the
    v6 shim can reuse them without re-running KNN.
    """

    predictions: list[dict[str, Any]]
    pair_features: dict[tuple[str, str], dict[str, Any]]
    neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]]
    go_map_by_aspect: dict[str, dict[str, list[dict[str, Any]]]]


def _build_predict_config(
    p: PredictGOTermsBatchPayload, *, use_cos: bool, aspect_separated: bool,
    prediction_set_id: uuid.UUID,
) -> PredictConfig:
    """Translate the PROTEA payload into the ``PredictConfig`` shape.

    ``prediction_set_id`` is forwarded via ``PredictConfig`` (the F2C.5a
    contract) so ``pipeline.predict`` stamps it on every row without
    adding to its already-saturated kwarg signature.
    """
    cfg = PredictConfig(
        k=p.limit_per_entry,
        metric=p.metric,
        backend=p.search_backend,
        distance_threshold=p.distance_threshold,
        aspect_separated=aspect_separated,
        compute_v6_features=False,
        compute_taxonomy=False,
        pre_normalized=use_cos,
        extra={"prediction_set_id": str(prediction_set_id)},
    )
    return cfg


def _select_ref_embeddings(ref_data: dict[str, Any], *, use_cos: bool) -> np.ndarray:
    """Return the float32 reference matrix matching the configured metric."""
    return (
        ref_data["embeddings_f32_cos"] if use_cos else ref_data["embeddings_f32"]
    )


def _pre_search_neighbors(
    inputs: AdapterInputs, *, use_cos: bool,
) -> list[list[tuple[str, float]]]:
    """Run KNN once over the unified pool to identify unique neighbours.

    PROTEA needs the neighbour set ahead of ``pipeline.predict()`` so it
    can load sequences / taxonomy / annotations only for the refs that
    actually voted. ``pipeline.predict()`` re-runs the KNN internally
    with the same numpy inputs; the redundant call is a hot-cache
    no-op (~ms at production sizes) compared to the DB round-trips it
    saves.
    """
    p = inputs.p
    return search_knn(
        inputs.query_embeddings,
        _select_ref_embeddings(inputs.ref_data, use_cos=use_cos),
        inputs.ref_data["accessions"],
        k=p.limit_per_entry,
        distance_threshold=p.distance_threshold,
        backend=p.search_backend,
        metric=p.metric,
        pre_normalized=use_cos,
        faiss_index_type=p.faiss_index_type,
        faiss_nlist=p.faiss_nlist,
        faiss_nprobe=p.faiss_nprobe,
        faiss_hnsw_m=p.faiss_hnsw_m,
        faiss_hnsw_ef_search=p.faiss_hnsw_ef_search,
    )


def _build_pair_features_from_neighbors(
    inputs: AdapterInputs,
    neighbors_per_query: list[list[tuple[str, float]]],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Compute alignment / taxonomy features per ``(q_acc, ref_acc)`` pair.

    Walks the neighbour list once and computes each pair's features.
    The dict is later passed to ``pipeline.predict(pair_features=...)``
    which propagates them onto every emitted row whose donor ref
    matches the pair.
    """
    p = inputs.p
    pair_features: dict[tuple[str, str], dict[str, Any]] = {}
    if not p.compute_alignments and not p.compute_taxonomy:
        return pair_features
    for q_acc, hits in zip(inputs.valid_accessions, neighbors_per_query, strict=False):
        for ref_acc, _distance in hits:
            key = (q_acc, ref_acc)
            if key in pair_features:
                continue
            pair_features[key] = _pair_features_for(inputs, q_acc, ref_acc)
    return pair_features


def _pair_features_for(
    inputs: AdapterInputs, q_acc: str, ref_acc: str,
) -> dict[str, Any]:
    """Compute one pair's alignment + taxonomy feature dict."""
    p = inputs.p
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
    return features


def _build_pair_features_from_aspect_neighbors(
    inputs: AdapterInputs,
    neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Pair-feature builder for the aspect-separated path.

    Walks the per-aspect neighbour lists once across aspects so each
    ``(q_acc, ref_acc)`` is computed at most once even when a donor
    ref appears in multiple aspect banks.
    """
    p = inputs.p
    pair_features: dict[tuple[str, str], dict[str, Any]] = {}
    if not p.compute_alignments and not p.compute_taxonomy:
        return pair_features
    for hits_per_query in neighbors_by_aspect.values():
        for q_acc, hits in zip(inputs.valid_accessions, hits_per_query, strict=False):
            for ref_acc, _distance in hits:
                key = (q_acc, ref_acc)
                if key in pair_features:
                    continue
                pair_features[key] = _pair_features_for(inputs, q_acc, ref_acc)
    return pair_features


def _diagnostics_to_neighbors(
    diagnostics: PredictDiagnostics,
) -> dict[str, list[list[tuple[str, float]]]]:
    """Type-shim: ``PredictDiagnostics.neighbors_by_aspect`` already has the
    right shape; this exists so callers don't reach into the dataclass."""
    return diagnostics.neighbors_by_aspect


def call_pipeline_predict(inputs: AdapterInputs) -> AdapterResult:
    """Run ``protea_method.pipeline.predict`` for the unified-pool path.

    Returns the prediction dicts shaped exactly as PROTEA's downstream
    consumers expect (``protea-method`` 0.3.0 stamps every PROTEA
    field, including ``prediction_set_id`` via ``PredictConfig``), plus
    the pair-feature dict the v6 enricher wants and the diagnostics'
    aspect maps for the v6 context.
    """
    p = inputs.p
    use_cos = p.metric == "cosine"
    neighbors_per_query = _pre_search_neighbors(inputs, use_cos=use_cos)
    pair_features = _build_pair_features_from_neighbors(inputs, neighbors_per_query)
    cfg = _build_predict_config(
        p, use_cos=use_cos, aspect_separated=False,
        prediction_set_id=inputs.prediction_set_id,
    )
    predictions, diagnostics = pipeline_predict(
        query_accessions=inputs.valid_accessions,
        query_embeddings=inputs.query_embeddings,
        reference_accessions=inputs.ref_data["accessions"],
        reference_embeddings=_select_ref_embeddings(inputs.ref_data, use_cos=use_cos),
        annotations=inputs.annotations,
        go_id_map=inputs.go_id_map,
        go_aspect_map=inputs.go_aspect_map,
        pair_features=pair_features,
        config=cfg,
        return_diagnostics=True,
    )
    return AdapterResult(
        predictions=predictions,
        pair_features=pair_features,
        neighbors_by_aspect=_diagnostics_to_neighbors(diagnostics),
        go_map_by_aspect=diagnostics.go_map_by_aspect,
    )


def call_pipeline_predict_aspect_separated(
    inputs: AdapterInputs,
    neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]],
) -> AdapterResult:
    """Run ``pipeline.predict(aspect_separated=True)`` for the per-aspect path.

    The caller has already KNN-searched each aspect's pre-filtered
    reference pool and loaded the union of hit-ref annotations (across
    aspects). ``protea-method`` re-runs the partitioned KNN against
    the unified pool we pass — bit-exact with PROTEA's per-aspect
    slicing because both put a ref in aspect ``a``'s bank iff at
    least one of its annotations resolves to aspect ``a``.

    The redundant KNN inside ``pipeline.predict()`` keeps the
    delegation pure (no neighbour injection): in practice the numpy
    arrays are warm in L3 by then, so the cost is dwarfed by the
    annotation / sequence loads the pre-search already saved.
    """
    p = inputs.p
    use_cos = p.metric == "cosine"
    pair_features = _build_pair_features_from_aspect_neighbors(
        inputs, neighbors_by_aspect,
    )
    cfg = _build_predict_config(
        p, use_cos=use_cos, aspect_separated=True,
        prediction_set_id=inputs.prediction_set_id,
    )
    predictions, diagnostics = pipeline_predict(
        query_accessions=inputs.valid_accessions,
        query_embeddings=inputs.query_embeddings,
        reference_accessions=inputs.ref_data["accessions"],
        reference_embeddings=_select_ref_embeddings(inputs.ref_data, use_cos=use_cos),
        annotations=inputs.annotations,
        go_id_map=inputs.go_id_map,
        go_aspect_map=inputs.go_aspect_map,
        pair_features=pair_features,
        config=cfg,
        return_diagnostics=True,
    )
    return AdapterResult(
        predictions=predictions,
        pair_features=pair_features,
        neighbors_by_aspect=_diagnostics_to_neighbors(diagnostics),
        go_map_by_aspect=diagnostics.go_map_by_aspect,
    )
