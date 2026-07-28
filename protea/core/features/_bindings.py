"""Per-feature compute bindings for the canonical FeatureRegistry (T2B.2).

T2B.1 populated :mod:`protea.core.features.registry` with placeholder
``compute`` callables that raise :class:`NotImplementedError`. This
module wires each feature in :data:`protea_contracts.ALL_FEATURES` to
the legacy producer function it is filled by in the current code path
and calls :meth:`CanonicalFeatureRegistry.bind_compute` once at
import time to replace the placeholders.

Mapping (family group -> producer):

* KNN-derived columns (``distance``, ``vote_count``, ``k_position``,
  ``go_term_frequency``, ``ref_annotation_density``,
  ``neighbor_distance_std``, ``neighbor_vote_fraction``,
  ``neighbor_min_distance``, ``neighbor_mean_distance``) come from
  the per-aspect record builder driven by
  :class:`protea.core._knn_transfer_runner._KnnTransferRunner`. They
  are bound to :func:`_knn_record_producer`, a marker callable that
  carries a reference to the runner on its ``__protea_producer__``
  attribute.
* NW / SW alignment columns and ``length_query`` / ``length_ref``
  come from :func:`protea.core.feature_engineering.compute_alignment`
  (called per (query, ref) pair in ``_knn_transfer_runner``).
* Taxonomy pair columns (``taxonomic_distance``,
  ``taxonomic_common_ancestors``, ``taxonomic_relation``) come from
  :func:`protea.core.feature_engineering.compute_taxonomy`.
* The v6 enrichment columns (``tax_voters_*``, ``go_term_frequency``
  share, ``anc2vec_*``, ``emb_pca_query_*``) come from
  :func:`protea.core.feature_enricher.enrich_v6_features`.
* Lineage columns (``lineage_is_ancestor_of_known``,
  ``lineage_is_descendant_of_known``,
  ``lineage_ancestor_of_count``, ``lineage_descendant_of_count``)
  come from :func:`protea_method.lineage.compute_lineage_features`.
  Added in T-RES.1 to consume the lineage feature family registered
  by ``protea-contracts`` v0.3.0.
* Categorical metadata (``qualifier``, ``evidence_code``, ``aspect``)
  is sourced from annotation rows during record construction; the
  marker :func:`_annotation_metadata_producer` carries that
  intent.

The bound compute callables are not invoked by ``parquet_export``
(the exporter reads pre-computed shards from disk). They exist so
that any downstream consumer of the registry can introspect which
legacy function produces each column. The ``__protea_producer__``
attribute makes that legacy reference machine-readable.

Idempotency: :func:`apply_canonical_bindings` is safe to call more
than once on the same registry. It walks every feature in
:data:`protea_contracts.ALL_FEATURES` and rebinds, so a fresh
registry (after :func:`reset_canonical_registry`) is restored to
fully-bound state by re-importing this module's public function.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from protea_contracts import (
    ALL_FEATURES,
    CATEGORICAL_FEATURES,
    EMBEDDING_PCA_DIM,
)

from protea.core.features.registry import CanonicalFeatureRegistry


def _make_producer_marker(producer: Callable[..., Any], label: str) -> Callable[[Any, Any], None]:
    """Build a no-op compute that records its legacy producer.

    ``parquet_export`` does not call ``Feature.compute`` (features are
    pre-computed upstream and read from parquet shards). The marker
    is what downstream introspection consults to find the legacy
    producer of each feature. Calling it is a no-op so any accidental
    invocation does not raise but does not silently fabricate a value
    either: the caller sees the unchanged predictions dict.
    """

    def _compute(_ctx: Any, _predictions: Any) -> None:
        return None

    _compute.__name__ = f"_produced_by_{label}"
    _compute.__protea_producer__ = producer  # type: ignore[attr-defined]
    _compute.__protea_producer_label__ = label  # type: ignore[attr-defined]
    return _compute


def _knn_record_producer() -> Callable[..., Any]:
    """Lazy import shim for :class:`_KnnTransferRunner.run`.

    Imported lazily so importing this module never pulls in the
    heavy KNN runner (numpy / pyarrow / DB session deps).
    """
    from protea.core._knn_transfer_runner import _KnnTransferRunner

    return _KnnTransferRunner.run


def _compute_alignment_producer() -> Callable[..., Any]:
    from protea.core.feature_engineering import compute_alignment

    return compute_alignment


def _compute_taxonomy_producer() -> Callable[..., Any]:
    from protea.core.feature_engineering import compute_taxonomy

    return compute_taxonomy


def _enrich_v6_producer() -> Callable[..., Any]:
    from protea.core.feature_enricher import enrich_v6_features

    return enrich_v6_features


def _compute_lineage_producer() -> Callable[..., Any]:
    """Lazy import shim for the lineage producer in ``protea-method``.

    Imported lazily so importing this module never pulls in the
    GO-DAG closure helpers when the lineage feature family is not
    requested. Registered in T-RES.1 alongside contracts v0.3.0.
    """
    from protea_method.lineage import compute_lineage_features

    return compute_lineage_features


def _interpro_producer() -> Callable[..., Any]:
    """Lazy reference for the InterPro signature->GO feature family.

    The 11 ``interpro_*`` / presence columns (contracts 1.1.0) are
    materialised as a post-pass over the KNN leaf records by
    :func:`protea.core._interpro_features.apply_interpro_features`,
    keyed on ``(protein, go_id)`` against an env-configured InterPro
    GO-prediction table. The marker keeps the registry's producer
    coverage complete; the default zero-fill lives in
    ``_leaf_record_builder._interpro_default_fields`` so every record
    carries all 11 columns unconditionally.
    """
    from protea.core._interpro_features import apply_interpro_features

    return apply_interpro_features


def _self_prior_producer() -> Callable[..., Any]:
    """Reference for the self-prior feature family (lafa-integrate INT-2).

    ``self_prior_score`` is filled in by the native compute in
    :func:`protea.core.operations.predict_go_terms._post_knn_pipeline.apply_self_prior`,
    gated by the ``compute_self_prior`` payload flag. When the flag is
    off (default) every record keeps the zero-fill default emitted by
    ``_leaf_record_builder._lafa_default_fields`` so the canonical-column
    boundary holds without a compute pass. The marker keeps the
    registry's producer coverage complete.
    """
    from protea.core.operations.predict_go_terms._post_knn_pipeline import apply_self_prior

    return apply_self_prior


def _classifier_producer() -> Callable[..., Any]:
    """Reference for the classifier feature family (lafa-integrate INT-2).

    The two ``classifier_*`` columns stay zero-filled in this slice; a
    later lafa-integrate slice wires the full-catalogue classifier
    predictor. The default zero-fill lives in
    ``_leaf_record_builder._lafa_default_fields`` so every record carries
    both columns unconditionally. The marker keeps the registry's
    producer coverage complete.
    """
    from protea.core._leaf_record_builder import _LeafRecordBuilder

    return _LeafRecordBuilder._lafa_default_fields


def _association_producer() -> Callable[..., Any]:
    """Reference for the cross-aspect association feature family (INT-3).

    ``association_total`` / ``association_cross`` / ``association_present``
    are filled in by the native compute in
    :func:`protea.core.operations.predict_go_terms._post_knn_pipeline.apply_association`,
    gated by the ``compute_association`` payload flag. When the flag is off
    (default) every record keeps the zero-fill default emitted by
    ``_leaf_record_builder._lafa_default_fields`` so the canonical-column
    boundary holds without a compute pass. The marker keeps the registry's
    producer coverage complete.
    """
    from protea.core.operations.predict_go_terms._post_knn_pipeline import apply_association

    return apply_association


def _protst_text_producer() -> Callable[..., Any]:
    """Reference for the ProtST text-to-GO transfer family (protst_text lever).

    ``protst_text_score`` / ``protst_vote_fraction`` / ``protst_present`` are
    filled in by the native compute in
    :func:`protea.core.operations.predict_go_terms._protst_text.apply_protst_text`,
    gated by the ``compute_protst`` payload flag (predict) / the ``protst_text``
    export flag. When the flag is off (default) every record keeps the NaN
    declared-absent default emitted by
    ``_leaf_record_builder._protst_default_fields`` so the canonical-column
    boundary holds without a compute pass. The marker keeps the registry's
    producer coverage complete.
    """
    from protea.core.operations.predict_go_terms._protst_text import apply_protst_text

    return apply_protst_text


#: Declared by the contracts, stamped by the lab when pooling manifests.
#: PROTEA emits neither, so they get an explicit not-produced-here marker.
_POOL_INJECTED_FEATURES: tuple[str, ...] = ("plm_id", "k_context")


def _pool_injected_producer() -> Callable[..., Any]:
    """Reference for columns PROTEA declares but does not produce.

    ``plm_id`` and ``k_context`` identify which PLM and which K a row came
    from. They are meaningless for a single manifest and are stamped by the
    lab's pooled multi-manifest loader when several sources are combined to
    train a universal booster. PROTEA never writes them: they are absent from
    every raw parquet dump this platform emits.

    The marker records that absence explicitly rather than binding a fake
    producer, which is the mistake ADR-D45 exists to prevent. A declared
    column with no producer must say so, not quietly resolve to something.
    """
    return _pool_injected_producer


def _annotation_metadata_producer() -> Callable[..., Any]:
    """Reference for categorical metadata columns sourced from annotation rows.

    These columns are not "computed"; they ride alongside each
    candidate prediction from the annotation lookup in the per-aspect
    record builder. The marker keeps the registry's coverage
    complete so :meth:`FeatureRegistry.names` matches
    :data:`ALL_FEATURES` even for non-compute columns.
    """
    from protea.core._knn_transfer_runner import _KnnTransferRunner

    return _KnnTransferRunner._build_records  # type: ignore[attr-defined]


# Static feature -> producer-label map. The label is used to name the
# bound compute function (`_produced_by_<label>`) and to look up the
# lazy producer callable below. Keep alphabetised within each group.
_KNN_RECORD_FEATURES: tuple[str, ...] = (
    "distance",
    "vote_count",
    "k_position",
    "go_term_frequency",
    "ref_annotation_density",
    "neighbor_distance_std",
    "neighbor_vote_fraction",
    "neighbor_min_distance",
    "neighbor_mean_distance",
)

_ALIGNMENT_FEATURES: tuple[str, ...] = (
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
)

_TAXONOMY_PAIR_FEATURES: tuple[str, ...] = (
    "taxonomic_distance",
    "taxonomic_common_ancestors",
    "taxonomic_relation",
)

_V6_ENRICHMENT_FEATURES: tuple[str, ...] = (
    "tax_voters_same_frac",
    "tax_voters_close_frac",
    "tax_voters_mean_common_ancestors",
    "anc2vec_neighbor_cos",
    "anc2vec_neighbor_maxcos",
    "anc2vec_has_emb",
    "anc2vec_query_known_cos",
    "anc2vec_query_known_maxcos",
    "anc2vec_query_known_count",
) + tuple(f"emb_pca_query_{i}" for i in range(EMBEDDING_PCA_DIM))

_LINEAGE_FEATURES: tuple[str, ...] = (
    "lineage_is_ancestor_of_known",
    "lineage_is_descendant_of_known",
    "lineage_ancestor_of_count",
    "lineage_descendant_of_count",
)

_INTERPRO_FEATURES: tuple[str, ...] = (
    "interpro_hit",
    "interpro_score",
    "interpro_n_signatures",
    "interpro_db_pfam",
    "interpro_db_panther",
    "interpro_db_superfamily",
    "interpro_db_smart",
    "interpro_db_cdd",
    "interpro_db_prosite",
    "knn_present",
    "interpro_present",
)

_CLASSIFIER_FEATURES: tuple[str, ...] = (
    "classifier_score",
    "classifier_present",
)

_SELF_PRIOR_FEATURES: tuple[str, ...] = ("self_prior_score",)

_ASSOCIATION_FEATURES: tuple[str, ...] = (
    "association_total",
    "association_cross",
    "association_present",
)

_PROTST_TEXT_FEATURES: tuple[str, ...] = (
    "protst_text_score",
    "protst_vote_fraction",
    "protst_present",
)

_ANNOTATION_METADATA_FEATURES: tuple[str, ...] = tuple(
    name for name in CATEGORICAL_FEATURES if name != "taxonomic_relation"
)


def _build_feature_to_producer() -> dict[str, tuple[Callable[..., Any], str]]:
    """Return ``{feature_name: (lazy_producer_factory, label)}`` for every
    feature in :data:`ALL_FEATURES`. Raises ``KeyError`` if any feature is
    missing a binding, so adding a column to ``ALL_FEATURES`` without a
    binding fails loudly here rather than silently shipping a placeholder.
    """
    mapping: dict[str, tuple[Callable[..., Any], str]] = {}
    for name in _KNN_RECORD_FEATURES:
        mapping[name] = (_knn_record_producer, "knn_transfer_runner")
    for name in _ALIGNMENT_FEATURES:
        mapping[name] = (_compute_alignment_producer, "compute_alignment")
    for name in _TAXONOMY_PAIR_FEATURES:
        mapping[name] = (_compute_taxonomy_producer, "compute_taxonomy")
    for name in _V6_ENRICHMENT_FEATURES:
        mapping[name] = (_enrich_v6_producer, "enrich_v6_features")
    for name in _LINEAGE_FEATURES:
        mapping[name] = (_compute_lineage_producer, "compute_lineage_features")
    for name in _INTERPRO_FEATURES:
        mapping[name] = (_interpro_producer, "interpro_features")
    for name in _CLASSIFIER_FEATURES:
        mapping[name] = (_classifier_producer, "lafa_classifier")
    for name in _SELF_PRIOR_FEATURES:
        mapping[name] = (_self_prior_producer, "lafa_self_prior")
    for name in _ASSOCIATION_FEATURES:
        mapping[name] = (_association_producer, "lafa_association")
    for name in _PROTST_TEXT_FEATURES:
        mapping[name] = (_protst_text_producer, "protst_text")
    for name in _ANNOTATION_METADATA_FEATURES:
        mapping[name] = (_annotation_metadata_producer, "annotation_metadata")
    for name in _POOL_INJECTED_FEATURES:
        mapping[name] = (_pool_injected_producer, "pool_injected")
    missing = [name for name in ALL_FEATURES if name not in mapping]
    if missing:
        raise KeyError(
            "T2B.2 binding map missing producers for features: "
            f"{missing!r}. Add an entry in protea.core.features._bindings."
        )
    return mapping


def apply_canonical_bindings(registry: CanonicalFeatureRegistry) -> int:
    """Bind every :data:`ALL_FEATURES` feature on ``registry`` to its legacy
    producer reference. Returns the count of features bound.

    Idempotent: rebinding an already-bound feature is a no-op (the
    underlying :meth:`CanonicalFeatureRegistry.bind_compute` replaces
    the callable without touching dtype / family).
    """
    feature_to_producer = _build_feature_to_producer()
    count = 0
    for name in ALL_FEATURES:
        lazy_factory, label = feature_to_producer[name]
        compute = _make_producer_marker(lazy_factory, label)
        registry.bind_compute(name, compute)
        count += 1
    return count


__all__ = ["apply_canonical_bindings"]
