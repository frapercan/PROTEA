"""Content-addressed cache of the STABLE per-candidate export feature table.

NFR-ARCH "export decouple". The ``export_research_dataset`` operation spends
the bulk of its (~8h) runtime computing per-candidate features that depend only
on the KNN / alignment / taxonomy / anc2vec / lineage / emb_pca / self_prior /
association configuration, NOT on the classifier model. A/B-testing classifier
variants (7-seed Deep Ensemble vs EMA vs SWA vs late-fusion) historically meant
re-running that whole pipeline for each variant.

This module splits the export's per-candidate feature columns into two classes:

* **STABLE** -- every column whose value is a pure function of the *stable
  config* (embedding_config, ontology_snapshot, train/test versions, k,
  distance/metric/backend, and the stable feature flags). These are the
  expensive ones and they are byte-identical across classifier swaps.
* **VARIABLE** -- ``classifier_score`` / ``classifier_present`` (and any
  future per-classifier-variant columns). These are cheap and DO change when
  the classifier model changes.

The cache stores the consolidated STABLE ``train.parquet`` / ``eval.parquet``
(the classifier columns zero-filled) under a ``stable_features/<hash>/`` prefix
in the configured :class:`ArtifactStore`, keyed by
:func:`compute_stable_cache_key`. A later export with the SAME stable config but
a DIFFERENT classifier reuses the cached stable table and only recomputes the
classifier column(s), turning a classifier A/B from hours into the
classifier-compute time alone.

Leakage discipline is unchanged: the classifier feature reads ONLY the query
protein's frozen PLM embeddings (never an annotation set), so the variable
post-pass is leakage-equivalent to the inline producer regardless of which
stable table it re-stamps. The stable columns themselves are produced by the
unchanged inline pipeline (with the classifier flag held off), so a cached
stable parquet is byte-identical to what the same stable config would emit
inline.

Everything here is additive and default-off: the cache only engages when the
``stable_feature_cache`` payload flag is set.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from protea.core.training_dump._payload import TrainRerankerAutoPayload

# Cache layout version. Bump ONLY when the stable feature semantics change in a
# way that must invalidate every existing cached table (e.g. a stable producer
# changes its output for the same config). Folding this into the key means a
# bump silently routes around stale tables instead of serving wrong features.
STABLE_CACHE_PRODUCER_VERSION = "1"

# Artifact-store prefix the stable tables live under.
STABLE_CACHE_PREFIX = "stable_features/"

# The VARIABLE columns: recomputed per classifier variant, never cached. Mirror
# of ``protea.core.features._bindings._CLASSIFIER_FEATURES`` (kept as a local
# literal so this leaf module does not import the feature-registry graph).
VARIABLE_FEATURE_COLUMNS: tuple[str, ...] = ("classifier_score", "classifier_present")


@dataclass(frozen=True)
class ClassifierVariantSpec:
    """One classifier variant to emit as its own column family (SKETCH).

    Phase 2 (multi-classifier emission) accepts a LIST of these so one export
    covers several classifier variants in a single pass over the cached stable
    table. ``label`` is the column suffix (``classifier_score__<label>``);
    ``seed_dir`` / ``seed_paths`` / ``model_path`` select the checkpoint(s)
    exactly like the ``PROTEA_CLASSIFIER_SEED_DIR`` / ``_SEED_PATHS`` /
    ``_MODEL_PATH`` env vars do for the default single classifier (see
    ``protea.core.classifier_producer.resolve_seed_paths`` /
    :func:`~protea.core.classifier_producer.get_classifier`).

    Default-output back-compat: when no variants are given the export still
    emits the canonical ``classifier_score`` / ``classifier_present`` columns
    from the env-selected default classifier, so existing lab boosters are
    unaffected.
    """

    label: str
    seed_dir: str | None = None
    seed_paths: str | None = None
    model_path: str | None = None

    def score_column(self) -> str:
        return f"classifier_score__{self.label}"

    def present_column(self) -> str:
        return f"classifier_present__{self.label}"


def _stable_cache_key_inputs(p: TrainRerankerAutoPayload) -> dict[str, Any]:
    """The exact, ordered inputs that define a stable table's identity.

    Everything here is classifier-INDEPENDENT. Adding a NEW stable feature
    family (a new flag that changes the stable columns) MUST be added here so a
    config that toggles it gets a fresh key; adding a VARIABLE column (a new
    classifier knob) MUST NOT be added here, so classifier swaps keep the same
    stable key and hit the cache.
    """
    return {
        "producer_version": STABLE_CACHE_PRODUCER_VERSION,
        "embedding_config_id": str(p.embedding_config_id),
        "ontology_snapshot_id": str(p.ontology_snapshot_id),
        "annotation_source": p.annotation_source,
        "train_versions": sorted(p.train_versions),
        "test_versions": sorted(p.test_versions),
        "k": int(p.limit_per_entry),
        "distance_threshold": p.distance_threshold,
        "metric": p.metric,
        "search_backend": p.search_backend,
        "faiss_index_type": p.faiss_index_type,
        "faiss_nlist": int(p.faiss_nlist),
        "faiss_nprobe": int(p.faiss_nprobe),
        # Stable feature flags (each changes the stable columns).
        "compute_alignments": bool(p.compute_alignments),
        "compute_taxonomy": bool(p.compute_taxonomy),
        "expand_votes_to_ancestors": bool(p.expand_votes_to_ancestors),
        "use_embedding_pca": bool(p.use_embedding_pca),
        "compute_self_prior": bool(p.compute_self_prior),
        "compute_association": bool(p.compute_association),
        # NOTE: compute_classifier is deliberately ABSENT -- it is the variable
        # family and must not perturb the stable key.
    }


def compute_stable_cache_key(p: TrainRerankerAutoPayload) -> str:
    """Deterministic content-address hash of the stable export config.

    Same stable config -> same key (so a classifier swap reuses the cached
    table). Any change to embedding_config / versions / k / distance / metric /
    a stable feature flag -> different key. The classifier flags / variants are
    NOT part of the key by construction.
    """
    blob = json.dumps(_stable_cache_key_inputs(p), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def stable_cache_prefix(key: str) -> str:
    """Artifact-store key prefix the stable train/eval parquets live under."""
    return f"{STABLE_CACHE_PREFIX}{key}/"


def cache_manifest(p: TrainRerankerAutoPayload, key: str) -> dict[str, Any]:
    """Human-auditable provenance written next to the cached stable parquets."""
    return {
        "stable_cache_key": key,
        "stable_cache_producer_version": STABLE_CACHE_PRODUCER_VERSION,
        "variable_columns": list(VARIABLE_FEATURE_COLUMNS),
        "inputs": _stable_cache_key_inputs(p),
    }


__all__ = (
    "STABLE_CACHE_PREFIX",
    "STABLE_CACHE_PRODUCER_VERSION",
    "VARIABLE_FEATURE_COLUMNS",
    "ClassifierVariantSpec",
    "cache_manifest",
    "compute_stable_cache_key",
    "stable_cache_prefix",
)
