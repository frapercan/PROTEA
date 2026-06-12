"""Concrete in-process :class:`FeatureRegistry` for PROTEA (T2B.1).

The registry is the single point of truth for *which* feature columns
are computed and *which families* they belong to. It implements the
:class:`protea_contracts.FeatureRegistry` ABC and, at import time, is
populated from the canonical
:data:`protea_contracts.FEATURE_FAMILIES` map so every consumer sees
the same column set and family layout the lab was trained against.

Compute callables are intentionally placeholders in this slice (T2B.1).
T2B.2 of master plan v3.2 §24 wires the existing per-feature compute
helpers (``feature_engineering``, ``feature_enricher``,
``anc2vec_embeddings``, ``pca_cache``) into
:meth:`CanonicalFeatureRegistry.bind_compute` so ``parquet_export`` and
``_predict_batch`` can drive feature generation through the registry
instead of the open-coded loops they use today.

Calling a placeholder compute raises ``NotImplementedError`` with the
feature name so a premature integration fails loudly rather than
silently emitting empty columns.

A module-level :data:`REGISTRY` singleton mirrors the cross-repo
contract test pinned in :mod:`tests.test_feature_contract`
(``TestRegistryCoversContracts``). The test activates automatically
on the presence of this module and pins the
registry-vs-contracts invariant.
"""

from __future__ import annotations

from typing import Any, cast

from protea_contracts import (
    ALL_FEATURES,
    CATEGORICAL_FEATURES,
    FEATURE_FAMILIES,
    Feature,
    FeatureDtype,
    FeatureRegistry,
)


def _placeholder(name: str) -> Any:
    """Return a compute callable that flags an unbound feature.

    T2B.2 replaces these with real compute helpers via
    :meth:`CanonicalFeatureRegistry.bind_compute`. Until then any
    accidental use raises so the wiring gap is obvious.
    """

    def _raise(_ctx: Any, _predictions: Any) -> None:
        raise NotImplementedError(
            f"feature {name!r} has no compute bound. T2B.2 wires the canonical compute helpers."
        )

    _raise.__name__ = f"_unbound_{name}"
    return _raise


def _dtype_for(feature_name: str) -> FeatureDtype:
    return "categorical" if feature_name in CATEGORICAL_FEATURES else "numeric"


# Order mirrors the lab's preferred dispatch (most specific subset first)
# and decides which family each :class:`Feature` instance carries on its
# ``family`` field. The full overlapping FEATURE_FAMILIES map is still
# what :meth:`CanonicalFeatureRegistry.families` exposes; this constant
# only picks the canonical scalar tag.
_PRIMARY_FAMILY_ORDER: tuple[str, ...] = (
    "knn_distance",
    "knn_vote",
    "alignment_nw",
    "alignment_sw",
    "length",
    "taxonomy_pair",
    "taxonomy_voters",
    "go_context",
    "anc2vec_neighbor",
    "anc2vec_query",
    "emb_pca",
    "lineage",
    "interpro",
    "annotation_meta",
    "knn",
)


def _resolve_primary_family(feature_name: str) -> str:
    """Pick the most specific family for ``feature_name``.

    Used only to populate the :attr:`Feature.family` scalar. The
    overlapping family map exposed via :meth:`families` is the true
    grouping consumers rely on.
    """
    for fam in _PRIMARY_FAMILY_ORDER:
        if feature_name in FEATURE_FAMILIES.get(fam, ()):
            return fam
    raise KeyError(
        f"feature {feature_name!r} is not listed in any family of protea_contracts.FEATURE_FAMILIES"
    )


class CanonicalFeatureRegistry(FeatureRegistry):
    """In-memory :class:`FeatureRegistry` populated from the contracts.

    Two membership views co-exist:

    * Per-feature ``Feature.family`` scalar (the most specific family,
      used for metadata / single-tag dispatch).
    * The overlapping :data:`protea_contracts.FEATURE_FAMILIES` map,
      returned by :meth:`families` and consulted by :meth:`selected`.
      This is what the lab trains against and is the invariant pinned
      by :class:`tests.test_feature_contract.TestRegistryCoversContracts`.

    Insertion order is preserved so :meth:`names` returns features in
    the order they were registered.
    """

    def __init__(self) -> None:
        self._features: dict[str, Feature] = {}

    def register(self, feature: Feature) -> None:
        if feature.name in self._features:
            raise ValueError(f"feature already registered: {feature.name!r}")
        self._features[feature.name] = feature

    def get(self, name: str) -> Feature:
        try:
            return self._features[name]
        except KeyError as exc:
            raise KeyError(f"unknown feature: {name!r}") from exc

    def names(self) -> list[str]:
        return list(self._features.keys())

    def families(self) -> dict[str, list[str]]:
        """Return the overlapping family map for currently registered features.

        Walks :data:`FEATURE_FAMILIES` so the result mirrors the
        canonical contracts grouping (``knn`` is the union of
        ``knn_distance`` and ``knn_vote`` plus the std feature, etc.).
        Only features actually registered are included; the map is
        empty for a fresh registry.
        """
        out: dict[str, list[str]] = {}
        registered = self._features
        for family, members in FEATURE_FAMILIES.items():
            present = [name for name in members if name in registered]
            if present:
                out[family] = present
        return out

    def selected(
        self,
        active_families: list[str],
        drop: list[str] | None = None,
    ) -> list[Feature]:
        """Union the features in every requested family, then apply ``drop``.

        Preserves registration order (which mirrors
        :data:`ALL_FEATURES`) so the returned list is deterministic
        across calls. Unknown family names raise ``KeyError`` to
        mirror the lab's strict family validation.
        """
        drop_set = set(drop or [])
        selected_names: set[str] = set()
        for family in active_families:
            if family not in FEATURE_FAMILIES:
                raise KeyError(f"unknown family: {family!r}")
            selected_names.update(FEATURE_FAMILIES[family])
        return [
            feat
            for feat in self._features.values()
            if feat.name in selected_names and feat.name not in drop_set
        ]

    def bind_compute(
        self,
        name: str,
        compute: Any,
    ) -> None:
        """Replace the placeholder compute for ``name``.

        T2B.2 calls this once per feature at module import time to
        swap the :func:`_placeholder` raiser for the real compute
        helper. The feature must already be registered.
        """
        if name not in self._features:
            raise KeyError(f"cannot bind compute: unknown feature {name!r}")
        existing = self._features[name]
        self._features[name] = Feature(
            name=existing.name,
            family=existing.family,
            dtype=existing.dtype,
            compute=compute,
        )


def _build_canonical_registry() -> CanonicalFeatureRegistry:
    """Construct the canonical registry from the contracts module.

    Iterates :data:`protea_contracts.ALL_FEATURES` and registers each
    feature exactly once with a placeholder compute and its
    most-specific family tag. The registration order matches
    ``ALL_FEATURES`` so downstream readers can iterate in the lab's
    canonical column order.
    """
    registry = CanonicalFeatureRegistry()
    for feature_name in ALL_FEATURES:
        registry.register(
            Feature(
                name=feature_name,
                family=_resolve_primary_family(feature_name),
                dtype=_dtype_for(feature_name),
                compute=_placeholder(feature_name),
            )
        )
    return registry


_CANONICAL_REGISTRY: CanonicalFeatureRegistry | None = None


def get_canonical_registry() -> CanonicalFeatureRegistry:
    """Return the process-wide canonical registry, building it on first use.

    The registry is rebuilt only after :func:`reset_canonical_registry`
    (test-only seam). Callers that need to bind real compute helpers
    (T2B.2) acquire the singleton and call
    :meth:`CanonicalFeatureRegistry.bind_compute` per feature.
    """
    global _CANONICAL_REGISTRY
    if _CANONICAL_REGISTRY is None:
        _CANONICAL_REGISTRY = _build_canonical_registry()
    return cast(CanonicalFeatureRegistry, _CANONICAL_REGISTRY)


def reset_canonical_registry() -> None:
    """Drop the cached canonical registry. Test-only seam."""
    global _CANONICAL_REGISTRY
    _CANONICAL_REGISTRY = None


#: Process-wide canonical registry instance. Importing this name
#: activates the cross-repo contract test
#: :class:`tests.test_feature_contract.TestRegistryCoversContracts`
#: which pins the registry-vs-:data:`FEATURE_FAMILIES` invariant.
REGISTRY: CanonicalFeatureRegistry = get_canonical_registry()
