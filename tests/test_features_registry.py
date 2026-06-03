"""Tests for the concrete FeatureRegistry implementation (T2B.1).

Pins:

* contract conformance with :class:`protea_contracts.FeatureRegistry`,
* registration order matching ``ALL_FEATURES``,
* every canonical feature being registered with the right dtype and
  exactly one family,
* the placeholder compute raising when invoked so accidental T2B.2
  pre-empts surface loudly,
* ``bind_compute`` swapping the compute without touching dtype/family,
* the module-level singleton being process-stable across calls and
  resettable via :func:`reset_canonical_registry`.
"""

from __future__ import annotations

from typing import Any

import pytest
from protea_contracts import (
    ALL_FEATURES,
    CATEGORICAL_FEATURES,
    FEATURE_FAMILIES,
    NUMERIC_FEATURES,
    Feature,
    FeatureRegistry,
)

from protea.core.features.registry import (
    CanonicalFeatureRegistry,
    get_canonical_registry,
    reset_canonical_registry,
)


@pytest.fixture(autouse=True)
def _isolate_singleton():
    reset_canonical_registry()
    yield
    reset_canonical_registry()


class TestCanonicalFeatureRegistry:
    def test_is_feature_registry_subclass(self) -> None:
        assert issubclass(CanonicalFeatureRegistry, FeatureRegistry)

    def test_register_and_get_round_trip(self) -> None:
        reg = CanonicalFeatureRegistry()
        feat = Feature(
            name="distance",
            family="knn",
            dtype="numeric",
            compute=lambda _c, _p: None,
        )
        reg.register(feat)
        assert reg.get("distance") is feat

    def test_double_register_raises_value_error(self) -> None:
        reg = CanonicalFeatureRegistry()
        feat = Feature(
            name="distance",
            family="knn",
            dtype="numeric",
            compute=lambda _c, _p: None,
        )
        reg.register(feat)
        with pytest.raises(ValueError, match="distance"):
            reg.register(feat)

    def test_get_unknown_raises_key_error(self) -> None:
        reg = CanonicalFeatureRegistry()
        with pytest.raises(KeyError, match="unknown"):
            reg.get("not-a-feature")

    def test_names_preserves_insertion_order(self) -> None:
        reg = CanonicalFeatureRegistry()
        reg.register(Feature("b", "knn", "numeric", lambda _c, _p: None))
        reg.register(Feature("a", "knn", "numeric", lambda _c, _p: None))
        assert reg.names() == ["b", "a"]

    def test_families_view_uses_contracts_overlap(self) -> None:
        # Register one feature that lives in both ``knn`` and ``knn_distance``;
        # the families view must list it under both per FEATURE_FAMILIES.
        reg = CanonicalFeatureRegistry()
        reg.register(Feature("distance", "knn_distance", "numeric", lambda _c, _p: None))
        fams = reg.families()
        assert fams["knn_distance"] == ["distance"]
        assert fams["knn"] == ["distance"]

    def test_families_empty_when_nothing_registered(self) -> None:
        reg = CanonicalFeatureRegistry()
        assert reg.families() == {}

    def test_selected_filters_by_family(self) -> None:
        reg = CanonicalFeatureRegistry()
        reg.register(Feature("identity_nw", "alignment_nw", "numeric", lambda _c, _p: None))
        reg.register(Feature("alignment_score_nw", "alignment_nw", "numeric", lambda _c, _p: None))
        names = {f.name for f in reg.selected(["alignment_nw"])}
        assert names == {"identity_nw", "alignment_score_nw"}

    def test_selected_drop_excludes_named_feature(self) -> None:
        reg = CanonicalFeatureRegistry()
        reg.register(Feature("identity_nw", "alignment_nw", "numeric", lambda _c, _p: None))
        reg.register(Feature("alignment_score_nw", "alignment_nw", "numeric", lambda _c, _p: None))
        kept = reg.selected(["alignment_nw"], drop=["identity_nw"])
        assert {f.name for f in kept} == {"alignment_score_nw"}

    def test_selected_unknown_family_raises(self) -> None:
        reg = CanonicalFeatureRegistry()
        with pytest.raises(KeyError, match="unknown family"):
            reg.selected(["not-a-family"])

    def test_bind_compute_replaces_callable_only(self) -> None:
        reg = CanonicalFeatureRegistry()
        reg.register(Feature("distance", "knn_distance", "numeric", lambda _c, _p: None))
        called: list[str] = []

        def real_compute(_ctx: Any, _preds: Any) -> None:
            called.append("ran")

        reg.bind_compute("distance", real_compute)
        bound = reg.get("distance")
        assert bound.family == "knn_distance"
        assert bound.dtype == "numeric"
        bound.compute(None, None)
        assert called == ["ran"]

    def test_bind_compute_unknown_raises_key_error(self) -> None:
        reg = CanonicalFeatureRegistry()
        with pytest.raises(KeyError, match="cannot bind"):
            reg.bind_compute("nope", lambda _c, _p: None)


class TestCanonicalRegistryPopulation:
    def test_registers_every_all_features_entry(self) -> None:
        reg = get_canonical_registry()
        assert reg.names() == list(ALL_FEATURES)

    def test_dtype_partitions_numeric_and_categorical(self) -> None:
        reg = get_canonical_registry()
        for name in NUMERIC_FEATURES:
            assert reg.get(name).dtype == "numeric"
        for name in CATEGORICAL_FEATURES:
            assert reg.get(name).dtype == "categorical"

    def test_every_feature_has_a_family_from_contracts(self) -> None:
        reg = get_canonical_registry()
        known_families = set(FEATURE_FAMILIES.keys())
        for name in ALL_FEATURES:
            assert reg.get(name).family in known_families

    def test_families_view_matches_contracts_map(self) -> None:
        reg = get_canonical_registry()
        registry_fams = {k: sorted(v) for k, v in reg.families().items()}
        contract_fams = {k: sorted(v) for k, v in FEATURE_FAMILIES.items()}
        assert registry_fams == contract_fams

    def test_module_level_registry_singleton_exposes_canonical_names(self) -> None:
        from protea.core.features.registry import REGISTRY

        # The module-level REGISTRY is built at import time and is the
        # name imported by the cross-repo contract test
        # ``tests.test_feature_contract.TestRegistryCoversContracts``.
        # Identity with the cached singleton is not asserted because
        # ``reset_canonical_registry`` rebuilds the cache; the module
        # attribute deliberately remains stable.
        assert REGISTRY.names() == list(ALL_FEATURES)

    def test_placeholder_compute_raises_until_bound(self) -> None:
        reg = get_canonical_registry()
        feat = reg.get("distance")
        with pytest.raises(NotImplementedError, match="distance"):
            feat.compute(None, None)

    def test_selected_uses_contracts_family_definitions(self) -> None:
        reg = get_canonical_registry()
        # alignment_nw is a leaf family (no overlap with knn_*).
        names = {f.name for f in reg.selected(["alignment_nw"])}
        assert names == set(FEATURE_FAMILIES["alignment_nw"])


class TestSingletonLifecycle:
    def test_get_canonical_registry_returns_same_instance(self) -> None:
        first = get_canonical_registry()
        second = get_canonical_registry()
        assert first is second

    def test_reset_canonical_registry_rebuilds(self) -> None:
        first = get_canonical_registry()
        reset_canonical_registry()
        second = get_canonical_registry()
        assert first is not second
        assert second.names() == first.names()
