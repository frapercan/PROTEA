"""Invariant tests cross-repo (T1.7 of master plan v3).

Pins the contract between ``protea_contracts`` (canonical schema)
and PROTEA's re-exports / future registry. Any drift here means the
booster cache is at risk.

Two tiers of assertions:

1. **Re-export identity** (active now): every name PROTEA still
   exposes from its old module locations must be the *same object*
   (or equal value) as the canonical one in ``protea_contracts``.
2. **Registry cover** (skipped until F2B.1 of master plan v3 lands
   the in-process registry in ``protea/core/features/``): the live
   registry must cover exactly ``ALL_FEATURES`` and group them
   under the same family map.

Once F2B.1 ships, the skipped tests turn green automatically and
the suite gains a hard guarantee that the platform's feature
generation matches what every booster was trained against.
"""

from __future__ import annotations

import importlib

import protea_contracts
import pytest

# ---------------------------------------------------------------------------
# Re-export identity (active)
# ---------------------------------------------------------------------------


class TestReexportIdentity:
    """Every constant PROTEA still re-exports must be the same object
    or equal value as the canonical one in protea_contracts. Hard
    guarantee that ``from protea.core.reranker import ALL_FEATURES``
    does NOT diverge from ``from protea_contracts import ALL_FEATURES``.
    """

    def test_all_features_identity(self) -> None:
        from protea.core.reranker import ALL_FEATURES as proto_all

        assert proto_all is protea_contracts.ALL_FEATURES

    def test_numeric_features_identity(self) -> None:
        from protea.core.reranker import NUMERIC_FEATURES as proto_num

        assert proto_num is protea_contracts.NUMERIC_FEATURES

    def test_categorical_features_identity(self) -> None:
        from protea.core.reranker import CATEGORICAL_FEATURES as proto_cat

        assert proto_cat is protea_contracts.CATEGORICAL_FEATURES

    def test_embedding_pca_dim_value(self) -> None:
        from protea.core.reranker import EMBEDDING_PCA_DIM as proto_dim

        assert proto_dim == protea_contracts.EMBEDDING_PCA_DIM

    def test_label_column_value(self) -> None:
        from protea.core.reranker import LABEL_COLUMN as proto_label

        assert proto_label == protea_contracts.LABEL_COLUMN

    def test_protea_payload_identity(self) -> None:
        from protea.core.contracts.operation import ProteaPayload as proto_base

        assert proto_base is protea_contracts.ProteaPayload

    def test_predict_payloads_identity(self) -> None:
        from protea.core.operations.predict_go_terms import (
            PredictGOTermsBatchPayload,
            PredictGOTermsPayload,
            StorePredictionsPayload,
        )

        assert PredictGOTermsPayload is protea_contracts.PredictGOTermsPayload
        assert PredictGOTermsBatchPayload is protea_contracts.PredictGOTermsBatchPayload
        assert StorePredictionsPayload is protea_contracts.StorePredictionsPayload


# ---------------------------------------------------------------------------
# Sha consistency (active)
# ---------------------------------------------------------------------------


class TestShaConsistency:
    """compute_schema_sha must produce the same digest from any caller
    on the same column list. Pinning here catches accidental drift
    if PROTEA ever stops re-exporting and grows its own copy."""

    def test_all_features_sha_matches_canonical(self) -> None:
        sha_via_contracts = protea_contracts.compute_schema_sha(
            protea_contracts.ALL_FEATURES
        )
        # Re-import through PROTEA path — must hit the same constant
        # and the same function (re-exported).
        from protea.core.reranker import ALL_FEATURES as proto_all

        sha_via_protea = protea_contracts.compute_schema_sha(proto_all)
        assert sha_via_contracts == sha_via_protea

    def test_sha_is_pinned_to_golden(self) -> None:
        # Mirrors the golden test inside protea-contracts: bumping any
        # column forces a SemVer major bump on protea-contracts AND a
        # re-train of every downstream LightGBM booster. Pinned here
        # too so PROTEA's CI fails before the booster cache invalidates.
        # T-RES.1b rolled this forward to v0.3.0 of protea-contracts,
        # which added the four ``lineage_*`` columns; prior golden
        # ``"145592ed186c"`` is superseded. S3 rolls it forward to
        # contracts 0.4.0, which added the 11-column interpro feature
        # family (67 columns total); prior golden ``"8d9a1668b1a2"``
        # is superseded. lafa-integrate INT-2 rolls it forward to
        # contracts 0.5.0, which added the six ``classifier_*`` /
        # ``self_prior_score`` / ``association_*`` columns (73 columns
        # total); prior golden ``"4390db315e76"`` is superseded. Mirrors
        # the golden in protea-contracts ``tests/test_feature_schema.py``.
        sha = protea_contracts.compute_schema_sha(protea_contracts.ALL_FEATURES)
        assert sha == "b2a5cd46ec6b"


# ---------------------------------------------------------------------------
# Feature-family coverage invariants (active)
# ---------------------------------------------------------------------------


class TestFeatureFamilyCoverage:
    def test_every_family_member_in_all_features(self) -> None:
        all_set = set(protea_contracts.ALL_FEATURES)
        offenders = []
        for family, cols in protea_contracts.FEATURE_FAMILIES.items():
            for col in cols:
                if col not in all_set:
                    offenders.append((family, col))
        assert offenders == [], (
            "FEATURE_FAMILIES references columns missing from ALL_FEATURES: "
            f"{offenders}"
        )

    def test_emb_pca_family_size_matches_dim(self) -> None:
        emb_pca = protea_contracts.FEATURE_FAMILIES["emb_pca"]
        assert len(emb_pca) == protea_contracts.EMBEDDING_PCA_DIM

    def test_emb_pca_family_names_are_canonical(self) -> None:
        expected = [
            f"emb_pca_query_{i}" for i in range(protea_contracts.EMBEDDING_PCA_DIM)
        ]
        assert protea_contracts.FEATURE_FAMILIES["emb_pca"] == expected


# ---------------------------------------------------------------------------
# Future registry cover (skipped until F2B.1)
# ---------------------------------------------------------------------------


def _registry_module_available() -> bool:
    try:
        importlib.import_module("protea.core.features.registry")
        return True
    except ImportError:
        return False


@pytest.mark.skipif(
    not _registry_module_available(),
    reason=(
        "F2B.1 of master plan v3 lands the in-process feature registry "
        "in protea/core/features/registry.py. Until then this contract "
        "test is dormant; once the registry ships it activates "
        "automatically and pins the registry-vs-contracts invariant."
    ),
)
class TestRegistryCoversContracts:
    def test_registry_names_match_all_features(self) -> None:
        from protea.core.features.registry import REGISTRY  # type: ignore[import-not-found]

        assert set(REGISTRY.names()) == set(protea_contracts.ALL_FEATURES)

    def test_registry_families_match_contracts(self) -> None:
        from protea.core.features.registry import REGISTRY  # type: ignore[import-not-found]

        # Order inside each family list does not matter at the registry
        # level; the dataset side enforces order via ALL_FEATURES.
        registry_fams = {k: sorted(v) for k, v in REGISTRY.families().items()}
        contract_fams = {
            k: sorted(v) for k, v in protea_contracts.FEATURE_FAMILIES.items()
        }
        assert registry_fams == contract_fams
