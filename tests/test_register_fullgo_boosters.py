"""Unit tests for the fullgo per-category booster registration helper.

Targets ``scripts/register_fullgo_boosters.py``: the request-body construction
for the three NK / LK / PK boosters and the schema-sha resolution. No live API
or DB is touched; only the pure body-builders are exercised.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "register_fullgo_boosters.py"
_spec = importlib.util.spec_from_file_location("register_fullgo_boosters", _SCRIPT)
assert _spec and _spec.loader
mod = importlib.util.module_from_spec(_spec)
# Register before exec so the module's frozen dataclass can resolve its own
# module via sys.modules during class processing.
sys.modules["register_fullgo_boosters"] = mod
_spec.loader.exec_module(mod)


def test_build_request_body_stamps_category_and_sha() -> None:
    body = mod.build_request_body(
        category="nk",
        artifact_uri="s3://b/ensemble_gbm_NK.txt",
        feature_schema_sha="abc123",
        name_prefix="fullgo",
        dataset_id="00000000-0000-0000-0000-000000000003",
        external_source="protea-reranker-lab@deadbee",
        force=True,
        families=["knn", "annotation_meta"],
    )
    assert body["artifact_uri"] == "s3://b/ensemble_gbm_NK.txt"
    assert body["name"] == "fullgo-nk"
    assert body["run"]["run_id"] == "fullgo-nk"
    # The router reads run.features.feature_schema_sha when contracts is absent.
    assert body["run"]["features"]["feature_schema_sha"] == "abc123"
    assert body["run"]["features"]["families_enabled"] == ["knn", "annotation_meta"]
    # The category is stamped via spec.yaml training.cell for INT-5 dispatch.
    assert "cell: nk" in body["spec_yaml"]
    assert body["dataset_id"] == "00000000-0000-0000-0000-000000000003"
    assert body["external_source"] == "protea-reranker-lab@deadbee"
    assert body["force"] is True


def test_build_request_body_optional_fields_omitted() -> None:
    body = mod.build_request_body(
        category="pk",
        artifact_uri="file:///m.txt",
        feature_schema_sha="sha",
        name_prefix="fullgo",
        dataset_id=None,
        external_source=None,
        force=False,
    )
    assert "dataset_id" not in body
    assert "external_source" not in body
    assert body["force"] is False
    # No families passed -> families_enabled is not recorded.
    assert "families_enabled" not in body["run"]["features"]


def test_build_request_body_rejects_unknown_category() -> None:
    with pytest.raises(ValueError):
        mod.build_request_body(
            category="zz",
            artifact_uri="file:///m.txt",
            feature_schema_sha="sha",
            name_prefix="fullgo",
            dataset_id=None,
            external_source=None,
            force=False,
        )


def test_build_all_bodies_three_in_canonical_order() -> None:
    boosters = mod._BoosterArgs(
        uris={"nk": "s3://b/nk.txt", "lk": "s3://b/lk.txt", "pk": "s3://b/pk.txt"}
    )
    bodies = mod.build_all_bodies(
        boosters,
        feature_schema_sha="livesha",
        name_prefix="fullgo",
        dataset_id=None,
        external_source=None,
        force=False,
    )
    assert [b["name"] for b in bodies] == ["fullgo-nk", "fullgo-lk", "fullgo-pk"]
    # Every booster carries the SAME live schema sha (the FARM-EXP.5 guard).
    assert {b["run"]["features"]["feature_schema_sha"] for b in bodies} == {"livesha"}
    assert [b["artifact_uri"] for b in bodies] == [
        "s3://b/nk.txt",
        "s3://b/lk.txt",
        "s3://b/pk.txt",
    ]


def test_resolve_schema_sha_prefers_supplied() -> None:
    assert mod._resolve_schema_sha("explicit-sha") == "explicit-sha"


def test_live_families_match_scorer_inference() -> None:
    """The auto-derived families equal what the scorer uses at inference.

    Both call ``infer_active_feature_families(True, True, True)``, so the
    registered booster's recorded sha equals the live sha the scorer computes
    and the FARM-EXP.5 guard accepts it. Skips if protea is not importable.
    """
    reranker = pytest.importorskip("protea.core.reranker")
    expected = reranker.infer_active_feature_families(
        compute_alignments=True, compute_taxonomy=True, compute_v6_features=True
    )
    assert mod.live_feature_families() == expected


def test_resolve_schema_sha_computes_from_live_families() -> None:
    """When no sha is supplied it is derived from the live families.

    Mirrors what the live pipeline computes, so a registered booster's
    fingerprint equals the inference-time one. Skips if contracts is absent.
    """
    contracts = pytest.importorskip("protea_contracts")
    pytest.importorskip("protea.core.reranker")
    expected = contracts.compute_feature_schema_sha(mod.live_feature_families())
    assert mod._resolve_schema_sha(None) == expected
