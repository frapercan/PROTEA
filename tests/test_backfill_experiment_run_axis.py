"""Unit tests for ``scripts/backfill_experiment_run_axis.py``.

These tests exercise the pure derivation helpers (``derive_axis_payload``,
``_compute_shortid_if_complete``) on representative provenance payloads.
The integration test against a live Postgres lives in the dedicated
``--with-postgres`` suite to keep this file fast and offline.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from protea.core.axis_tuple import (  # noqa: E402
    CANONICAL_AXIS_KEYS,
    axis_tuple_shortid,
)
from scripts.backfill_experiment_run_axis import (  # noqa: E402
    DEFAULT_PROPAGATION,
    SHORTID_REQUIRED_KEYS,
    _compute_shortid_if_complete,
    derive_axis_payload,
)

# ----------------------------------------------------------------- derive


def test_derive_from_complete_config() -> None:
    cfg = {
        "plm": "esm2_t33",
        "k": 5,
        "reranker_spec_id": "lgbm-v6",
        "feature_schema_sha": "0123456789ab",
        "eval_set_name": "bench-v1-K5-v226",
        "eval_set_manifest_sha": "cafe1234beef",
        "propagation": "none",
        "ensemble_spec": None,
    }
    out = derive_axis_payload(cfg, {})
    assert out == {
        "plm": "esm2_t33",
        "k": 5,
        "reranker_spec_id": "lgbm-v6",
        "feature_schema_sha": "0123456789ab",
        "eval_set_name": "bench-v1-K5-v226",
        "eval_set_manifest_sha": "cafe1234beef",
        "propagation": "none",
        "ensemble_spec": None,
    }


def test_derive_uses_provenance_when_config_missing() -> None:
    cfg = {"plm": "esm2_t33"}
    prov = {
        "k": 10,
        "reranker_spec_id": "lgbm-v7",
        "feature_schema_sha": "deadbeefcafe",
        "eval_set_name": "bench-v1-K10-v230",
        "eval_set_manifest_sha": "feedface1234",
        "propagation": "ancestor",
    }
    out = derive_axis_payload(cfg, prov)
    assert out["plm"] == "esm2_t33"
    assert out["k"] == 10
    assert out["reranker_spec_id"] == "lgbm-v7"
    assert out["feature_schema_sha"] == "deadbeefcafe"
    assert out["eval_set_name"] == "bench-v1-K10-v230"
    assert out["eval_set_manifest_sha"] == "feedface1234"
    assert out["propagation"] == "ancestor"


def test_derive_falls_back_to_nested_embedding_config_for_plm() -> None:
    cfg = {"embedding_config": {"plm": "esm3c"}}
    out = derive_axis_payload(cfg, {})
    assert out["plm"] == "esm3c"


def test_derive_falls_back_to_model_id_for_reranker_spec_id() -> None:
    cfg = {"model": {"id": "lgbm-v6"}}
    out = derive_axis_payload(cfg, {})
    assert out["reranker_spec_id"] == "lgbm-v6"


def test_derive_falls_back_to_evaluation_set_for_eval_set_name() -> None:
    cfg = {"evaluation_set": "bench-v1-K5-v226-lineage"}
    out = derive_axis_payload(cfg, {})
    assert out["eval_set_name"] == "bench-v1-K5-v226-lineage"


def test_derive_propagation_defaults_to_none_string() -> None:
    out = derive_axis_payload({}, {})
    assert out["propagation"] == DEFAULT_PROPAGATION


def test_derive_returns_keys_for_every_canonical_axis() -> None:
    out = derive_axis_payload({}, {})
    assert set(out.keys()) == set(CANONICAL_AXIS_KEYS)


def test_derive_handles_none_jsonb() -> None:
    # JSONB columns are NOT NULL on the table, but the helper defensively
    # accepts None so callers can pass row["config"] without checking.
    out = derive_axis_payload(None, None)
    assert out["propagation"] == DEFAULT_PROPAGATION
    assert out["plm"] is None


def test_derive_coerces_k_to_int() -> None:
    # JSONB may stringify numeric values depending on producer; the
    # script coerces to int so the typed Integer column accepts it.
    out = derive_axis_payload({"k": "5"}, {})
    assert out["k"] == 5
    assert isinstance(out["k"], int)


def test_derive_falsy_values_are_treated_as_missing() -> None:
    # Empty string, empty list, empty dict, and None all mean "absent"
    # so the fallback chain walks past them.
    cfg = {"plm": ""}
    prov = {"plm": "esm2_t33"}
    out = derive_axis_payload(cfg, prov)
    assert out["plm"] == "esm2_t33"


# ----------------------------------------------------------------- shortid


def test_shortid_required_keys_excludes_propagation_and_ensemble() -> None:
    # propagation defaults to "none"; ensemble_spec is legitimately None
    # for single-model runs. Both feed the digest but neither gates the
    # shortid being computable.
    assert "propagation" not in SHORTID_REQUIRED_KEYS
    assert "ensemble_spec" not in SHORTID_REQUIRED_KEYS
    assert set(SHORTID_REQUIRED_KEYS) == {
        "plm",
        "k",
        "reranker_spec_id",
        "feature_schema_sha",
        "eval_set_name",
        "eval_set_manifest_sha",
    }


def test_shortid_computed_when_required_keys_present() -> None:
    payload = {
        "plm": "esm2_t33",
        "k": 5,
        "reranker_spec_id": "lgbm-v6",
        "feature_schema_sha": "0123456789ab",
        "eval_set_name": "bench-v1-K5-v226",
        "eval_set_manifest_sha": "cafe1234beef",
        "propagation": "none",
        "ensemble_spec": None,
    }
    out = _compute_shortid_if_complete(payload)
    assert out is not None
    assert out == axis_tuple_shortid(payload)


def test_shortid_is_none_when_required_key_missing() -> None:
    payload = {
        "plm": "esm2_t33",
        "k": 5,
        # reranker_spec_id missing
        "feature_schema_sha": "0123456789ab",
        "eval_set_name": "bench-v1-K5-v226",
        "eval_set_manifest_sha": "cafe1234beef",
        "propagation": "none",
        "ensemble_spec": None,
    }
    assert _compute_shortid_if_complete(payload) is None


def test_shortid_unchanged_by_ensemble_spec_being_none_vs_absent() -> None:
    # JSON serialises ``None`` as ``null``; an absent key drops from the
    # dict. The two are *distinct* digests by design (so two cells with
    # an explicit no-ensemble marker do not collide with cells whose
    # provenance simply omitted the field). The required-keys check
    # treats both as "non-blocking" for shortid completeness.
    base = {
        "plm": "esm2",
        "k": 5,
        "reranker_spec_id": "lgbm-v6",
        "feature_schema_sha": "0123456789ab",
        "eval_set_name": "bench-v1",
        "eval_set_manifest_sha": "deadbeef",
        "propagation": "none",
    }
    a = _compute_shortid_if_complete({**base, "ensemble_spec": None})
    b = _compute_shortid_if_complete(base)  # no ensemble_spec key at all
    assert a is not None and b is not None
    assert a != b  # distinct digests, as documented
