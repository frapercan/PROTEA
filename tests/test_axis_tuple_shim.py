"""Tests for the PROTEA-side axis-tuple shim (FARM-EXP.1).

The shim at :mod:`protea.core.axis_tuple` re-exports the canonical
helper from :mod:`protea_contracts.axis_tuple` when available and falls
back to a byte-identical local implementation otherwise. These tests
pin the contract surface in PROTEA itself so a renamed / deleted helper
upstream is caught locally.
"""

from __future__ import annotations

import hashlib
import json
import re

import pytest

from protea.core.axis_tuple import (
    CANONICAL_AXIS_KEYS,
    SHORTID_HEX_LEN,
    axis_tuple_shortid,
)

# ---------------------------------------------------------------- format


def test_returns_lowercase_hex_of_fixed_length() -> None:
    out = axis_tuple_shortid({"plm": "esm2_t33", "k": 5})
    assert re.fullmatch(r"[0-9a-f]{12}", out)
    assert len(out) == SHORTID_HEX_LEN


# ---------------------------------------------------------------- canon


def test_canonical_axis_keys_matches_orm_columns() -> None:
    # Pinning the 8 axis names that the FARM-EXP.1 alembic migration
    # adds to ``experiment_run``. Renames here force a SemVer-major bump
    # because the cell catalog join key changes.
    assert CANONICAL_AXIS_KEYS == (
        "plm",
        "k",
        "reranker_spec_id",
        "feature_schema_sha",
        "eval_set_name",
        "eval_set_manifest_sha",
        "propagation",
        "ensemble_spec",
    )


# ---------------------------------------------------------------- stable


def test_golden_vector_pins_the_formula() -> None:
    payload = {
        "plm": "esm2_t33_650M",
        "k": 5,
        "reranker_spec_id": "lgbm-v6",
        "feature_schema_sha": "0123456789ab",
        "eval_set_name": "bench-v1-K5-v226",
        "eval_set_manifest_sha": "cafe1234beef",
        "propagation": "none",
        "ensemble_spec": None,
    }
    expected = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()
    ).hexdigest()[:12]
    assert axis_tuple_shortid(payload) == expected


def test_order_invariance() -> None:
    a = {"k": 5, "plm": "esm2"}
    b = {"plm": "esm2", "k": 5}
    assert axis_tuple_shortid(a) == axis_tuple_shortid(b)


def test_unencodable_value_raises_typeerror() -> None:
    with pytest.raises(TypeError, match="JSON-encodable"):
        axis_tuple_shortid({"plm": {1, 2, 3}})  # type: ignore[dict-item]


def test_shim_matches_lab_experimentspec_hash_formula() -> None:
    # Cross-check against the lab ExperimentSpec.hash() formula at
    # protea-reranker-lab/src/protea_reranker_lab/experiment.py:108-111.
    payload = {
        "schema_version": "v1",
        "name": "lgbm-v6-bench",
        "dataset": {"manifest": "/tmp/m.parquet"},
        "model": {"kind": "lgbm_reranker", "defaults": {}},
        "training": {
            "cell": "bp",
            "val_strategy": "temporal",
            "val_fraction": 0.2,
            "val_holdout_snapshot": "v226",
            "seed": 42,
            "propagate_labels": False,
            "neg_pos_ratio": None,
        },
        "sweep": {"backend": "none", "project": None, "config": None},
        "keep_staging": False,
    }
    expected = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()
    ).hexdigest()[:12]
    assert axis_tuple_shortid(payload) == expected
