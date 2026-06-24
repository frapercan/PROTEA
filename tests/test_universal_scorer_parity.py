"""Universal reranker scorer: correctness + core/runtime parity.

Covers the F-RERANK-UNIVERSAL inference wiring:

* :mod:`protea.core._universal_reranker` (the DB-backed predict path) and
  ``apps/method_runtime/universal_scorer`` (the container path) must produce
  bit-identical scores for the same booster + rows, so the two copies of the
  encoding algorithm cannot drift.
* The encoding reproduces the lab's authoritative staging: ``plm_id`` is
  vocab-encoded to its int code, ``k_context`` is injected as the model
  constant, string categoricals are vocab-encoded (None / unseen -> -1), and
  raw LambdaRank scores are sigmoid-squashed into (0, 1).
* ``is_universal_booster`` detects the universal layout (plm_id + k_context
  both present) and rejects a per-cell booster.
"""

from __future__ import annotations

import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[1]
_RUNTIME_DIR = _REPO_ROOT / "apps" / "method_runtime"
if str(_RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(_RUNTIME_DIR))

import universal_scorer as rt_uni  # noqa: E402  (apps/method_runtime)

from protea.core import _universal_reranker as core_uni  # noqa: E402

# Minimal universal feature layout: a couple of numerics, the injected
# constants, and the categoricals (no ``aspect`` — it is not a feature).
_FEATURES = [
    "distance",
    "vote_count",
    "k_context",
    "qualifier",
    "evidence_code",
    "taxonomic_relation",
    "plm_id",
]
_CAT_IDX = [3, 4, 5, 6]  # qualifier, evidence_code, taxonomic_relation, plm_id

_CAT_CODES = {
    "qualifier": ["", "enables", "involved_in", "located_in"],
    "evidence_code": ["EXP", "IEA", "ISS"],
    "taxonomic_relation": ["same", "close", "distant", "unrelated"],
    "plm_id": ["prot_t5"],
}


def _train_universal_booster(seed: int = 0) -> lgb.Booster:
    """Train a tiny LambdaRank booster over the universal feature layout."""
    rng = np.random.default_rng(seed)
    n = 600
    x = np.empty((n, len(_FEATURES)), dtype=np.float32)
    x[:, 0] = rng.random(n)  # distance
    x[:, 1] = rng.integers(0, 5, n)  # vote_count
    x[:, 2] = 10.0  # k_context
    x[:, 3] = rng.integers(-1, 4, n)  # qualifier codes
    x[:, 4] = rng.integers(-1, 3, n)  # evidence_code codes
    x[:, 5] = rng.integers(-1, 4, n)  # taxonomic_relation codes
    x[:, 6] = 0  # plm_id -> prot_t5
    # label correlated with vote_count so the booster learns something.
    y = (x[:, 1] + rng.random(n) > 2.5).astype(int)
    group = [20] * (n // 20)
    ds = lgb.Dataset(
        x,
        label=y,
        feature_name=list(_FEATURES),
        categorical_feature=list(_CAT_IDX),
        group=group,
    )
    return lgb.train(
        {"objective": "lambdarank", "num_leaves": 7, "min_data_in_leaf": 5,
         "verbose": -1, "seed": seed},
        ds,
        num_boost_round=15,
    )


def _sample_rows() -> pd.DataFrame:
    """Feature-complete prediction rows, including out-of-vocab + None cats."""
    return pd.DataFrame(
        [
            {"distance": 0.1, "vote_count": 4, "qualifier": "enables",
             "evidence_code": "EXP", "taxonomic_relation": "same"},
            {"distance": 0.9, "vote_count": 0, "qualifier": "located_in",
             "evidence_code": "IEA", "taxonomic_relation": "distant"},
            {"distance": 0.5, "vote_count": 2, "qualifier": None,
             "evidence_code": "NOT_IN_VOCAB", "taxonomic_relation": "close"},
            {"distance": 0.3, "vote_count": 1, "qualifier": "",
             "evidence_code": "ISS", "taxonomic_relation": "unrelated"},
        ]
    )


def test_core_and_runtime_scorers_are_bit_identical() -> None:
    booster = _train_universal_booster()
    df = _sample_rows()
    core_scores = core_uni.score_universal(
        booster, df, categorical_codes=_CAT_CODES, plm_id="prot_t5", k_context=10
    )
    rt_scores = rt_uni.score_universal(
        booster, df, categorical_codes=_CAT_CODES, plm_id="prot_t5", k_context=10
    )
    assert core_scores.shape == (len(df),)
    np.testing.assert_array_equal(core_scores, rt_scores)


def test_scores_are_in_unit_interval() -> None:
    booster = _train_universal_booster()
    scores = core_uni.score_universal(
        booster, _sample_rows(), categorical_codes=_CAT_CODES,
        plm_id="prot_t5", k_context=10,
    )
    assert np.all(scores > 0.0)
    assert np.all(scores < 1.0)


def test_matches_manual_injected_encoding() -> None:
    """The scorer must reproduce the lab encoding: explicit X build, same scores."""
    booster = _train_universal_booster()
    df = _sample_rows()
    feat = booster.feature_name()
    code_maps = {c: {v: i for i, v in enumerate(vals)} for c, vals in _CAT_CODES.items()}
    cat_set = set(rt_uni.UNIVERSAL_CATEGORICAL_COLUMNS)
    x = np.empty((len(df), len(feat)), dtype=np.float32)
    for j, c in enumerate(feat):
        if c == "plm_id":
            x[:, j] = code_maps["plm_id"]["prot_t5"]
        elif c == "k_context":
            x[:, j] = 10.0
        elif c in cat_set:
            cm = code_maps[c]
            x[:, j] = [
                -1 if (v is None or (isinstance(v, float) and v != v)) else cm.get(v, -1)
                for v in df[c].to_numpy(dtype=object)
            ]
        else:
            x[:, j] = pd.to_numeric(df[c], errors="coerce").to_numpy(dtype=np.float32)
    expected = 1.0 / (1.0 + np.exp(-np.asarray(booster.predict(x))))
    got = core_uni.score_universal(
        booster, df, categorical_codes=_CAT_CODES, plm_id="prot_t5", k_context=10
    )
    np.testing.assert_allclose(got, expected, rtol=0, atol=0)


def test_is_universal_booster_detection() -> None:
    booster = _train_universal_booster()
    assert core_uni.is_universal_booster(booster) is True
    assert rt_uni.is_universal_booster(booster) is True


def test_per_cell_booster_is_not_universal() -> None:
    """A booster without plm_id + k_context is not a universal model."""
    rng = np.random.default_rng(1)
    x = rng.random((100, 2)).astype(np.float32)
    y = (x[:, 0] > 0.5).astype(int)
    ds = lgb.Dataset(x, label=y, feature_name=["distance", "vote_count"])
    booster = lgb.train({"objective": "binary", "verbose": -1}, ds, num_boost_round=3)
    assert core_uni.is_universal_booster(booster) is False


def test_unseen_plm_maps_to_missing_code() -> None:
    """An unseen plm_id must encode to CAT_MISSING_CODE, not crash."""
    booster = _train_universal_booster()
    scores = core_uni.score_universal(
        booster, _sample_rows(), categorical_codes=_CAT_CODES,
        plm_id="esm2_unseen", k_context=10,
    )
    assert scores.shape == (4,)


def test_empty_frame_returns_empty() -> None:
    booster = _train_universal_booster()
    empty = pd.DataFrame(
        columns=["distance", "vote_count", "qualifier", "evidence_code",
                 "taxonomic_relation"]
    )
    scores = core_uni.score_universal(
        booster, empty, categorical_codes=_CAT_CODES, plm_id="prot_t5", k_context=10
    )
    assert scores.shape == (0,)


def test_universal_meta_from_run_single_source() -> None:
    run = {"multi_manifest_pool": [{"plm_id": "prot_t5", "k_context": 10}]}
    meta = core_uni.universal_meta_from_run(run)
    assert meta == {"plm_id": "prot_t5", "k_context": 10.0}


def test_universal_meta_from_run_missing_pool() -> None:
    assert core_uni.universal_meta_from_run({}) is None


def test_runtime_load_universal_meta(tmp_path: Path) -> None:
    import json

    rdir = tmp_path / "reranker"
    rdir.mkdir()
    (rdir / "universal_run.json").write_text(
        json.dumps(
            {
                "categorical_codes": _CAT_CODES,
                "multi_manifest_pool": [{"plm_id": "prot_t5", "k_context": 10}],
            }
        )
    )
    meta = rt_uni.load_universal_meta(tmp_path)
    assert meta is not None
    assert meta["plm_id"] == "prot_t5"
    assert meta["k_context"] == 10.0
    assert meta["categorical_codes"]["plm_id"] == ["prot_t5"]


def test_runtime_load_universal_meta_absent(tmp_path: Path) -> None:
    assert rt_uni.load_universal_meta(tmp_path) is None
