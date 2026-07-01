"""Regression tests for the ad-hoc ``aspect_code`` reranker feature.

The registered per-category boosters (e.g. sha ``851849df48e2``) list
``aspect_code`` in their LightGBM ``feature_name()`` and split on it as an
INTEGER encoding of the GO aspect (the offline
``protea-reranker-lab.train_serve_reranker`` bakes
``df["aspect_code"] = df["aspect"].map({"mfo": 0, "bpo": 1, "cco": 2})``).
``aspect_code`` is NOT part of the governed ``protea_contracts`` feature
schema, so the serve/eval feature-prep never built it: it reached LightGBM
as an ``object`` column and blew up ``run_cafa_evaluation`` (and any serve
reranker scoring) with::

    ValueError: pandas dtypes must be int, float or bool.
    Fields with bad pandas dtypes: aspect_code: object

These tests pin the fix: :func:`prepare_reranker_frame` derives the INT
``aspect_code`` when the booster expects it, and guards any other
ungoverned object column with a clear, named error.
"""

from __future__ import annotations

import lightgbm as lgb
import numpy as np
import pandas as pd
import pytest

from protea.core.domain.aspect import reranker_aspect_code
from protea.core.reranker import (
    RerankerFeatureDtypeError,
    predict,
    prepare_reranker_frame,
)


def _fit_booster_with_features(feature_cols: list[str], seed: int = 7) -> lgb.Booster:
    """Train a tiny binary booster whose ``feature_name()`` == ``feature_cols``.

    All columns are treated as plain numeric features (that is exactly how
    ``aspect_code`` is trained in the lab: an integer numeric column, not a
    LightGBM categorical), so the booster ends up splitting on the int codes.
    """
    rng = np.random.RandomState(seed)
    n = 240
    labels = (rng.random(n) < 0.4).astype(int)
    data: dict[str, list] = {}
    for col in feature_cols:
        if col == "aspect_code":
            data[col] = rng.randint(0, 3, n).tolist()
        else:
            data[col] = (rng.random(n) + labels * 0.3).tolist()
    x = pd.DataFrame(data)
    dataset = lgb.Dataset(x, label=labels, free_raw_data=False)
    return lgb.train(
        {"objective": "binary", "metric": "binary_logloss", "verbose": -1, "seed": seed},
        dataset,
        num_boost_round=15,
    )


# ---------------------------------------------------------------------------
# reranker_aspect_code (domain mapping)
# ---------------------------------------------------------------------------


class TestRerankerAspectCode:
    def test_single_char_wire_codes(self) -> None:
        # F=MFO=0, P=BPO=1, C=CCO=2 (NOT the enum P->F->C iteration order).
        assert reranker_aspect_code("F") == 0
        assert reranker_aspect_code("P") == 1
        assert reranker_aspect_code("C") == 2

    def test_three_char_cafa_codes_any_case(self) -> None:
        assert reranker_aspect_code("mfo") == 0
        assert reranker_aspect_code("BPO") == 1
        assert reranker_aspect_code("Cco") == 2

    def test_empty_or_unknown_is_none(self) -> None:
        assert reranker_aspect_code("") is None
        assert reranker_aspect_code(None) is None
        assert reranker_aspect_code("XX") is None


# ---------------------------------------------------------------------------
# prepare_reranker_frame: aspect_code derivation
# ---------------------------------------------------------------------------


class TestPrepareRerankerFrameAspectCode:
    def test_builds_int_aspect_code_from_cafa_codes(self) -> None:
        booster = _fit_booster_with_features(["distance", "aspect_code"])
        df = pd.DataFrame(
            {
                "distance": [0.1, 0.2, 0.3],
                "aspect": ["mfo", "bpo", "cco"],
            }
        )
        out = prepare_reranker_frame(booster, df)
        assert list(out["aspect_code"]) == [0, 1, 2]
        assert pd.api.types.is_integer_dtype(out["aspect_code"])
        # The source frame is never mutated in place.
        assert "aspect_code" not in df.columns

    def test_builds_int_aspect_code_from_single_char_codes(self) -> None:
        booster = _fit_booster_with_features(["distance", "aspect_code"])
        df = pd.DataFrame(
            {
                "distance": [0.1, 0.2, 0.3],
                "aspect": ["F", "P", "C"],
            }
        )
        out = prepare_reranker_frame(booster, df)
        assert list(out["aspect_code"]) == [0, 1, 2]
        assert pd.api.types.is_integer_dtype(out["aspect_code"])

    def test_booster_scores_without_object_dtype_error(self) -> None:
        # The regression: a booster that expects aspect_code must score a
        # small frame carrying only the string aspect, no dtype crash.
        booster = _fit_booster_with_features(["distance", "vote_count", "aspect_code"])
        df = pd.DataFrame(
            {
                "distance": [0.1, 0.5, 0.9, 0.2],
                "vote_count": [3, 1, 5, 2],
                "aspect": ["mfo", "bpo", "cco", "F"],
            }
        )
        out = prepare_reranker_frame(booster, df)
        scores = predict(booster, out)
        assert scores.shape == (4,)
        assert np.all(np.isfinite(scores))

    def test_raw_predict_without_prepare_raises(self) -> None:
        # Proves the bug is real: feeding the string-aspect frame straight to
        # predict (no prepare) trips LightGBM's object-dtype error, because
        # aspect_code lands as an un-coerced NA/object column.
        booster = _fit_booster_with_features(["distance", "aspect_code"])
        df = pd.DataFrame({"distance": [0.1, 0.2], "aspect": ["mfo", "bpo"]})
        with pytest.raises(ValueError, match="aspect_code"):
            predict(booster, df)

    def test_missing_aspect_column_leaves_frame_untouched(self) -> None:
        # No aspect column at all: nothing to derive, no crash (aspect_code
        # then flows through predict's own missing-column NA fill).
        booster = _fit_booster_with_features(["distance", "aspect_code"])
        df = pd.DataFrame({"distance": [0.1, 0.2]})
        out = prepare_reranker_frame(booster, df)
        assert "aspect_code" not in out.columns

    def test_booster_without_aspect_code_is_untouched(self) -> None:
        booster = _fit_booster_with_features(["distance", "vote_count"])
        df = pd.DataFrame({"distance": [0.1, 0.2], "vote_count": [1, 2], "aspect": ["mfo", "bpo"]})
        out = prepare_reranker_frame(booster, df)
        assert "aspect_code" not in out.columns


# ---------------------------------------------------------------------------
# prepare_reranker_frame: ungoverned object-column guard
# ---------------------------------------------------------------------------


class TestUngovernedObjectGuard:
    def test_raises_named_error_on_ungoverned_object_column(self) -> None:
        booster = _fit_booster_with_features(["distance", "made_up_feature"])
        df = pd.DataFrame({"distance": [0.1, 0.2], "made_up_feature": ["a", "b"]})
        with pytest.raises(RerankerFeatureDtypeError, match="made_up_feature"):
            prepare_reranker_frame(booster, df)

    def test_governed_numeric_object_column_does_not_raise(self) -> None:
        # A governed NUMERIC feature arriving as object is fine: the scorer
        # numeric-coerces it downstream, so the guard must not flag it.
        booster = _fit_booster_with_features(["distance"])
        df = pd.DataFrame({"distance": ["0.1", "0.2"]})
        out = prepare_reranker_frame(booster, df)
        assert list(out["distance"]) == ["0.1", "0.2"]
