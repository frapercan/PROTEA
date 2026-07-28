"""Unit tests for the re-ranker inference module.

Training was moved to ``protea-reranker-lab``; the tests below cover
only the helpers that remain in PROTEA: feature-column constants,
``prepare_dataset``, ``predict``, and model serialization round-trips.
"""

from __future__ import annotations

import lightgbm as lgb
import numpy as np
import pandas as pd

from protea.core.reranker import (
    ALL_FEATURES,
    CATEGORICAL_FEATURES,
    LABEL_COLUMN,
    NUMERIC_FEATURES,
    model_from_string,
    predict,
    prepare_dataset,
)


def _make_training_df(n: int = 200, positive_rate: float = 0.3, seed: int = 42) -> pd.DataFrame:
    """Generate a synthetic DataFrame with realistic feature distributions."""
    rng = np.random.RandomState(seed)

    labels = (rng.random(n) < positive_rate).astype(int)

    data: dict[str, list] = {
        "protein_accession": [f"P{i:05d}" for i in range(n)],
        "go_id": [f"GO:{rng.randint(1, 99999):07d}" for _ in range(n)],
        "aspect": rng.choice(["F", "P", "C"], n).tolist(),
        "label": labels.tolist(),
    }

    for col in NUMERIC_FEATURES:
        if col == "distance":
            data[col] = (rng.random(n) * 0.5 + (1 - labels) * 0.3).tolist()
        elif "identity" in col or "similarity" in col:
            data[col] = (rng.random(n) * 0.5 + labels * 0.3).tolist()
        elif "gaps" in col:
            data[col] = (rng.random(n) * 0.1).tolist()
        elif "score" in col:
            data[col] = (rng.random(n) * 500 + labels * 200).tolist()
        elif "length" in col or "alignment_length" in col:
            data[col] = (rng.randint(100, 1000, n)).tolist()
        elif col == "vote_count":
            data[col] = (rng.randint(1, 10, n) + labels * 2).tolist()
        elif col == "k_position":
            data[col] = (rng.randint(1, 5, n)).tolist()
        elif col == "go_term_frequency":
            data[col] = (rng.randint(1, 100, n)).tolist()
        elif col == "ref_annotation_density":
            data[col] = (rng.randint(1, 50, n)).tolist()
        elif col == "neighbor_distance_std":
            data[col] = (rng.random(n) * 0.1).tolist()
        else:
            data[col] = (rng.random(n) * 10).tolist()

    data["qualifier"] = rng.choice(["enables", "involved_in", "located_in", ""], n).tolist()
    data["evidence_code"] = rng.choice(["IDA", "IEA", "ISS", "EXP", ""], n).tolist()
    data["taxonomic_relation"] = rng.choice(["self", "sibling", "ancestor", ""], n).tolist()
    # plm_id is the pool-injected categorical the lab stamps when it pools
    # several manifests to train a universal booster (contracts v1.4.0). These
    # tests exercise the training path, which sees pooled frames, so the
    # fixture generates it; k_context (its numeric sibling) comes from the
    # NUMERIC_FEATURES loop above.
    data["plm_id"] = rng.choice(["esm2_650m", "ankh_base", "prott5", ""], n).tolist()

    return pd.DataFrame(data)


def _fit_minimal_booster(df: pd.DataFrame, num_boost_round: int = 10) -> lgb.Booster:
    """Train a minimal LightGBM booster inline — replaces the removed
    ``protea.core.reranker.train`` helper, so tests still have a real
    booster to exercise ``predict`` / ``model_from_string`` against.

    Mirrors ``protea-reranker-lab.reranker.encode_categoricals``: label-
    encode categoricals to int codes so the booster trained here matches
    the booster shape PROTEA's ``predict`` (no-label branch) expects at
    inference time.
    """
    X, y = prepare_dataset(df)
    cat_cols = [c for c in CATEGORICAL_FEATURES if c in X.columns]
    for col in cat_cols:
        s = X[col].astype("object").where(X[col].notna(), None)
        codes, _ = pd.factorize(s, use_na_sentinel=True)
        X[col] = codes
    dataset = lgb.Dataset(X, label=y, categorical_feature=cat_cols, free_raw_data=False)
    return lgb.train(
        {
            "objective": "binary",
            "metric": "binary_logloss",
            "verbose": -1,
            "seed": 42,
        },
        dataset,
        num_boost_round=num_boost_round,
    )


# ---------------------------------------------------------------------------
# prepare_dataset
# ---------------------------------------------------------------------------


class TestPrepareDataset:
    def test_returns_correct_shapes(self):
        df = _make_training_df(50)
        X, y = prepare_dataset(df)
        assert X.shape == (50, len(ALL_FEATURES))
        assert y.shape == (50,)

    def test_categorical_columns_are_int_codes(self):
        # Mirror protea-reranker-lab encoding: categoricals are
        # label-encoded to int64 codes (missing → -1), not pandas
        # ``category`` dtype. Required so cross-instance lab boosters
        # don't trip "train and valid dataset categorical_feature do
        # not match" at inference time.
        df = _make_training_df(20)
        X, _ = prepare_dataset(df)
        for col in CATEGORICAL_FEATURES:
            assert X[col].dtype.kind == "i", f"{col} dtype = {X[col].dtype}"

    def test_label_is_int(self):
        df = _make_training_df(20)
        _, y = prepare_dataset(df)
        assert y.dtype == int

    def test_only_feature_columns_in_X(self):
        df = _make_training_df(20)
        X, _ = prepare_dataset(df)
        assert list(X.columns) == ALL_FEATURES
        assert "protein_accession" not in X.columns
        assert "go_id" not in X.columns

    def test_empty_strings_become_minus_one_codes(self):
        # Empty-string qualifier collapses to None → factorize sentinel
        # code -1, matching the lab's encode_categoricals contract.
        df = _make_training_df(20)
        df.loc[0, "qualifier"] = ""
        X, _ = prepare_dataset(df)
        assert int(X.loc[0, "qualifier"]) == -1


# ---------------------------------------------------------------------------
# predict
# ---------------------------------------------------------------------------


class TestPredict:
    def test_returns_probabilities(self):
        df = _make_training_df(200)
        model = _fit_minimal_booster(df)
        scores = predict(model, df)
        assert len(scores) == 200
        assert all(0.0 <= s <= 1.0 for s in scores)

    def test_scores_without_label_column(self):
        df = _make_training_df(200)
        model = _fit_minimal_booster(df)
        df_no_label = df.drop(columns=[LABEL_COLUMN])
        scores = predict(model, df_no_label)
        assert len(scores) == 200


# ---------------------------------------------------------------------------
# Serialization round-trip — scoring router loads stored boosters via
# model_from_string, so the roundtrip must yield identical scores.
# ---------------------------------------------------------------------------


class TestSerialization:
    def test_roundtrip(self):
        df = _make_training_df(200)
        model = _fit_minimal_booster(df)
        model_str = model.model_to_string()
        restored = model_from_string(model_str)
        np.testing.assert_array_almost_equal(predict(model, df), predict(restored, df))


# ---------------------------------------------------------------------------
# Feature constants
# ---------------------------------------------------------------------------


class TestFeatureConstants:
    def test_no_duplicate_features(self):
        assert len(ALL_FEATURES) == len(set(ALL_FEATURES))

    def test_all_features_is_union(self):
        assert ALL_FEATURES == NUMERIC_FEATURES + CATEGORICAL_FEATURES

    def test_numeric_and_categorical_disjoint(self):
        assert set(NUMERIC_FEATURES) & set(CATEGORICAL_FEATURES) == set()


class TestInferActiveFeatureFamiliesLineage:
    """Lineage governance for the PROTEA-side ``infer_active_feature_families``.

    The serve path threads ``compute_lineage_features`` from the payload
    through this helper into ``compute_feature_schema_sha`` so a booster
    trained with the GO-DAG lineage family gets a matching live sha.

    The load-bearing invariant (FARM-EXP.5 schema-sha guard): with the
    flag off (the default) the family list and resulting schema sha are
    byte-identical to today for every align/tax/v6 combination.
    """

    #: The eight align/tax/v6 schema shas that MUST NOT move when the
    #: lineage flag is off. Anchored against contracts' family-aware
    #: ``compute_feature_schema_sha`` (see the registered trio
    #: ``7fcecf26aa0a`` = align+tax, no v6).
    _EXPECTED_OFF_SHAS = {
        (False, False, False): "9ef6a6609424",
        (False, False, True): "2f14dea205b5",
        (False, True, False): "a49027735c06",
        (False, True, True): "ec3df5057de0",
        (True, False, False): "178eb1cfeb36",
        (True, False, True): "dca060fd7996",
        (True, True, False): "7fcecf26aa0a",
        (True, True, True): "94e87ae6f4ed",
    }

    def test_flag_off_is_byte_identical_to_library(self) -> None:
        """Flag off => same families the protea-method library returns."""
        import itertools

        from protea_method.reranker import (
            infer_active_feature_families as lib_infer,
        )

        from protea.core.reranker import infer_active_feature_families

        for ca, ct, v6 in itertools.product([False, True], repeat=3):
            wrapped = infer_active_feature_families(
                compute_alignments=ca,
                compute_taxonomy=ct,
                compute_v6_features=v6,
            )
            lib = lib_infer(
                compute_alignments=ca,
                compute_taxonomy=ct,
                compute_v6_features=v6,
            )
            assert wrapped == lib
            assert "lineage" not in wrapped

    def test_flag_off_schema_shas_unchanged(self) -> None:
        """Flag off => the eight existing schema shas are unchanged."""
        import itertools

        from protea_contracts import compute_feature_schema_sha

        from protea.core.reranker import infer_active_feature_families

        for ca, ct, v6 in itertools.product([False, True], repeat=3):
            families = infer_active_feature_families(
                compute_alignments=ca,
                compute_taxonomy=ct,
                compute_v6_features=v6,
            )
            assert compute_feature_schema_sha(families) == self._EXPECTED_OFF_SHAS[(ca, ct, v6)]

    def test_flag_on_appends_lineage_family(self) -> None:
        """Flag on => exactly the ``lineage`` family is added, nothing else."""
        import itertools

        from protea.core.reranker import infer_active_feature_families

        for ca, ct, v6 in itertools.product([False, True], repeat=3):
            off = infer_active_feature_families(
                compute_alignments=ca,
                compute_taxonomy=ct,
                compute_v6_features=v6,
            )
            on = infer_active_feature_families(
                compute_alignments=ca,
                compute_taxonomy=ct,
                compute_v6_features=v6,
                compute_lineage_features=True,
            )
            assert on == sorted({*off, "lineage"})
            assert on == sorted(on)  # contract: returns a sorted list

    def test_flag_on_yields_new_lineage_inclusive_sha(self) -> None:
        """Flag on for the registered trio => the lineage-inclusive sha."""
        from protea_contracts import compute_feature_schema_sha

        from protea.core.reranker import infer_active_feature_families

        on = infer_active_feature_families(
            compute_alignments=True,
            compute_taxonomy=True,
            compute_v6_features=False,
            compute_lineage_features=True,
        )
        assert compute_feature_schema_sha(on) == "0810bef8fd4d"
        # And it differs from the flag-off trio sha (no silent collision).
        assert "0810bef8fd4d" != self._EXPECTED_OFF_SHAS[(True, True, False)]
