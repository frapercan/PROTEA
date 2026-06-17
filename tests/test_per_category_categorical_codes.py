"""B1 (NFR-PERF): categorical-vocabulary wiring for per-category boosters.

The per-category (NK / LK / PK) boosters are NOT universal (no ``plm_id`` /
``k_context`` features), so the live predict path routes them through the
generic ``protea_method.reranker`` helpers. Before this slice the only generic
entry point was ``apply_reranker``, which numeric-coerces every column and so
turns the STRING categoricals (``aspect`` / ``qualifier`` / ``evidence_code`` /
``taxonomic_relation``) into NaN at predict time. A booster trained with real
categorical codes therefore saw NaN at predict (train/predict mismatch).

These tests pin the fix:

* ``load_per_category_categorical_codes`` recovers the per-column ``factorize``
  vocabulary from the booster's sibling run.json, and is tolerant (missing /
  malformed -> ``None`` -> backward-compatible bare ``apply_reranker``).
* the scorer routes a non-universal booster through ``predict(..., codes)``
  when the vocabulary is present and through ``apply_reranker`` when it is not.
* the encoding round-trips: the code a value gets at PREDICT equals the index
  the value had in the training ``factorize`` order; an unknown value -> -1.
* the registration script threads ``--categorical-codes-json`` onto the run
  body.
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from protea.core import _universal_reranker as uni
from protea.core.operations.predict_go_terms._common import PredictGOTermsBatchPayload
from protea.core.operations.predict_go_terms._reranker_scorer import RerankerScorer

# Per-cell / per-category categorical layout (the four real categoricals,
# ``aspect`` included -- unlike the universal model, aspect IS a feature here).
_CAT_CODES = {
    "aspect": ["C", "F", "P"],
    "qualifier": ["", "enables", "involved_in", "located_in"],
    "evidence_code": ["EXP", "IEA", "ISS"],
    "taxonomic_relation": ["same", "close", "distant", "unrelated"],
}


def _fake_store_with_run(run_obj: object | None) -> MagicMock:
    """Return a store whose ``get`` yields ``run_obj`` JSON (or raises)."""
    store = MagicMock()
    if run_obj is None:
        store.get.side_effect = FileNotFoundError("no such key")
    else:
        store.get.return_value = json.dumps(run_obj).encode("utf-8")
    return store


# ---------------------------------------------------------------------------
# load_per_category_categorical_codes
# ---------------------------------------------------------------------------


class TestLoadPerCategoryCategoricalCodes:
    def test_roundtrips_vocab_from_sibling_run_json(self) -> None:
        store = _fake_store_with_run({"categorical_codes": _CAT_CODES})
        out = uni.load_per_category_categorical_codes("s3://bucket/runs/fullgo-lk/model.txt", store)
        assert out == _CAT_CODES
        # The sibling run.json key is derived from the model.txt URI.
        store.get.assert_called_once_with("runs/fullgo-lk/run.json")

    def test_missing_sibling_returns_none(self) -> None:
        store = _fake_store_with_run(None)
        out = uni.load_per_category_categorical_codes("s3://bucket/runs/fullgo-lk/model.txt", store)
        assert out is None

    def test_run_without_categorical_codes_returns_none(self) -> None:
        store = _fake_store_with_run({"run_id": "fullgo-lk", "metrics": {}})
        assert (
            uni.load_per_category_categorical_codes("s3://bucket/runs/fullgo-lk/model.txt", store)
            is None
        )

    def test_malformed_json_returns_none(self) -> None:
        store = MagicMock()
        store.get.return_value = b"{not valid json"
        assert (
            uni.load_per_category_categorical_codes("s3://bucket/runs/fullgo-lk/model.txt", store)
            is None
        )

    def test_non_dict_categorical_codes_returns_none(self) -> None:
        store = _fake_store_with_run({"categorical_codes": ["not", "a", "dict"]})
        assert (
            uni.load_per_category_categorical_codes("s3://bucket/runs/fullgo-lk/model.txt", store)
            is None
        )


# ---------------------------------------------------------------------------
# encoding round-trip: train factorize order == predict codes
# ---------------------------------------------------------------------------


class TestCategoricalEncodingRoundTrip:
    """The codes assigned at PREDICT must equal the training factorize order.

    ``prepare_dataset`` (training) factorizes each categorical with
    ``use_na_sentinel=True`` ("" -> NA -> -1) and the ordered-unique values ARE
    the per-column vocabulary. ``predict(..., categorical_codes=vocab)`` maps a
    value to ``vocab.index(value)`` (-1 for unseen). This test proves the two
    encoders agree for the same value, for every categorical including
    ``aspect``.
    """

    def test_predict_codes_match_training_factorize_order(self) -> None:
        from protea_method.reranker import predict, prepare_dataset

        # A tiny training frame; the factorize order becomes the vocabulary.
        train = pd.DataFrame(
            {
                "distance": [0.1, 0.2, 0.3, 0.4],
                "aspect": ["P", "F", "C", "P"],
                "qualifier": ["enables", "", "located_in", "enables"],
                "evidence_code": ["EXP", "IEA", "ISS", "EXP"],
                "taxonomic_relation": ["same", "close", "distant", "same"],
                "label": [1, 0, 1, 0],
            }
        )
        x_train, _ = prepare_dataset(
            train.assign(
                **{
                    col: 0.0
                    for col in __import__(
                        "protea_method.reranker", fromlist=["NUMERIC_FEATURES"]
                    ).NUMERIC_FEATURES
                    if col not in train.columns
                }
            )
        )
        # Recover the per-column training vocabulary the way the lab records it:
        # the ordered-unique of the non-empty values (factorize order).
        vocab: dict[str, list[str]] = {}
        for col in ("aspect", "qualifier", "evidence_code", "taxonomic_relation"):
            s = train[col].replace("", pd.NA)
            s = s.astype("object").where(s.notna(), None)
            _, uniques = pd.factorize(s, use_na_sentinel=True)
            vocab[col] = [str(u) for u in uniques]

        # Build a feature-name-aware fake booster that just records the X it
        # receives, so we can inspect the encoded categorical columns.
        recorded: dict[str, pd.DataFrame] = {}

        class _Recorder:
            def feature_name(self) -> list[str]:
                return ["distance", "aspect", "qualifier", "evidence_code", "taxonomic_relation"]

            def predict(self, x: pd.DataFrame) -> np.ndarray:
                recorded["x"] = x
                return np.zeros(len(x))

        predict_df = pd.DataFrame(
            {
                "distance": [0.5, 0.6],
                "aspect": ["F", "C"],
                "qualifier": ["located_in", "UNSEEN_QUALIFIER"],
                "evidence_code": ["ISS", "IEA"],
                "taxonomic_relation": ["distant", "same"],
            }
        )
        predict(_Recorder(), predict_df, categorical_codes=vocab)
        x = recorded["x"]

        # aspect "F" -> its index in vocab["aspect"]; "C" -> its index.
        assert int(x["aspect"].iloc[0]) == vocab["aspect"].index("F")
        assert int(x["aspect"].iloc[1]) == vocab["aspect"].index("C")
        # known qualifier -> its code; the UNSEEN one -> -1.
        assert int(x["qualifier"].iloc[0]) == vocab["qualifier"].index("located_in")
        assert int(x["qualifier"].iloc[1]) == -1
        assert int(x["evidence_code"].iloc[0]) == vocab["evidence_code"].index("ISS")
        assert int(x["taxonomic_relation"].iloc[0]) == vocab["taxonomic_relation"].index("distant")


# ---------------------------------------------------------------------------
# scorer wiring: predict(codes) when present, apply_reranker when absent
# ---------------------------------------------------------------------------


_ANN_SET_ID = str(uuid.uuid4())
_SNAPSHOT_ID = str(uuid.uuid4())


def _payload(**kwargs) -> PredictGOTermsBatchPayload:
    defaults = dict(
        embedding_config_id=str(uuid.uuid4()),
        annotation_set_id=_ANN_SET_ID,
        ontology_snapshot_id=_SNAPSHOT_ID,
        prediction_set_id=str(uuid.uuid4()),
        parent_job_id=str(uuid.uuid4()),
        query_accessions=["Q1"],
        limit_per_entry=2,
    )
    defaults.update(kwargs)
    return PredictGOTermsBatchPayload.model_validate(defaults)


def _noop_attach_aspect(_session, _dicts) -> None:
    return None


def _non_universal_booster() -> MagicMock:
    """A booster whose feature_name lacks plm_id / k_context (not universal)."""
    b = MagicMock(name="booster")
    b.feature_name.return_value = ["distance", "aspect", "qualifier"]
    return b


class TestScorerRoutesPerCategoryThroughPredict:
    """``_score_with`` uses ``predict(codes)`` when the vocab is recoverable."""

    def _patches(self, booster: MagicMock, *, predict_ret, apply_ret):
        return (
            patch(
                "protea.core.operations.predict_go_terms._batch_op_reranker.load_reranker",
                return_value=booster,
            ),
            patch(
                "protea.core.operations.predict_go_terms._batch_op_reranker.get_artifact_store",
                return_value=MagicMock(),
            ),
            patch(
                "protea.core.operations.predict_go_terms._batch_op_reranker.load_settings",
                return_value=MagicMock(),
            ),
            patch(
                "protea.core.operations.predict_go_terms._batch_op_reranker.predict",
                return_value=predict_ret,
            ),
            patch(
                "protea.core.operations.predict_go_terms._batch_op_reranker.apply_reranker",
                return_value=apply_ret,
            ),
        )

    def test_vocab_present_routes_through_predict_with_codes(self) -> None:
        scorer = RerankerScorer(attach_aspect=_noop_attach_aspect)
        booster = _non_universal_booster()
        dicts = [
            {
                "protein_accession": "Q1",
                "go_term_id": 1,
                "distance": 0.1,
                "aspect": "P",
                "qualifier": "enables",
            },
            {
                "protein_accession": "Q1",
                "go_term_id": 2,
                "distance": 0.2,
                "aspect": "F",
                "qualifier": "",
            },
        ]
        predict_ret = np.array([0.7, 0.3])
        apply_ret = np.array([9.9, 9.9])  # sentinel: must NOT be used
        p_load, p_store, p_set, p_predict, p_apply = self._patches(
            booster, predict_ret=predict_ret, apply_ret=apply_ret
        )
        with (
            p_load,
            p_store,
            p_set,
            p_predict as m_predict,
            p_apply as m_apply,
            patch(
                "protea.core._universal_reranker.load_per_category_categorical_codes",
                return_value=_CAT_CODES,
            ),
        ):
            scores = scorer._score_with(
                dicts,
                artifact_uri="s3://bucket/runs/fullgo-lk/model.txt",
                feature_schema_sha="sha123",
            )
        assert m_predict.called
        assert not m_apply.called
        # predict is called as predict(booster, df, categorical_codes=...).
        _args, kwargs = m_predict.call_args
        assert kwargs["categorical_codes"] == _CAT_CODES
        assert list(scores) == [0.7, 0.3]
        assert dicts[0]["reranker_score"] == pytest.approx(0.7)
        assert dicts[1]["reranker_score"] == pytest.approx(0.3)

    def test_vocab_absent_falls_back_to_apply_reranker(self) -> None:
        scorer = RerankerScorer(attach_aspect=_noop_attach_aspect)
        booster = _non_universal_booster()
        dicts = [{"protein_accession": "Q1", "go_term_id": 1, "distance": 0.1, "aspect": "P"}]
        predict_ret = np.array([9.9])  # sentinel: must NOT be used
        apply_ret = np.array([0.42])
        p_load, p_store, p_set, p_predict, p_apply = self._patches(
            booster, predict_ret=predict_ret, apply_ret=apply_ret
        )
        with (
            p_load,
            p_store,
            p_set,
            p_predict as m_predict,
            p_apply as m_apply,
            patch(
                "protea.core._universal_reranker.load_per_category_categorical_codes",
                return_value=None,
            ),
        ):
            scores = scorer._score_with(
                dicts,
                artifact_uri="s3://bucket/runs/numeric-only/model.txt",
                feature_schema_sha="sha123",
            )
        assert m_apply.called
        assert not m_predict.called
        assert list(scores) == [0.42]
        assert dicts[0]["reranker_score"] == pytest.approx(0.42)


# ---------------------------------------------------------------------------
# registration script: --categorical-codes-json threads onto the run body
# ---------------------------------------------------------------------------


class TestRegistrationThreadsCategoricalCodes:
    def test_build_request_body_records_categorical_codes(self) -> None:
        from scripts.register_fullgo_boosters import build_request_body

        body = build_request_body(
            category="lk",
            artifact_uri="s3://b/runs/fullgo-lk/model.txt",
            feature_schema_sha="sha123",
            name_prefix="fullgo",
            dataset_id=None,
            external_source=None,
            force=False,
            families=["knn", "annotation_meta"],
            categorical_codes=_CAT_CODES,
        )
        assert body["run"]["categorical_codes"] == _CAT_CODES

    def test_build_request_body_omits_codes_when_none(self) -> None:
        from scripts.register_fullgo_boosters import build_request_body

        body = build_request_body(
            category="lk",
            artifact_uri="s3://b/runs/fullgo-lk/model.txt",
            feature_schema_sha="sha123",
            name_prefix="fullgo",
            dataset_id=None,
            external_source=None,
            force=False,
        )
        assert "categorical_codes" not in body["run"]

    def test_load_categorical_codes_reads_json(self, tmp_path) -> None:
        from scripts.register_fullgo_boosters import _load_categorical_codes

        path = tmp_path / "codes.json"
        path.write_text(json.dumps(_CAT_CODES))
        assert _load_categorical_codes(str(path)) == _CAT_CODES

    def test_load_categorical_codes_none_passthrough(self) -> None:
        from scripts.register_fullgo_boosters import _load_categorical_codes

        assert _load_categorical_codes(None) is None

    def test_load_categorical_codes_rejects_malformed(self, tmp_path) -> None:
        from scripts.register_fullgo_boosters import _load_categorical_codes

        path = tmp_path / "bad.json"
        path.write_text(json.dumps(["not", "a", "dict"]))
        with pytest.raises(SystemExit):
            _load_categorical_codes(str(path))
