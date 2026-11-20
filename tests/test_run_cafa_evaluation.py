"""Unit tests for RunCafaEvaluationOperation.

No real DB, network, or cafaeval binary required — everything is mocked.
"""

from __future__ import annotations

import gzip
import os
import tempfile
import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pydantic import ValidationError

from protea.core.band_registry import BandMismatchError
from protea.core.evaluation import EvaluationData
from protea.core.operations.run_cafa_evaluation import (
    _NS_LABELS,
    _NS_SHORT,
    RunCafaEvaluationOperation,
    RunCafaEvaluationPayload,
)


@pytest.fixture(autouse=True)
def _every_set_here_is_scored(monkeypatch: pytest.MonkeyPatch) -> None:
    """The sessions here are doubles, and the check that a set stores scores is a
    query; it is tested against Postgres in test_method_score_predictions.py."""
    monkeypatch.setattr(
        "protea.core.operations.run_cafa_evaluation.refuse_unless_scored",
        lambda _session, _set_id: None,
    )


def _gets(eval_set=None, pred_set=None, snapshot=None, config=None, scoring_cfg=None):
    """A ``session.get`` keyed by model rather than by call order.

    The positional form, ``side_effect=[eval_set, pred_set, snapshot]``,
    breaks the moment the code under test reads one more row, whatever the
    row is and wherever it is read. It broke here when the evaluation
    started refusing an encoding whose training cut it could not certify,
    which needs the config and the frame's two annotation sets.

    Keyed on the model, the double answers what it is asked for and is
    silent about order, so a new read is only a failure if the test cares
    about it.
    """

    answers = {
        "EvaluationSet": eval_set,
        "PredictionSet": pred_set,
        "OntologySnapshot": snapshot,
        "EmbeddingConfig": config,
        "ScoringConfig": scoring_cfg,
        "AnnotationSet": None,
    }

    def get(model, pk):  # noqa: ANN001
        # Matched by containment rather than by an exact __name__, because a
        # test that patches a model class hands this a MagicMock whose
        # __name__ is itself a MagicMock. Its repr still carries the patched
        # target, so the name is recoverable from the text either way.
        name = getattr(model, "__name__", None)
        text = name if isinstance(name, str) else repr(model)
        for key, value in answers.items():
            if key in text:
                return value
        return None

    return get


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

EVAL_SET_ID = str(uuid.uuid4())
PRED_SET_ID = str(uuid.uuid4())
OLD_ANN_SET_ID = uuid.uuid4()
NEW_ANN_SET_ID = uuid.uuid4()
SNAP_ID = uuid.uuid4()
SCORING_CONFIG_ID = str(uuid.uuid4())


def _make_emit():
    """Return a mock emit function that records all calls."""
    return MagicMock()


def _make_eval_set(eval_set_id=None):
    es = MagicMock()
    es.id = uuid.UUID(eval_set_id or EVAL_SET_ID)
    es.old_annotation_set_id = OLD_ANN_SET_ID
    es.new_annotation_set_id = NEW_ANN_SET_ID
    return es


def _make_pred_set(pred_set_id=None):
    ps = MagicMock()
    ps.id = uuid.UUID(pred_set_id or PRED_SET_ID)
    # A set no method row produced, as every set before ADR-D60's method
    # tables is. Left to the mock, method_id would be a truthy MagicMock and the
    # leakage guard would read it as a method the record does not hold.
    ps.method_id = None
    return ps


def _make_ann_old():
    ann = MagicMock()
    ann.ontology_snapshot_id = SNAP_ID
    return ann


def _make_snapshot(
    obo_url="https://example.com/go.obo",
    ia_url=None,
    obo_version=None,
    obo_uri=None,
    obo_sha256=None,
):
    snap = MagicMock()
    snap.obo_url = obo_url
    snap.ia_url = ia_url
    snap.obo_version = obo_version
    # ADR-D47 archive fields. These MUST be set explicitly: on a bare MagicMock
    # every attribute is truthy, so _resolve_obo would take the archive branch
    # for a snapshot that has none and compare the downloaded bytes against a
    # mock hash. Default None matches a snapshot loaded before archival.
    snap.obo_uri = obo_uri
    snap.obo_sha256 = obo_sha256
    return snap


def _make_eval_data(nk=None, lk=None, pk=None, known=None, pk_known=None):
    return EvaluationData(
        nk=nk or {"P1": {"GO:0000001"}},
        lk=lk or {"P2": {"GO:0000002"}},
        pk=pk or {},
        known=known or {},
        pk_known=pk_known or {},
    )


def _dfs_best_fixture(*, with_weighted: bool = False):
    """Build a dfs_best dict matching cafaeval output format.

    When ``with_weighted`` is True, also include the ``f_w``,
    ``f_micro`` and ``f_micro_w`` frames cafaeval emits when an IA
    file is supplied.
    """
    df_f = pd.DataFrame(
        [
            {
                "ns": "biological_process",
                "f": 0.45,
                "pr": 0.51,
                "rc": 0.40,
                "tau": 0.32,
                "cov_max": 0.95,
                "n": 100,
            },
            {
                "ns": "molecular_function",
                "f": 0.60,
                "pr": 0.65,
                "rc": 0.55,
                "tau": 0.20,
                "cov_max": 0.88,
                "n": 50,
            },
            {
                "ns": "cellular_component",
                "f": 0.70,
                "pr": 0.72,
                "rc": 0.68,
                "tau": 0.15,
                "cov_max": 0.92,
                "n": 75,
            },
        ]
    )
    out: dict[str, Any] = {"f": df_f}
    if with_weighted:
        out["f_w"] = pd.DataFrame(
            [
                {"ns": "biological_process", "f_w": 0.40},
                {"ns": "molecular_function", "f_w": 0.55},
                {"ns": "cellular_component", "f_w": 0.62},
            ]
        )
        out["f_micro"] = pd.DataFrame(
            [
                {"ns": "biological_process", "f_micro": 0.30},
                {"ns": "molecular_function", "f_micro": 0.50},
                {"ns": "cellular_component", "f_micro": 0.58},
            ]
        )
        out["f_micro_w"] = pd.DataFrame(
            [
                {
                    "ns": "biological_process",
                    "f_micro_w": 0.25,
                    "pr_micro_w": 0.33,
                    "rc_micro_w": 0.20,
                    "cov_max": 0.94,
                },
                {
                    "ns": "molecular_function",
                    "f_micro_w": 0.45,
                    "pr_micro_w": 0.50,
                    "rc_micro_w": 0.41,
                    "cov_max": 0.87,
                },
                {
                    "ns": "cellular_component",
                    "f_micro_w": 0.50,
                    "pr_micro_w": 0.55,
                    "rc_micro_w": 0.46,
                    "cov_max": 0.91,
                },
            ]
        )
    return out


# ---------------------------------------------------------------------------
# Payload validation
# ---------------------------------------------------------------------------


class TestRunCafaEvaluationPayload:
    def test_valid_payload(self):
        p = RunCafaEvaluationPayload(
            evaluation_set_id=EVAL_SET_ID,
            prediction_set_id=PRED_SET_ID,
        )
        assert p.evaluation_set_id == EVAL_SET_ID
        assert p.prediction_set_id == PRED_SET_ID
        assert p.ia_file is None

    def test_lafa_parity_defaults(self):
        # The defaults must reproduce LAFA's cafaeval invocation exactly:
        # th_step=0.01 (cafaeval default), no max_terms cap, snapshot TOI.
        # See docs/EVAL_LAFA_PARITY.md. A finer th_step (e.g. 0.001) would
        # inflate f_micro_w and break numeric parity with LAFA.
        p = RunCafaEvaluationPayload(
            evaluation_set_id=EVAL_SET_ID,
            prediction_set_id=PRED_SET_ID,
        )
        assert p.th_step == 0.01
        assert p.max_terms is None
        assert p.toi_file is None

    def test_th_step_out_of_range(self):
        with pytest.raises(ValidationError):
            RunCafaEvaluationPayload(
                evaluation_set_id=EVAL_SET_ID,
                prediction_set_id=PRED_SET_ID,
                th_step=0.0,
            )

    def test_max_terms_must_be_positive(self):
        with pytest.raises(ValidationError):
            RunCafaEvaluationPayload(
                evaluation_set_id=EVAL_SET_ID,
                prediction_set_id=PRED_SET_ID,
                max_terms=0,
            )

    @pytest.mark.parametrize(
        ("field", "value", "pointer"),
        [
            ("scoring_config_id", SCORING_CONFIG_ID, "score_prediction_set"),
            ("max_distance", 1.5, "score_prediction_set"),
            ("max_k_position", 3, "score_prediction_set"),
            ("max_sequence_rank", 3, "score_prediction_set"),
            ("reranker_id_nk", SCORING_CONFIG_ID, "when predicting"),
            ("rerankers", {"nk": {"bpo": SCORING_CONFIG_ID}}, "when predicting"),
            ("interpro_graft", True, "on top of a reranker"),
            ("softprop", True, "stores them propagated"),
        ],
    )
    def test_a_retired_field_asking_for_work_is_refused_with_where_it_is_done(
        self, field, value, pointer
    ):
        with pytest.raises(ValidationError, match=pointer):
            RunCafaEvaluationPayload.model_validate(
                {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID, field: value}
            )

    def test_every_retired_field_has_a_pointer(self):
        from protea.core.operations._run_cafa_payload import RETIRED_FIELDS

        assert set(RETIRED_FIELDS) == {
            "scoring_config_id",
            "max_distance",
            "max_k_position",
            "max_sequence_rank",
            "reranker_id_nk",
            "reranker_id_lk",
            "reranker_id_pk",
            "rerankers",
            "interpro_graft",
            "interpro_protein2ipr_file",
            "interpro_ipr2go_file",
            "interpro_graft_weight",
            "softprop",
        }
        assert all(RETIRED_FIELDS.values())

    def test_a_retired_field_left_empty_asked_for_nothing_and_is_accepted(self):
        """A caller built before the change sends the old defaults; they ask for nothing."""
        p = RunCafaEvaluationPayload.model_validate(
            {
                "evaluation_set_id": EVAL_SET_ID,
                "prediction_set_id": PRED_SET_ID,
                "scoring_config_id": None,
                "max_k_position": None,
                "softprop": False,
                "interpro_graft": False,
                "rerankers": {},
                "ia_file": "/tmp/ia.tsv",
            }
        )
        assert p.ia_file == "/tmp/ia.tsv"
        assert not hasattr(p, "scoring_config_id")

    def test_empty_evaluation_set_id_raises(self):
        with pytest.raises(ValidationError, match="non-empty"):
            RunCafaEvaluationPayload(
                evaluation_set_id="  ",
                prediction_set_id=PRED_SET_ID,
            )

    def test_empty_prediction_set_id_raises(self):
        with pytest.raises(ValidationError, match="non-empty"):
            RunCafaEvaluationPayload(
                evaluation_set_id=EVAL_SET_ID,
                prediction_set_id="",
            )

    def test_non_string_evaluation_set_id_raises(self):
        with pytest.raises(ValidationError):
            RunCafaEvaluationPayload(
                evaluation_set_id=123,
                prediction_set_id=PRED_SET_ID,
            )

    def test_strips_whitespace(self):
        p = RunCafaEvaluationPayload(
            evaluation_set_id=f"  {EVAL_SET_ID}  ",
            prediction_set_id=f"  {PRED_SET_ID}  ",
        )
        assert p.evaluation_set_id == EVAL_SET_ID
        assert p.prediction_set_id == PRED_SET_ID

    def test_frozen_payload(self):
        p = RunCafaEvaluationPayload(
            evaluation_set_id=EVAL_SET_ID,
            prediction_set_id=PRED_SET_ID,
        )
        with pytest.raises(ValidationError):
            p.evaluation_set_id = "new_value"

    def test_provenance_fields_accepted(self):
        p = RunCafaEvaluationPayload(
            evaluation_set_id=EVAL_SET_ID,
            prediction_set_id=PRED_SET_ID,
            frame="internal",
            temporal_window="SELECT_220_227",
            leakage_role="select",
            window_role="valid",
            arms_enabled={"knn": True, "reranker": False},
        )
        assert p.frame == "internal"
        assert p.temporal_window == "SELECT_220_227"
        assert p.leakage_role == "select"
        assert p.window_role == "valid"
        assert p.arms_enabled == {"knn": True, "reranker": False}

    def test_provenance_defaults_are_none(self):
        p = RunCafaEvaluationPayload(
            evaluation_set_id=EVAL_SET_ID,
            prediction_set_id=PRED_SET_ID,
        )
        assert p.frame is None
        assert p.temporal_window is None
        assert p.leakage_role is None
        assert p.window_role is None
        assert p.arms_enabled is None

    @pytest.mark.parametrize(
        "field,value",
        [
            ("frame", "bogus"),
            ("leakage_role", "bogus"),
            ("window_role", "bogus"),
            ("window_role", "probe"),  # valid leakage_role but not a window_role
        ],
    )
    def test_provenance_vocab_enforced(self, field, value):
        with pytest.raises(ValidationError):
            RunCafaEvaluationPayload(
                evaluation_set_id=EVAL_SET_ID,
                prediction_set_id=PRED_SET_ID,
                **{field: value},
            )


# ---------------------------------------------------------------------------
# Provenance stamping helpers (FIX-UI-PROVENANCE)
# ---------------------------------------------------------------------------


class TestEvalProvenanceStamping:
    def test_stamp_window_role_sets_when_blank(self):
        es = _make_eval_set()
        es.window_role = None
        RunCafaEvaluationOperation._stamp_window_role(es, "valid", _make_emit())
        assert es.window_role == "valid"

    def test_stamp_window_role_never_overwrites(self):
        es = _make_eval_set()
        es.window_role = "test"
        RunCafaEvaluationOperation._stamp_window_role(es, "valid", _make_emit())
        assert es.window_role == "test"

    def test_stamp_window_role_noop_when_payload_none(self):
        es = _make_eval_set()
        es.window_role = None
        RunCafaEvaluationOperation._stamp_window_role(es, None, _make_emit())
        assert es.window_role is None

    def test_build_provenance_derives_leakage_from_window_role(self):
        es = _make_eval_set()
        es.window_role = "valid"
        p = RunCafaEvaluationPayload(evaluation_set_id=EVAL_SET_ID, prediction_set_id=PRED_SET_ID)
        frame, window, role, arms = RunCafaEvaluationOperation._build_eval_provenance(p, es)
        assert role == "select"
        assert frame is None and window is None
        # No longer derived: the evaluator runs no arm, and what the method ran
        # is in its prediction set's record.
        assert arms is None

    def test_build_provenance_explicit_wins(self):
        es = _make_eval_set()
        es.window_role = "valid"
        p = RunCafaEvaluationPayload(
            evaluation_set_id=EVAL_SET_ID,
            prediction_set_id=PRED_SET_ID,
            frame="internal",
            temporal_window="SELECT_220_227",
            leakage_role="probe",
            arms_enabled={"knn": True, "reranker": True},
        )
        frame, window, role, arms = RunCafaEvaluationOperation._build_eval_provenance(p, es)
        assert (frame, window, role) == ("internal", "SELECT_220_227", "probe")
        assert arms == {"knn": True, "reranker": True}

    def test_build_provenance_without_a_role_leaves_leakage_unknown(self):
        es = _make_eval_set()
        es.window_role = None
        p = RunCafaEvaluationPayload(evaluation_set_id=EVAL_SET_ID, prediction_set_id=PRED_SET_ID)
        _, _, role, _ = RunCafaEvaluationOperation._build_eval_provenance(p, es)
        assert role is None


# ---------------------------------------------------------------------------
# Operation name
# ---------------------------------------------------------------------------


class TestOperationName:
    def test_name(self):
        op = RunCafaEvaluationOperation()
        assert op.name == "run_cafa_evaluation"


# ---------------------------------------------------------------------------
# _parse_results
# ---------------------------------------------------------------------------


class TestParseResults:
    def setup_method(self):
        self.op = RunCafaEvaluationOperation()

    def test_parse_all_namespaces(self):
        dfs_best = _dfs_best_fixture()
        result = self.op._parse_results(dfs_best)
        assert set(result.keys()) == {"BPO", "MFO", "CCO"}

    def test_parse_bpo_values(self):
        dfs_best = _dfs_best_fixture()
        result = self.op._parse_results(dfs_best)
        bpo = result["BPO"]
        assert bpo["fmax"] == 0.45
        assert bpo["precision"] == 0.51
        assert bpo["recall"] == 0.40
        assert bpo["tau"] == 0.32
        assert bpo["coverage"] == 0.95
        assert bpo["n_proteins"] == 100

    def test_parse_mfo_values(self):
        dfs_best = _dfs_best_fixture()
        result = self.op._parse_results(dfs_best)
        mfo = result["MFO"]
        assert mfo["fmax"] == 0.60
        assert mfo["precision"] == 0.65
        assert mfo["recall"] == 0.55

    def test_parse_empty_dfs_best(self):
        result = self.op._parse_results({})
        assert result == {}

    def test_parse_none_df_f(self):
        result = self.op._parse_results({"f": None})
        assert result == {}

    def test_parse_empty_df_f(self):
        result = self.op._parse_results({"f": pd.DataFrame()})
        assert result == {}

    def test_parse_ignores_unknown_namespaces(self):
        df_f = pd.DataFrame(
            [
                {
                    "ns": "unknown_namespace",
                    "f": 0.5,
                    "pr": 0.5,
                    "rc": 0.5,
                    "tau": 0.1,
                    "cov_max": 0.9,
                    "n": 10,
                }
            ]
        )
        result = self.op._parse_results({"f": df_f})
        assert result == {}

    def test_parse_unweighted_only_omits_weighted_keys(self):
        dfs_best = _dfs_best_fixture()
        result = self.op._parse_results(dfs_best)
        bpo = result["BPO"]
        assert "fmax_w" not in bpo
        assert "f_micro" not in bpo
        assert "f_micro_w" not in bpo
        assert "precision_w" not in bpo
        assert "recall_w" not in bpo

    def test_parse_with_weighted_surfaces_extra_keys(self):
        dfs_best = _dfs_best_fixture(with_weighted=True)
        result = self.op._parse_results(dfs_best)
        bpo = result["BPO"]
        assert bpo["fmax"] == 0.45
        assert bpo["fmax_w"] == 0.40
        assert bpo["f_micro"] == 0.30
        assert bpo["f_micro_w"] == 0.25
        cco = result["CCO"]
        assert cco["fmax_w"] == 0.62
        assert cco["f_micro_w"] == 0.50

    def test_parse_with_weighted_surfaces_weighted_precision_recall(self):
        # The IA-weighted micro precision / recall / coverage that go with
        # f_micro_w must be persisted per aspect (FIX-METRIC-IA): these are
        # the LAFA-comparable numbers, distinct from the unweighted pr/rc.
        dfs_best = _dfs_best_fixture(with_weighted=True)
        result = self.op._parse_results(dfs_best)
        bpo = result["BPO"]
        assert bpo["precision_w"] == 0.33
        assert bpo["recall_w"] == 0.20
        assert bpo["coverage_w"] == 0.94
        # unweighted pr/rc are unchanged and kept alongside
        assert bpo["precision"] == 0.51
        assert bpo["recall"] == 0.40
        mfo = result["MFO"]
        assert mfo["precision_w"] == 0.50
        assert mfo["recall_w"] == 0.41

    def test_parse_weighted_handles_missing_namespace_in_extra_frame(self):
        dfs_best = _dfs_best_fixture(with_weighted=True)
        # Drop one ns from the f_w frame — it should NOT remove the
        # unweighted keys for that namespace, only skip the _w one.
        dfs_best["f_w"] = dfs_best["f_w"][dfs_best["f_w"]["ns"] != "molecular_function"]
        result = self.op._parse_results(dfs_best)
        mfo = result["MFO"]
        assert mfo["fmax"] == 0.60
        assert "fmax_w" not in mfo
        # Other namespaces still get both
        assert result["BPO"]["fmax_w"] == 0.40

    def test_parse_uses_cov_fallback_when_no_cov_max(self):
        df_f = pd.DataFrame(
            [
                {
                    "ns": "biological_process",
                    "f": 0.5,
                    "pr": 0.5,
                    "rc": 0.5,
                    "tau": 0.1,
                    "cov": 0.85,
                    "n": 10,
                }
            ]
        )
        result = self.op._parse_results({"f": df_f})
        assert result["BPO"]["coverage"] == 0.85

    def test_parse_missing_n_column(self):
        df_f = pd.DataFrame(
            [
                {
                    "ns": "biological_process",
                    "f": 0.5,
                    "pr": 0.5,
                    "rc": 0.5,
                    "tau": 0.1,
                    "cov_max": 0.9,
                }
            ]
        )
        result = self.op._parse_results({"f": df_f})
        assert result["BPO"]["n_proteins"] is None


# ---------------------------------------------------------------------------
# _write_gt
# ---------------------------------------------------------------------------


class TestWriteGt:
    def setup_method(self):
        self.op = RunCafaEvaluationOperation()

    def test_write_gt_basic(self):
        annotations = {
            "P2": {"GO:0000002", "GO:0000003"},
            "P1": {"GO:0000001"},
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as f:
            path = f.name
        try:
            self.op._write_gt(annotations, path)
            with open(path) as f:
                lines = f.read().strip().split("\n")
            # Sorted by protein then by GO ID
            assert lines[0] == "P1\tGO:0000001"
            assert lines[1] == "P2\tGO:0000002"
            assert lines[2] == "P2\tGO:0000003"
            assert len(lines) == 3
        finally:
            os.unlink(path)

    def test_write_gt_empty(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as f:
            path = f.name
        try:
            self.op._write_gt({}, path)
            with open(path) as f:
                content = f.read()
            assert content == ""
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# _download_obo
# ---------------------------------------------------------------------------


class TestDownloadObo:
    def setup_method(self):
        self.op = RunCafaEvaluationOperation()

    @patch("protea.core.operations._run_cafa_artifacts.requests.get")
    def test_download_plain(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = "format-version: 1.2\n"
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with tempfile.NamedTemporaryFile(suffix=".obo", delete=False) as f:
            path = f.name
        try:
            self.op._download_obo("https://example.com/go.obo", path)
            with open(path) as f:
                assert f.read() == "format-version: 1.2\n"
        finally:
            os.unlink(path)

    @patch("protea.core.operations._run_cafa_artifacts.requests.get")
    def test_download_gzip(self, mock_get):
        original = b"format-version: 1.2\n"
        compressed = gzip.compress(original)
        mock_resp = MagicMock()
        mock_resp.content = compressed
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with tempfile.NamedTemporaryFile(suffix=".obo", delete=False) as f:
            path = f.name
        try:
            self.op._download_obo("https://example.com/go.obo.gz", path)
            with open(path, "rb") as f:
                assert f.read() == original
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# _download_tsv
# ---------------------------------------------------------------------------


class TestDownloadTsv:
    def setup_method(self):
        self.op = RunCafaEvaluationOperation()

    def test_local_absolute_path(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as src:
            src.write("GO:0001\t0.5\n")
            src_path = src.name
        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as dst:
            dst_path = dst.name
        try:
            self.op._download_tsv(src_path, dst_path)
            with open(dst_path) as f:
                assert f.read() == "GO:0001\t0.5\n"
        finally:
            os.unlink(src_path)
            os.unlink(dst_path)

    def test_local_file_scheme(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as src:
            src.write("GO:0002\t0.8\n")
            src_path = src.name
        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as dst:
            dst_path = dst.name
        try:
            self.op._download_tsv(f"file://{src_path}", dst_path)
            with open(dst_path) as f:
                assert f.read() == "GO:0002\t0.8\n"
        finally:
            os.unlink(src_path)
            os.unlink(dst_path)

    def test_local_gzip_path(self):
        original = b"GO:0003\t0.3\n"
        with tempfile.NamedTemporaryFile(suffix=".tsv.gz", delete=False) as src:
            src.write(gzip.compress(original))
            src_path = src.name
        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as dst:
            dst_path = dst.name
        try:
            self.op._download_tsv(src_path, dst_path)
            with open(dst_path, "rb") as f:
                assert f.read() == original
        finally:
            os.unlink(src_path)
            os.unlink(dst_path)

    @patch("protea.core.operations._run_cafa_artifacts.requests.get")
    def test_http_download(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = "GO:0004\t0.9\n"
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as dst:
            dst_path = dst.name
        try:
            self.op._download_tsv("https://example.com/ia.tsv", dst_path)
            with open(dst_path) as f:
                assert f.read() == "GO:0004\t0.9\n"
        finally:
            os.unlink(dst_path)

    @patch("protea.core.operations._run_cafa_artifacts.requests.get")
    def test_http_gzip_download(self, mock_get):
        original = b"GO:0005\t0.6\n"
        mock_resp = MagicMock()
        mock_resp.content = gzip.compress(original)
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as dst:
            dst_path = dst.name
        try:
            self.op._download_tsv("https://example.com/ia.tsv.gz", dst_path)
            with open(dst_path, "rb") as f:
                assert f.read() == original
        finally:
            os.unlink(dst_path)


# ---------------------------------------------------------------------------
# execute — error paths
# ---------------------------------------------------------------------------


class TestExecuteErrors:
    def setup_method(self):
        self.op = RunCafaEvaluationOperation()
        self.emit = _make_emit()

    def test_missing_evaluation_set(self):
        session = MagicMock()
        session.get.return_value = None

        with pytest.raises(ValueError, match="EvaluationSet.*not found"):
            self.op.execute(
                session,
                {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID},
                emit=self.emit,
            )

    def test_missing_prediction_set(self):
        session = MagicMock()
        eval_set = _make_eval_set()
        # First call returns eval_set, second returns None (pred_set missing)
        session.get.side_effect = _gets(eval_set=eval_set, pred_set=None)

        with pytest.raises(ValueError, match="PredictionSet.*not found"):
            self.op.execute(
                session,
                {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID},
                emit=self.emit,
            )

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_no_delta_proteins(self, mock_compute):
        mock_compute.return_value = (
            EvaluationData(nk={}, lk={}, pk={}, known={}, pk_known={}),
            uuid.uuid4(),
        )
        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        snapshot = _make_snapshot()
        session.get.side_effect = _gets(eval_set=eval_set, pred_set=pred_set, snapshot=snapshot)

        with pytest.raises(ValueError, match="No delta proteins"):
            self.op.execute(
                session,
                {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID},
                emit=self.emit,
            )

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_a_set_of_candidates_is_refused_before_anything_is_computed(
        self, mock_compute, monkeypatch
    ):
        from protea.core.scoring import MethodScoreMissing

        def refuse(_session, set_id):
            raise MethodScoreMissing(f"prediction set {set_id} stores candidates without scores")

        monkeypatch.setattr(
            "protea.core.operations.run_cafa_evaluation.refuse_unless_scored", refuse
        )
        session = MagicMock()
        session.get.side_effect = _gets(
            eval_set=_make_eval_set(), pred_set=_make_pred_set(), snapshot=_make_snapshot()
        )
        with pytest.raises(MethodScoreMissing, match="candidates without scores"):
            self.op.execute(
                session,
                {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID},
                emit=self.emit,
            )
        mock_compute.assert_not_called()


# ---------------------------------------------------------------------------
# execute — happy path
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _mock_artifact_store(request):
    """Stub the ArtifactStore so happy-path tests don't try to reach MinIO.

    Only applied to TestExecuteHappyPath and TestExecuteErrors — the other
    classes in this file test pure helpers that don't touch the store.
    """
    if not request.cls or not request.cls.__name__.startswith("TestExecute"):
        yield
        return
    with (
        patch(
            "protea.core.operations.run_cafa_evaluation.get_artifact_store",
            return_value=MagicMock(),
        ),
        patch(
            "protea.core.operations.run_cafa_evaluation.load_settings",
            return_value=MagicMock(),
        ),
    ):
        yield


class TestExecuteHappyPath:
    def setup_method(self):
        self.op = RunCafaEvaluationOperation()
        self.emit = _make_emit()

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_full_run(self, mock_compute):
        mock_compute.return_value = (_make_eval_data(), uuid.uuid4())

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        snapshot = _make_snapshot()
        session.get.side_effect = _gets(eval_set=eval_set, pred_set=pred_set, snapshot=snapshot)

        # Mock the DB query for _write_predictions
        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        dfs_best = _dfs_best_fixture()

        with patch("protea.core.operations._run_cafa_artifacts.download_obo"):
            with patch(
                "cafaeval.evaluation.cafa_eval",
                return_value=(MagicMock(), dfs_best),
            ) as mock_cafa:
                result = self.op.execute(
                    session,
                    {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID},
                    emit=self.emit,
                )

        assert "evaluation_result_id" in result.result
        assert "results" in result.result
        # cafa_eval called 3 times: NK, LK, PK
        assert mock_cafa.call_count == 3
        # session.add called for EvaluationResult
        session.add.assert_called_once()
        session.flush.assert_called_once()

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_job_id_threaded_onto_eval_result(self, mock_compute):
        """R0.1: the worker-injected ``_job_id`` is stamped onto the result.

        A result born without its Job id is an orphan artifact (the
        job_id=None archaeology trap the reproducible frame eliminates).
        """
        mock_compute.return_value = (_make_eval_data(), uuid.uuid4())

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        snapshot = _make_snapshot()
        session.get.side_effect = _gets(eval_set=eval_set, pred_set=pred_set, snapshot=snapshot)

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        added: list = []
        session.add.side_effect = added.append

        dfs_best = _dfs_best_fixture()
        job_id = uuid.uuid4()

        with patch("protea.core.operations._run_cafa_artifacts.download_obo"):
            with patch(
                "cafaeval.evaluation.cafa_eval",
                return_value=(MagicMock(), dfs_best),
            ):
                self.op.execute(
                    session,
                    {
                        "evaluation_set_id": EVAL_SET_ID,
                        "prediction_set_id": PRED_SET_ID,
                        "_job_id": str(job_id),
                    },
                    emit=self.emit,
                )

        assert added, "no EvaluationResult was added"
        assert added[0].job_id == job_id

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_emit_events(self, mock_compute):
        mock_compute.return_value = (_make_eval_data(), uuid.uuid4())

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        snapshot = _make_snapshot()
        session.get.side_effect = _gets(eval_set=eval_set, pred_set=pred_set, snapshot=snapshot)

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        dfs_best = _dfs_best_fixture()

        with patch("protea.core.operations._run_cafa_artifacts.download_obo"):
            with patch(
                "cafaeval.evaluation.cafa_eval",
                return_value=(MagicMock(), dfs_best),
            ):
                self.op.execute(
                    session,
                    {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID},
                    emit=self.emit,
                )

        # Verify key emit events were fired
        emit_events = [c[0][0] for c in self.emit.call_args_list]
        assert "run_cafa_evaluation.start" in emit_events
        assert "run_cafa_evaluation.computing_delta" in emit_events
        assert "run_cafa_evaluation.delta_done" in emit_events
        assert "run_cafa_evaluation.downloading_obo" in emit_events
        assert "run_cafa_evaluation.writing_predictions" in emit_events
        assert "run_cafa_evaluation.done" in emit_events
        # 3 evaluating events (NK, LK, PK)
        assert emit_events.count("run_cafa_evaluation.evaluating") == 3
        assert emit_events.count("run_cafa_evaluation.setting_done") == 3

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_cafa_eval_failure_fails_the_job(self, mock_compute):
        """When cafa_eval raises, every setting is tried and then the job fails.

        This test used to assert the opposite -- three empty cells and a
        successful job -- which is the behaviour that let a real breakage sit
        unnoticed: `signal only works in main thread of the main interpreter`
        failed all three settings and the job still reported success with an
        EvaluationResult of three empty cells.
        """
        mock_compute.return_value = (_make_eval_data(), uuid.uuid4())

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        snapshot = _make_snapshot()
        session.get.side_effect = _gets(eval_set=eval_set, pred_set=pred_set, snapshot=snapshot)

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        from protea.core.operations._run_cafa_eval_driver import SettingEvaluationError

        with (
            patch("protea.core.operations._run_cafa_artifacts.download_obo"),
            patch(
                "cafaeval.evaluation.cafa_eval",
                side_effect=RuntimeError("cafa_eval exploded"),
            ),
            pytest.raises(SettingEvaluationError) as exc,
        ):
            self.op.execute(
                session,
                {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID},
                emit=self.emit,
            )

        # All three are named, so the audit says whether it is systemic.
        for setting in ("NK", "LK", "PK"):
            assert setting in str(exc.value)

        # And all three were tried before it raised, not just the first.
        emit_events = [c[0][0] for c in self.emit.call_args_list]
        assert emit_events.count("run_cafa_evaluation.setting_failed") == 3

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_ia_missing_warning(self, mock_compute):
        """When no IA file and no ia_url, a warning should be emitted."""
        mock_compute.return_value = (_make_eval_data(), uuid.uuid4())

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        snapshot = _make_snapshot(ia_url=None)  # no ia_url
        session.get.side_effect = _gets(eval_set=eval_set, pred_set=pred_set, snapshot=snapshot)

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        with patch("protea.core.operations._run_cafa_artifacts.download_obo"):
            with patch(
                "cafaeval.evaluation.cafa_eval",
                return_value=(MagicMock(), _dfs_best_fixture()),
            ):
                self.op.execute(
                    session,
                    {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID},
                    emit=self.emit,
                )

        emit_events = [c[0][0] for c in self.emit.call_args_list]
        assert "run_cafa_evaluation.ia_missing" in emit_events

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_ia_url_download(self, mock_compute):
        """When snapshot has ia_url, _download_tsv should be called."""
        mock_compute.return_value = (_make_eval_data(), uuid.uuid4())

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        snapshot = _make_snapshot(ia_url="https://example.com/ia.tsv")
        session.get.side_effect = _gets(eval_set=eval_set, pred_set=pred_set, snapshot=snapshot)

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        with (
            patch("protea.core.operations._run_cafa_artifacts.download_obo"),
            patch("protea.core.operations._run_cafa_artifacts.download_tsv") as mock_dl_tsv,
            patch(
                "cafaeval.evaluation.cafa_eval",
                return_value=(MagicMock(), _dfs_best_fixture()),
            ),
        ):
            self.op.execute(
                session,
                {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID},
                emit=self.emit,
            )

        mock_dl_tsv.assert_called_once()
        assert mock_dl_tsv.call_args[0][0] == "https://example.com/ia.tsv"

        emit_events = [c[0][0] for c in self.emit.call_args_list]
        assert "run_cafa_evaluation.downloading_ia" in emit_events
        assert "run_cafa_evaluation.ia_resolved" in emit_events

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_explicit_ia_file_takes_precedence(self, mock_compute):
        """Explicit ia_file in payload overrides snapshot ia_url."""
        mock_compute.return_value = (_make_eval_data(), uuid.uuid4())

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        snapshot = _make_snapshot(ia_url="https://example.com/ia.tsv")
        session.get.side_effect = _gets(eval_set=eval_set, pred_set=pred_set, snapshot=snapshot)

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        with (
            patch("protea.core.operations._run_cafa_artifacts.download_obo"),
            patch("protea.core.operations._run_cafa_artifacts.download_tsv") as mock_dl_tsv,
            patch(
                "cafaeval.evaluation.cafa_eval",
                return_value=(MagicMock(), _dfs_best_fixture()),
            ),
        ):
            self.op.execute(
                session,
                {
                    "evaluation_set_id": EVAL_SET_ID,
                    "prediction_set_id": PRED_SET_ID,
                    "ia_file": "/custom/ia.tsv",
                },
                emit=self.emit,
            )

        # _download_tsv should NOT be called because ia_file overrides ia_url
        mock_dl_tsv.assert_not_called()

        emit_events = [c[0][0] for c in self.emit.call_args_list]
        assert "run_cafa_evaluation.ia_resolved" in emit_events
        assert "run_cafa_evaluation.downloading_ia" not in emit_events

    def _run_with_band(self, mock_compute, *, snapshot, payload_extra):
        """Drive ``execute`` with a banded payload; return the emit-event names.

        Raises whatever the operation raises (the phantom-gap guard fires
        before cafa_eval, so the mocked binary is never reached on rejection).
        """
        mock_compute.return_value = (_make_eval_data(), uuid.uuid4())
        session = MagicMock()
        session.get.side_effect = _gets(
            eval_set=_make_eval_set(), pred_set=_make_pred_set(), snapshot=snapshot
        )
        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []
        payload = {
            "evaluation_set_id": EVAL_SET_ID,
            "prediction_set_id": PRED_SET_ID,
            **payload_extra,
        }
        with (
            patch("protea.core.operations._run_cafa_artifacts.download_obo"),
            patch("protea.core.operations._run_cafa_artifacts.download_tsv"),
            patch(
                "cafaeval.evaluation.cafa_eval",
                return_value=(MagicMock(), _dfs_best_fixture()),
            ),
        ):
            self.op.execute(session, payload, emit=self.emit)
        return [c[0][0] for c in self.emit.call_args_list]

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_band_canonical_pair_verified(self, mock_compute):
        """A v227-declared cell with the canonical snapshot + IA emits
        band_verified and proceeds."""
        snapshot = _make_snapshot(obo_version="releases/2025-07-22")
        events = self._run_with_band(
            mock_compute,
            snapshot=snapshot,
            payload_extra={"band": "v227", "ia_file": "/data/lafa_t0_Sep_2025/IA.tsv"},
        )
        assert "run_cafa_evaluation.band_verified" in events

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_band_rejects_cross_band_ia(self, mock_compute):
        """A v227 cell that resolves the v226 IA is rejected at runtime."""
        snapshot = _make_snapshot(obo_version="releases/2025-07-22")
        with pytest.raises(BandMismatchError, match="IA artifact"):
            self._run_with_band(
                mock_compute,
                snapshot=snapshot,
                payload_extra={"band": "v227", "ia_file": "/data/IA_cafa6.tsv"},
            )

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_band_rejects_cross_band_snapshot(self, mock_compute):
        """A v227 cell whose pivot snapshot is the v226 ontology is rejected."""
        snapshot = _make_snapshot(obo_version="releases/2025-03-16", ia_url=None)
        with pytest.raises(BandMismatchError, match="obo_version"):
            self._run_with_band(
                mock_compute,
                snapshot=snapshot,
                payload_extra={"band": "v227", "ia_file": "/data/IA.tsv"},
            )

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_band_rejects_ic1_fallback(self, mock_compute):
        """A band-declared cell with no IA (would be IC=1) is rejected."""
        snapshot = _make_snapshot(obo_version="releases/2025-03-16", ia_url=None)
        with pytest.raises(BandMismatchError, match="IC=1"):
            self._run_with_band(
                mock_compute,
                snapshot=snapshot,
                payload_extra={"band": "v226"},
            )

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_no_band_is_unguarded(self, mock_compute):
        """Without a declared band the guard is a no-op (legacy/ad-hoc runs)."""
        snapshot = _make_snapshot(obo_version="releases/2025-03-16", ia_url=None)
        events = self._run_with_band(
            mock_compute,
            snapshot=snapshot,
            payload_extra={},
        )
        assert "run_cafa_evaluation.band_verified" not in events

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_session_commit_before_cafa_eval(self, mock_compute):
        """Session should be committed before cafa_eval to release DB connection."""
        mock_compute.return_value = (_make_eval_data(), uuid.uuid4())

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        snapshot = _make_snapshot()
        session.get.side_effect = _gets(eval_set=eval_set, pred_set=pred_set, snapshot=snapshot)

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        call_order = []
        session.commit.side_effect = lambda: call_order.append("commit")

        with patch("protea.core.operations._run_cafa_artifacts.download_obo"):
            with patch(
                "cafaeval.evaluation.cafa_eval",
                side_effect=lambda *a, **kw: (
                    call_order.append("cafa_eval"),
                    (MagicMock(), _dfs_best_fixture()),
                )[-1],
            ):
                self.op.execute(
                    session,
                    {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID},
                    emit=self.emit,
                )

        assert call_order[0] == "commit"
        assert "cafa_eval" in call_order

    @patch("protea.core.operations.run_cafa_evaluation.get_artifact_store")
    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_artifacts_uploaded_to_store(self, mock_compute, mock_get_store):
        """Cafaeval output staged in a tempdir is uploaded via artifact_store.put."""
        mock_compute.return_value = (_make_eval_data(), uuid.uuid4())
        store = MagicMock()
        mock_get_store.return_value = store

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        snapshot = _make_snapshot()
        session.get.side_effect = _gets(eval_set=eval_set, pred_set=pred_set, snapshot=snapshot)

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        df_mock = MagicMock()  # non-None df triggers write_results inside the staging dir
        dfs_best = _dfs_best_fixture()

        with (
            patch("protea.core.operations._run_cafa_artifacts.download_obo"),
            patch(
                "cafaeval.evaluation.cafa_eval",
                return_value=(df_mock, dfs_best),
            ),
            patch("cafaeval.evaluation.write_results") as mock_write,
        ):
            result = self.op.execute(
                session,
                {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID},
                emit=self.emit,
            )

        # write_results called once per setting (NK, LK, PK)
        assert mock_write.call_count == 3
        # Result advertises the uploaded keys (via artifact_store.put)
        assert "results" in result.result
        # Test runs without MinIO; the artifact_store mock just records the calls.
        # We don't assert exact count because the staging tempdir is empty under
        # the cafaeval.write_results patch — what matters is the operation
        # finishes cleanly and the store is consulted.
        assert mock_get_store.called


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_ns_labels_mapping(self):
        assert _NS_LABELS["biological_process"] == "BPO"
        assert _NS_LABELS["molecular_function"] == "MFO"
        assert _NS_LABELS["cellular_component"] == "CCO"

    def test_ns_short_set(self):
        assert _NS_SHORT == {"BPO", "MFO", "CCO"}


# ---------------------------------------------------------------------------
# LOC regression guard (T2B.5 closure)
# ---------------------------------------------------------------------------


class TestSmellBudgetGuard:
    """Ratchet tests: assert no method in RunCafaEvaluationOperation exceeds
    the master-plan v3.2 §3 ceiling of 60 LOC. If a future edit pushes a
    method over the limit, this test fails before check_smells.py runs in CI,
    giving the author an early, targeted signal."""

    def test_all_methods_under_60_loc(self):
        import ast
        from pathlib import Path

        src_path = (
            Path(__file__).resolve().parents[1]
            / "protea"
            / "core"
            / "operations"
            / "run_cafa_evaluation.py"
        )
        tree = ast.parse(src_path.read_text())
        offenders: list[tuple[str, int]] = []
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                loc = (node.end_lineno or node.lineno) - node.lineno + 1
                if loc > 60:
                    offenders.append((node.name, loc))
        assert not offenders, (
            f"Methods exceed 60-LOC ceiling (T2B.5): {offenders}. "
            "Extract the body or apply the Method Object pattern."
        )


# ---------------------------------------------------------------------------
# LAFA parity: the cafaeval invocation must use LAFA-compatible flags
# ---------------------------------------------------------------------------


class TestCafaevalInvocationLafaParity:
    """The signal-safe cafa_eval call must forward the parity knobs.

    LAFA scores with cafaeval's default th_step (0.01) and no max_terms
    cap. PROTEA must pass exactly the values carried on the run context
    (defaulting to those), not hard-coded legacy values, otherwise the
    same prediction scores differently on each side. See
    docs/EVAL_LAFA_PARITY.md.
    """

    def _make_ctx(self, **overrides):
        from protea.core.operations._run_cafa_eval_driver import CafaEvalRunContext

        base = dict(
            artifacts_root=__import__("pathlib").Path("/tmp"),
            obo_path="/tmp/go.obo",
            nk_path="/tmp/nk.tsv",
            lk_path="/tmp/lk.tsv",
            pk_path="/tmp/pk.tsv",
            pk_known_path="/tmp/pk_known.tsv",
            ia_path="/tmp/ia.tsv",
            toi_path="/tmp/toi.txt",
            shared_pred_dir="/tmp/preds",
        )
        base.update(overrides)
        return CafaEvalRunContext(**base)

    def test_context_defaults_are_lafa_compatible(self):
        ctx = self._make_ctx()
        assert ctx.th_step == 0.01
        assert ctx.max_terms is None

    def test_invoke_forwards_th_step_and_max_terms(self):
        from protea.core.operations import _run_cafa_eval_driver as driver

        ctx = self._make_ctx(th_step=0.01, max_terms=None)
        captured: dict[str, Any] = {}

        def fake_cafa_eval(*args, **kwargs):
            captured.update(kwargs)
            return ("df", "dfs_best")

        with patch.dict(
            "sys.modules",
            {"cafaeval.evaluation": MagicMock(cafa_eval=fake_cafa_eval)},
        ):
            driver._invoke_cafaeval_signal_safe(
                ctx=ctx, pred_dir="/tmp/preds", gt_file="/tmp/nk.tsv", known_file=None
            )

        assert captured["th_step"] == 0.01
        assert captured["max_terms"] is None
        assert captured["toi_file"] == "/tmp/toi.txt"
        assert captured["prop"] == "fill"
        assert captured["norm"] == "cafa"
        assert captured["no_orphans"] is True

    def test_invoke_uses_custom_knobs_when_overridden(self):
        from protea.core.operations import _run_cafa_eval_driver as driver

        ctx = self._make_ctx(th_step=0.001, max_terms=500)
        captured: dict[str, Any] = {}

        def fake_cafa_eval(*args, **kwargs):
            captured.update(kwargs)
            return ("df", "dfs_best")

        with patch.dict(
            "sys.modules",
            {"cafaeval.evaluation": MagicMock(cafa_eval=fake_cafa_eval)},
        ):
            driver._invoke_cafaeval_signal_safe(
                ctx=ctx, pred_dir="/tmp/preds", gt_file="/tmp/nk.tsv", known_file=None
            )

        assert captured["th_step"] == 0.001
        assert captured["max_terms"] == 500


# ---------------------------------------------------------------------------
# Running cafaeval off the main thread, and refusing an empty result
# ---------------------------------------------------------------------------


class TestCafaevalOffTheMainThread:
    """``_job_pump`` runs every operation in a per-job thread.

    ``signal.signal`` is refused anywhere but the main thread, so the call
    that existed to make cafaeval safe was the one thing that made it
    impossible from a worker. The tests above only ever exercised it from the
    main thread, which is why they passed while every real run failed.
    """

    @staticmethod
    def _ctx():
        """Same context the parity tests build, so only the thread differs."""
        return TestCafaevalInvocationLafaParity()._make_ctx()

    def _invoke_in_a_thread(self, cafa_eval):
        """Call the driver from a non-main thread, re-raising what it raised."""
        import threading

        from protea.core.operations import _run_cafa_eval_driver as driver

        box: dict[str, Any] = {}

        def run():
            try:
                with patch.dict(
                    "sys.modules",
                    {"cafaeval.evaluation": MagicMock(cafa_eval=cafa_eval)},
                ):
                    box["out"] = driver._invoke_cafaeval_signal_safe(
                        ctx=self._ctx(),
                        pred_dir="/tmp/preds",
                        gt_file="/tmp/nk.tsv",
                        known_file=None,
                    )
            except BaseException as exc:  # noqa: BLE001 - re-raised below
                box["exc"] = exc

        t = threading.Thread(target=run, name="job-test")
        t.start()
        t.join(timeout=30)
        assert not t.is_alive(), "the driver call hung off the main thread"
        if "exc" in box:
            raise box["exc"]
        return box["out"]

    def test_the_driver_runs_off_the_main_thread(self):
        """The regression: this raised ValueError from signal.signal before."""
        called: dict[str, Any] = {}

        def fake_cafa_eval(*args, **kwargs):
            called["yes"] = True
            return ("df", "dfs_best")

        df, dfs_best, sink = self._invoke_in_a_thread(fake_cafa_eval)

        assert called.get("yes") is True
        assert (df, dfs_best) == ("df", "dfs_best")
        assert sink is not None

    def test_it_does_not_touch_signal_handlers_off_the_main_thread(self):
        """Not merely tolerated: the call is skipped, so it cannot raise."""
        import signal as signal_module

        with patch.object(signal_module, "signal", side_effect=AssertionError) as spy:
            self._invoke_in_a_thread(lambda *a, **k: ("df", "dfs_best"))

        assert spy.call_count == 0

    def test_it_still_resets_the_handlers_on_the_main_thread(self):
        """A direct in-process call can reach the dense kernel, so keep it."""
        import signal as signal_module

        from protea.core.operations import _run_cafa_eval_driver as driver

        with (
            patch.object(signal_module, "signal", return_value=None) as spy,
            patch.dict(
                "sys.modules",
                {"cafaeval.evaluation": MagicMock(cafa_eval=lambda *a, **k: ("df", "b"))},
            ),
        ):
            driver._invoke_cafaeval_signal_safe(
                ctx=self._ctx(), pred_dir="/tmp/p", gt_file="/tmp/nk.tsv", known_file=None
            )

        # Two to SIG_DFL on the way in, two restores on the way out.
        assert spy.call_count == 4
        assert {c.args[0] for c in spy.call_args_list} == {
            signal_module.SIGTERM,
            signal_module.SIGINT,
        }

    def test_the_sink_is_always_passed_which_forces_the_sparse_kernel(self):
        """Why skipping the reset is safe: no sink, no sparse-only, no pool.

        cafaeval refuses ``per_protein_sink`` on its dense fork-pool path, so
        as long as this caller always passes one there is no pool to protect
        from an unexited SIGTERM handler.
        """
        captured: dict[str, Any] = {}

        def fake_cafa_eval(*args, **kwargs):
            captured.update(kwargs)
            return ("df", "dfs_best")

        self._invoke_in_a_thread(fake_cafa_eval)

        assert captured["per_protein_sink"] is not None
        assert captured["n_cpu"] == 1


class TestAnEmptySettingFailsTheJob:
    """A result whose cells are empty is not a result.

    The driver used to record a failed setting as a warning and contribute
    ``{}``, so a run where all three failed still reported success and
    persisted an EvaluationResult of three empty cells -- indistinguishable
    from a configuration that genuinely scored nothing.
    """

    def _evaluate(self, per_setting):
        """Drive the loop with ``_run_cafaeval_for_setting`` stubbed per setting."""
        from protea.core.operations import _run_cafa_eval_driver as driver

        events: list[str] = []

        def emit(code, message=None, data=None, level="info"):
            events.append(code)

        ctx = MagicMock()
        with patch.object(
            driver,
            "_run_cafaeval_for_setting",
            side_effect=lambda **kw: per_setting[kw["setting"]],
        ):
            out = driver.evaluate_all_settings(ctx=ctx, emit=emit)
        return out, events

    def test_all_three_failing_raises_and_names_all_three(self):
        from protea.core.operations._run_cafa_eval_driver import SettingEvaluationError

        with pytest.raises(SettingEvaluationError) as exc:
            self._evaluate({"NK": {}, "LK": {}, "PK": {}})

        for setting in ("NK", "LK", "PK"):
            assert setting in str(exc.value)

    def test_one_failing_raises_and_names_only_that_one(self):
        from protea.core.operations._run_cafa_eval_driver import SettingEvaluationError

        with pytest.raises(SettingEvaluationError) as exc:
            self._evaluate({"NK": {"bpo": {}}, "LK": {}, "PK": {"cco": {}}})

        assert "LK" in str(exc.value)
        assert "NK" not in str(exc.value)
        assert "PK" not in str(exc.value)

    def test_all_three_run_before_it_raises(self):
        """The audit trail must name every failure, not only the first."""
        from protea.core.operations._run_cafa_eval_driver import SettingEvaluationError

        calls: list[str] = []

        from protea.core.operations import _run_cafa_eval_driver as driver

        def record(**kw):
            calls.append(kw["setting"])
            return {}

        with (
            patch.object(driver, "_run_cafaeval_for_setting", side_effect=record),
            pytest.raises(SettingEvaluationError),
        ):
            driver.evaluate_all_settings(ctx=MagicMock(), emit=lambda *a, **k: None)

        assert calls == ["NK", "LK", "PK"]

    def test_metrics_in_every_cell_return_normally(self):
        out, _ = self._evaluate(
            {"NK": {"bpo": {"f": 1}}, "LK": {"mfo": {"f": 2}}, "PK": {"cco": {"f": 3}}}
        )

        assert set(out) == {"NK", "LK", "PK"}
        assert out["NK"] == {"bpo": {"f": 1}}


# ---------------------------------------------------------------------------
# A cyclic snapshot cannot be scored, and should say so by name
# ---------------------------------------------------------------------------


class TestACyclicSnapshotIsRefusedByName:
    """cafaeval dies on a cycle with a message that names nothing.

    ``Graph.top_sort`` is Kahn's algorithm and raises
    ``Exception("The sparse matrix doesn't represent an acyclic graph")`` from
    inside ``Graph.__init__``, so the run dies while PARSING the obo. Verified
    against the real file: ``obo_parser`` on ``releases/2024-06-10`` raises
    exactly that, and the snapshot of release 221 closes two triangles.

    This does not make a cyclic snapshot scoreable. It makes the refusal
    legible, and moves it before a 31 MB download and a ground-truth write.
    """

    @staticmethod
    def _session(parent_rows):
        session = MagicMock()
        session.execute.return_value = parent_rows
        return session

    @staticmethod
    def _snapshot():
        return SimpleNamespace(id=uuid.uuid4(), obo_version="releases/2024-06-10")

    def test_an_acyclic_snapshot_passes_silently(self) -> None:
        from protea.core.operations.run_cafa_evaluation import _refuse_a_cyclic_snapshot

        emitted: list[str] = []
        _refuse_a_cyclic_snapshot(
            self._session([("GO:2", "GO:1"), ("GO:3", "GO:2")]),
            self._snapshot(),
            lambda code, *a, **k: emitted.append(code),
        )
        assert emitted == []

    def test_a_cycle_raises_naming_the_snapshot_and_the_terms(self) -> None:
        from protea.core.operations.run_cafa_evaluation import _refuse_a_cyclic_snapshot

        snap = self._snapshot()
        with pytest.raises(ValueError) as exc:
            _refuse_a_cyclic_snapshot(
                self._session([("GO:1", "GO:2"), ("GO:2", "GO:3"), ("GO:3", "GO:1")]),
                snap,
                lambda *a, **k: None,
            )
        mensaje = str(exc.value)
        assert str(snap.id) in mensaje
        assert "releases/2024-06-10" in mensaje
        for t in ("GO:1", "GO:2", "GO:3"):
            assert t in mensaje

    def test_it_says_the_ia_of_the_same_snapshot_can_still_be_computed(self) -> None:
        """So nobody concludes the snapshot is unusable for everything."""
        from protea.core.operations.run_cafa_evaluation import _refuse_a_cyclic_snapshot

        with pytest.raises(ValueError, match="compute_information_accretion"):
            _refuse_a_cyclic_snapshot(
                self._session([("GO:1", "GO:2"), ("GO:2", "GO:1")]),
                self._snapshot(),
                lambda *a, **k: None,
            )

    def test_it_emits_an_error_event_with_the_terms(self) -> None:
        from protea.core.operations.run_cafa_evaluation import _refuse_a_cyclic_snapshot

        eventos: list[tuple] = []

        def emit(code, message=None, data=None, level="info"):
            eventos.append((code, data, level))

        with pytest.raises(ValueError):
            _refuse_a_cyclic_snapshot(
                self._session([("GO:1", "GO:2"), ("GO:2", "GO:1")]),
                self._snapshot(),
                emit,
            )
        assert len(eventos) == 1
        code, data, level = eventos[0]
        assert code == "run_cafa_evaluation.cyclic_snapshot"
        assert level == "error"
        assert data["components"] == 1
        assert data["terms"] == ["GO:1", "GO:2"]

    def test_the_staging_step_calls_it_before_touching_the_obo(self) -> None:
        """The wiring: refusing after the download would still be a 31 MB wait."""
        import inspect

        from protea.core.operations.run_cafa_evaluation import RunCafaEvaluationOperation

        src = inspect.getsource(RunCafaEvaluationOperation._stage_evaluator_inputs)
        assert "_refuse_a_cyclic_snapshot(" in src
        assert src.index("_refuse_a_cyclic_snapshot(") < src.index("resolve_obo(")
