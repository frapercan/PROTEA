"""Unit tests for RunCafaEvaluationOperation.

No real DB, network, or cafaeval binary required — everything is mocked.
"""
from __future__ import annotations

import gzip
import os
import tempfile
import uuid
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pydantic import ValidationError

from protea.core.evaluation import EvaluationData
from protea.core.operations.run_cafa_evaluation import (
    _NS_LABELS,
    _NS_SHORT,
    RunCafaEvaluationOperation,
    RunCafaEvaluationPayload,
)

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
    return ps


def _make_ann_old():
    ann = MagicMock()
    ann.ontology_snapshot_id = SNAP_ID
    return ann


def _make_snapshot(obo_url="https://example.com/go.obo", ia_url=None):
    snap = MagicMock()
    snap.obo_url = obo_url
    snap.ia_url = ia_url
    return snap


def _make_eval_data(nk=None, lk=None, pk=None, known=None, pk_known=None):
    return EvaluationData(
        nk=nk or {"P1": {"GO:0000001"}},
        lk=lk or {"P2": {"GO:0000002"}},
        pk=pk or {},
        known=known or {},
        pk_known=pk_known or {},
    )


def _make_scoring_config():
    sc = MagicMock()
    sc.formula = "linear"
    sc.weights = {"embedding_similarity": 1.0}
    return sc


def _dfs_best_fixture():
    """Build a dfs_best dict matching cafaeval output format."""
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
    return {"f": df_f}


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
        assert p.max_distance is None
        assert p.artifacts_dir is None
        assert p.scoring_config_id is None
        assert p.ia_file is None

    def test_valid_payload_all_fields(self):
        p = RunCafaEvaluationPayload(
            evaluation_set_id=EVAL_SET_ID,
            prediction_set_id=PRED_SET_ID,
            max_distance=1.5,
            artifacts_dir="/tmp/artifacts",
            scoring_config_id=SCORING_CONFIG_ID,
            ia_file="/tmp/ia.tsv",
        )
        assert p.max_distance == 1.5
        assert p.artifacts_dir == "/tmp/artifacts"
        assert p.scoring_config_id == SCORING_CONFIG_ID
        assert p.ia_file == "/tmp/ia.tsv"

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

    def test_max_distance_out_of_range(self):
        with pytest.raises(ValidationError):
            RunCafaEvaluationPayload(
                evaluation_set_id=EVAL_SET_ID,
                prediction_set_id=PRED_SET_ID,
                max_distance=3.0,
            )

    def test_max_distance_negative(self):
        with pytest.raises(ValidationError):
            RunCafaEvaluationPayload(
                evaluation_set_id=EVAL_SET_ID,
                prediction_set_id=PRED_SET_ID,
                max_distance=-0.1,
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
            [{"ns": "unknown_namespace", "f": 0.5, "pr": 0.5, "rc": 0.5, "tau": 0.1, "cov_max": 0.9, "n": 10}]
        )
        result = self.op._parse_results({"f": df_f})
        assert result == {}

    def test_parse_uses_cov_fallback_when_no_cov_max(self):
        df_f = pd.DataFrame(
            [{"ns": "biological_process", "f": 0.5, "pr": 0.5, "rc": 0.5, "tau": 0.1, "cov": 0.85, "n": 10}]
        )
        result = self.op._parse_results({"f": df_f})
        assert result["BPO"]["coverage"] == 0.85

    def test_parse_missing_n_column(self):
        df_f = pd.DataFrame(
            [{"ns": "biological_process", "f": 0.5, "pr": 0.5, "rc": 0.5, "tau": 0.1, "cov_max": 0.9}]
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

    @patch("protea.core.operations.run_cafa_evaluation.requests.get")
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

    @patch("protea.core.operations.run_cafa_evaluation.requests.get")
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

    @patch("protea.core.operations.run_cafa_evaluation.requests.get")
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

    @patch("protea.core.operations.run_cafa_evaluation.requests.get")
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
# _write_predictions
# ---------------------------------------------------------------------------


class TestWritePredictions:
    def setup_method(self):
        self.op = RunCafaEvaluationOperation()

    def test_write_predictions_without_scoring_config(self):
        pred_mock = MagicMock()
        pred_mock.protein_accession = "P1"
        pred_mock.distance = 0.4
        pred_mock.identity_nw = None
        pred_mock.identity_sw = None
        pred_mock.evidence_code = None
        pred_mock.taxonomic_distance = None

        gt_mock = MagicMock()
        gt_mock.go_id = "GO:0000001"

        session = MagicMock()
        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = [(pred_mock, gt_mock)]

        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as f:
            path = f.name
        try:
            self.op._write_predictions(
                session, uuid.uuid4(), {"P1"}, None, path, None
            )
            with open(path) as f:
                line = f.read().strip()
            # score = max(0, 1 - 0.4/2) = 0.8
            assert line == "P1\tGO:0000001\t0.8000"
        finally:
            os.unlink(path)

    def test_write_predictions_deduplicates(self):
        pred1 = MagicMock()
        pred1.protein_accession = "P1"
        pred1.distance = 0.2

        pred2 = MagicMock()
        pred2.protein_accession = "P1"
        pred2.distance = 0.6

        gt_mock = MagicMock()
        gt_mock.go_id = "GO:0000001"

        session = MagicMock()
        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = [(pred1, gt_mock), (pred2, gt_mock)]

        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as f:
            path = f.name
        try:
            self.op._write_predictions(
                session, uuid.uuid4(), {"P1"}, None, path, None
            )
            with open(path) as f:
                lines = f.read().strip().split("\n")
            # Only the first (closest) prediction should be written
            assert len(lines) == 1
        finally:
            os.unlink(path)

    @patch("protea.core.operations.run_cafa_evaluation.compute_score")
    def test_write_predictions_with_scoring_config(self, mock_compute_score):
        mock_compute_score.return_value = 0.75

        pred_mock = MagicMock()
        pred_mock.protein_accession = "P1"
        pred_mock.distance = 0.4
        pred_mock.identity_nw = 0.8
        pred_mock.identity_sw = 0.9
        pred_mock.evidence_code = "IDA"
        pred_mock.taxonomic_distance = 2.0

        gt_mock = MagicMock()
        gt_mock.go_id = "GO:0000001"

        session = MagicMock()
        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = [(pred_mock, gt_mock)]

        scoring_config = _make_scoring_config()

        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as f:
            path = f.name
        try:
            self.op._write_predictions(
                session, uuid.uuid4(), {"P1"}, None, path, scoring_config
            )
            with open(path) as f:
                line = f.read().strip()
            assert line == "P1\tGO:0000001\t0.7500"
            mock_compute_score.assert_called_once()
        finally:
            os.unlink(path)

    def test_write_predictions_zero_distance(self):
        pred_mock = MagicMock()
        pred_mock.protein_accession = "P1"
        pred_mock.distance = 0.0

        gt_mock = MagicMock()
        gt_mock.go_id = "GO:0000001"

        session = MagicMock()
        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = [(pred_mock, gt_mock)]

        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as f:
            path = f.name
        try:
            self.op._write_predictions(
                session, uuid.uuid4(), {"P1"}, None, path, None
            )
            with open(path) as f:
                line = f.read().strip()
            # score = max(0, 1 - 0/2) = 1.0
            assert line == "P1\tGO:0000001\t1.0000"
        finally:
            os.unlink(path)

    def test_write_predictions_with_max_distance(self):
        """When max_distance is provided, query should include the filter."""
        pred_mock = MagicMock()
        pred_mock.protein_accession = "P1"
        pred_mock.distance = 0.3

        gt_mock = MagicMock()
        gt_mock.go_id = "GO:0000001"

        session = MagicMock()
        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = [(pred_mock, gt_mock)]

        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as f:
            path = f.name
        try:
            self.op._write_predictions(
                session, uuid.uuid4(), {"P1"}, 0.5, path, None
            )
            with open(path) as f:
                line = f.read().strip()
            assert line == "P1\tGO:0000001\t0.8500"
            # filter should have been called 3 times:
            # pred_set_id, protein_accession IN, distance <=
            assert query.filter.call_count == 3
        finally:
            os.unlink(path)

    def test_write_predictions_none_distance_fallback(self):
        pred_mock = MagicMock()
        pred_mock.protein_accession = "P1"
        pred_mock.distance = None

        gt_mock = MagicMock()
        gt_mock.go_id = "GO:0000001"

        session = MagicMock()
        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = [(pred_mock, gt_mock)]

        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as f:
            path = f.name
        try:
            self.op._write_predictions(
                session, uuid.uuid4(), {"P1"}, None, path, None
            )
            with open(path) as f:
                line = f.read().strip()
            # score = max(0, 1 - 0/2) = 1.0 (None → 0.0)
            assert line == "P1\tGO:0000001\t1.0000"
        finally:
            os.unlink(path)


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
        session.get.side_effect = [eval_set, None]

        with pytest.raises(ValueError, match="PredictionSet.*not found"):
            self.op.execute(
                session,
                {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID},
                emit=self.emit,
            )

    @patch("protea.core.operations.run_cafa_evaluation.compute_evaluation_data")
    def test_no_delta_proteins(self, mock_compute):
        mock_compute.return_value = EvaluationData(
            nk={}, lk={}, pk={}, known={}, pk_known={}
        )
        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        ann_old = _make_ann_old()
        snapshot = _make_snapshot()
        session.get.side_effect = [eval_set, pred_set, ann_old, snapshot]

        with pytest.raises(ValueError, match="No delta proteins"):
            self.op.execute(
                session,
                {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID},
                emit=self.emit,
            )

    @patch("protea.core.operations.run_cafa_evaluation.compute_evaluation_data")
    def test_missing_scoring_config(self, mock_compute):
        mock_compute.return_value = _make_eval_data()
        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        ann_old = _make_ann_old()
        snapshot = _make_snapshot()
        # get calls: eval_set, pred_set, ann_old, snapshot, scoring_config (None)
        session.get.side_effect = [eval_set, pred_set, ann_old, snapshot, None]

        with pytest.raises(ValueError, match="ScoringConfig.*not found"):
            self.op.execute(
                session,
                {
                    "evaluation_set_id": EVAL_SET_ID,
                    "prediction_set_id": PRED_SET_ID,
                    "scoring_config_id": SCORING_CONFIG_ID,
                },
                emit=self.emit,
            )


# ---------------------------------------------------------------------------
# execute — happy path
# ---------------------------------------------------------------------------


class TestExecuteHappyPath:
    def setup_method(self):
        self.op = RunCafaEvaluationOperation()
        self.emit = _make_emit()

    @patch("protea.core.operations.run_cafa_evaluation.compute_evaluation_data")
    def test_full_run(self, mock_compute):
        mock_compute.return_value = _make_eval_data()

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        ann_old = _make_ann_old()
        snapshot = _make_snapshot()
        session.get.side_effect = [eval_set, pred_set, ann_old, snapshot]

        # Mock the DB query for _write_predictions
        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        dfs_best = _dfs_best_fixture()

        with patch.object(self.op, "_download_obo"):
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

    @patch("protea.core.operations.run_cafa_evaluation.compute_evaluation_data")
    def test_emit_events(self, mock_compute):
        mock_compute.return_value = _make_eval_data()

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        ann_old = _make_ann_old()
        snapshot = _make_snapshot()
        session.get.side_effect = [eval_set, pred_set, ann_old, snapshot]

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        dfs_best = _dfs_best_fixture()

        with patch.object(self.op, "_download_obo"):
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

    @patch("protea.core.operations.run_cafa_evaluation.compute_evaluation_data")
    def test_cafa_eval_failure_catches_exception(self, mock_compute):
        """When cafa_eval raises for one setting, it should log warning and continue."""
        mock_compute.return_value = _make_eval_data()

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        ann_old = _make_ann_old()
        snapshot = _make_snapshot()
        session.get.side_effect = [eval_set, pred_set, ann_old, snapshot]

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        with patch.object(self.op, "_download_obo"):
            with patch(
                "cafaeval.evaluation.cafa_eval",
                side_effect=RuntimeError("cafa_eval exploded"),
            ):
                result = self.op.execute(
                    session,
                    {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID},
                    emit=self.emit,
                )

        # All three settings should be empty dicts (all failed)
        results = result.result["results"]
        assert results["NK"] == {}
        assert results["LK"] == {}
        assert results["PK"] == {}

        # Emit should have 3 setting_failed events
        emit_events = [c[0][0] for c in self.emit.call_args_list]
        assert emit_events.count("run_cafa_evaluation.setting_failed") == 3

    @patch("protea.core.operations.run_cafa_evaluation.compute_evaluation_data")
    def test_ia_missing_warning(self, mock_compute):
        """When no IA file and no ia_url, a warning should be emitted."""
        mock_compute.return_value = _make_eval_data()

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        ann_old = _make_ann_old()
        snapshot = _make_snapshot(ia_url=None)  # no ia_url
        session.get.side_effect = [eval_set, pred_set, ann_old, snapshot]

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        with patch.object(self.op, "_download_obo"):
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

    @patch("protea.core.operations.run_cafa_evaluation.compute_evaluation_data")
    def test_ia_url_download(self, mock_compute):
        """When snapshot has ia_url, _download_tsv should be called."""
        mock_compute.return_value = _make_eval_data()

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        ann_old = _make_ann_old()
        snapshot = _make_snapshot(ia_url="https://example.com/ia.tsv")
        session.get.side_effect = [eval_set, pred_set, ann_old, snapshot]

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        with patch.object(self.op, "_download_obo"), \
             patch.object(self.op, "_download_tsv") as mock_dl_tsv, \
             patch(
                 "cafaeval.evaluation.cafa_eval",
                 return_value=(MagicMock(), _dfs_best_fixture()),
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

    @patch("protea.core.operations.run_cafa_evaluation.compute_evaluation_data")
    def test_explicit_ia_file_takes_precedence(self, mock_compute):
        """Explicit ia_file in payload overrides snapshot ia_url."""
        mock_compute.return_value = _make_eval_data()

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        ann_old = _make_ann_old()
        snapshot = _make_snapshot(ia_url="https://example.com/ia.tsv")
        session.get.side_effect = [eval_set, pred_set, ann_old, snapshot]

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        with patch.object(self.op, "_download_obo"), \
             patch.object(self.op, "_download_tsv") as mock_dl_tsv, \
             patch(
                 "cafaeval.evaluation.cafa_eval",
                 return_value=(MagicMock(), _dfs_best_fixture()),
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

    @patch("protea.core.operations.run_cafa_evaluation.compute_evaluation_data")
    def test_session_commit_before_cafa_eval(self, mock_compute):
        """Session should be committed before cafa_eval to release DB connection."""
        mock_compute.return_value = _make_eval_data()

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        ann_old = _make_ann_old()
        snapshot = _make_snapshot()
        session.get.side_effect = [eval_set, pred_set, ann_old, snapshot]

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        call_order = []
        session.commit.side_effect = lambda: call_order.append("commit")

        with patch.object(self.op, "_download_obo"):
            with patch(
                "cafaeval.evaluation.cafa_eval",
                side_effect=lambda *a, **kw: (call_order.append("cafa_eval"), (MagicMock(), _dfs_best_fixture()))[-1],
            ):
                self.op.execute(
                    session,
                    {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID},
                    emit=self.emit,
                )

        assert call_order[0] == "commit"
        assert "cafa_eval" in call_order

    @patch("protea.core.operations.run_cafa_evaluation.compute_evaluation_data")
    def test_artifacts_dir(self, mock_compute):
        """When artifacts_dir is set, artifact directory should be created."""
        mock_compute.return_value = _make_eval_data()

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        ann_old = _make_ann_old()
        snapshot = _make_snapshot()
        session.get.side_effect = [eval_set, pred_set, ann_old, snapshot]

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(self.op, "_download_obo"):
                with patch(
                    "cafaeval.evaluation.cafa_eval",
                    return_value=(None, _dfs_best_fixture()),
                ):
                    result = self.op.execute(
                        session,
                        {
                            "evaluation_set_id": EVAL_SET_ID,
                            "prediction_set_id": PRED_SET_ID,
                            "artifacts_dir": tmpdir,
                        },
                        emit=self.emit,
                    )

            result_id = result.result["evaluation_result_id"]
            assert os.path.isdir(os.path.join(tmpdir, result_id))

    @patch("protea.core.operations.run_cafa_evaluation.compute_evaluation_data")
    def test_artifacts_dir_with_write_results(self, mock_compute):
        """When artifacts_dir is set and df is not None, write_results is called."""
        mock_compute.return_value = _make_eval_data()

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        ann_old = _make_ann_old()
        snapshot = _make_snapshot()
        session.get.side_effect = [eval_set, pred_set, ann_old, snapshot]

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        df_mock = MagicMock()  # non-None df triggers write_results
        dfs_best = _dfs_best_fixture()

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(self.op, "_download_obo"), \
                 patch(
                     "cafaeval.evaluation.cafa_eval",
                     return_value=(df_mock, dfs_best),
                 ), \
                 patch(
                     "cafaeval.evaluation.write_results"
                 ) as mock_write:
                result = self.op.execute(
                    session,
                    {
                        "evaluation_set_id": EVAL_SET_ID,
                        "prediction_set_id": PRED_SET_ID,
                        "artifacts_dir": tmpdir,
                    },
                    emit=self.emit,
                )

            # write_results called 3 times (NK, LK, PK)
            assert mock_write.call_count == 3
            result_id = result.result["evaluation_result_id"]
            # Check setting subdirectories were created
            for setting in ("NK", "LK", "PK"):
                setting_dir = os.path.join(tmpdir, result_id, setting)
                assert os.path.isdir(setting_dir)

    @patch("protea.core.operations.run_cafa_evaluation.compute_evaluation_data")
    def test_scoring_config_snapshot(self, mock_compute):
        """When scoring_config_id is provided and found, it snapshots the config."""
        mock_compute.return_value = _make_eval_data()

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        ann_old = _make_ann_old()
        snapshot = _make_snapshot()
        scoring_cfg = MagicMock()
        scoring_cfg.formula = "linear"
        scoring_cfg.weights = {"embedding_similarity": 1.0}
        session.get.side_effect = [eval_set, pred_set, ann_old, snapshot, scoring_cfg]

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        with patch.object(self.op, "_download_obo"), \
             patch(
                 "cafaeval.evaluation.cafa_eval",
                 return_value=(MagicMock(), _dfs_best_fixture()),
             ), \
             patch(
                 "protea.core.operations.run_cafa_evaluation.ScoringConfig"
             ) as mock_sc_cls:
            mock_sc_cls.return_value = MagicMock()
            result = self.op.execute(
                session,
                {
                    "evaluation_set_id": EVAL_SET_ID,
                    "prediction_set_id": PRED_SET_ID,
                    "scoring_config_id": SCORING_CONFIG_ID,
                },
                emit=self.emit,
            )

        # ScoringConfig constructor was called for snapshotting
        mock_sc_cls.assert_called_once_with(
            formula="linear",
            weights={"embedding_similarity": 1.0},
        )
        assert "evaluation_result_id" in result.result


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
