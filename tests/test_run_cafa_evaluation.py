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
from protea.core.operations._run_cafa_artifacts import WritePredictionsContext
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
        assert p.max_distance is None
        assert p.scoring_config_id is None
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

    def test_valid_payload_all_fields(self):
        p = RunCafaEvaluationPayload(
            evaluation_set_id=EVAL_SET_ID,
            prediction_set_id=PRED_SET_ID,
            max_distance=1.5,
            scoring_config_id=SCORING_CONFIG_ID,
            ia_file="/tmp/ia.tsv",
        )
        assert p.max_distance == 1.5
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
        frame, window, role, arms = RunCafaEvaluationOperation._build_eval_provenance(
            p, es, has_rerankers=False
        )
        assert role == "select"
        assert arms == {
            "knn": True,
            "reranker": False,
            "mlp_tower": False,
            "interpro": False,
            "interpro_graft": False,
        }
        assert frame is None and window is None

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
        frame, window, role, arms = RunCafaEvaluationOperation._build_eval_provenance(
            p, es, has_rerankers=False
        )
        assert (frame, window, role) == ("internal", "SELECT_220_227", "probe")
        assert arms == {"knn": True, "reranker": True}

    def test_build_provenance_reranker_arm_reflects_run(self):
        es = _make_eval_set()
        es.window_role = None
        p = RunCafaEvaluationPayload(evaluation_set_id=EVAL_SET_ID, prediction_set_id=PRED_SET_ID)
        _, _, role, arms = RunCafaEvaluationOperation._build_eval_provenance(
            p, es, has_rerankers=True
        )
        # No window_role on the set and none in the payload -> leakage unknown.
        assert role is None
        assert arms["reranker"] is True

    def test_build_provenance_interpro_graft_off_by_default(self):
        es = _make_eval_set()
        es.window_role = None
        p = RunCafaEvaluationPayload(evaluation_set_id=EVAL_SET_ID, prediction_set_id=PRED_SET_ID)
        _, _, _, arms = RunCafaEvaluationOperation._build_eval_provenance(
            p, es, has_rerankers=True
        )
        # Payload did not opt into the graft -> arm off even with rerankers.
        assert arms["interpro_graft"] is False

    def test_build_provenance_interpro_graft_needs_rerankers(self):
        es = _make_eval_set()
        es.window_role = None
        p = RunCafaEvaluationPayload(
            evaluation_set_id=EVAL_SET_ID,
            prediction_set_id=PRED_SET_ID,
            interpro_graft=True,
        )
        # Opted in but no rerankers -> the graft is skipped, so it must stay off.
        _, _, _, arms_no_rr = RunCafaEvaluationOperation._build_eval_provenance(
            p, es, has_rerankers=False
        )
        assert arms_no_rr["interpro_graft"] is False
        # Opted in with rerankers -> the graft applies, so it is recorded on.
        _, _, _, arms_rr = RunCafaEvaluationOperation._build_eval_provenance(
            p, es, has_rerankers=True
        )
        assert arms_rr["interpro_graft"] is True


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
# _write_predictions
# ---------------------------------------------------------------------------


# Base-row column order matches ``_BASE_SCORE_COLS`` in the Core columnar fetch.
_BASE_COLS_ORDER = (
    "protein_accession",
    "go_id",
    "distance",
    "identity_nw",
    "identity_sw",
    "evidence_code",
    "taxonomic_distance",
    "neighbor_vote_fraction",
)


def _base_row(
    protein="P1",
    go_id="GO:0000001",
    distance=0.4,
    identity_nw=None,
    identity_sw=None,
    evidence_code=None,
    taxonomic_distance=None,
    neighbor_vote_fraction=None,
    *,
    alignment_length_nw=None,
    gaps_pct_nw=None,
    alignment_length_sw=None,
    gaps_pct_sw=None,
    length_query=None,
    ref_annotation_density=None,
    anc2vec_neighbor_cos=None,
    anc2vec_neighbor_maxcos=None,
    go_term_frequency=None,
):
    """Build a single Core-row tuple in ``_BASE_SCORE_COLS`` order."""
    return (
        protein,
        go_id,
        distance,
        identity_nw,
        identity_sw,
        evidence_code,
        taxonomic_distance,
        neighbor_vote_fraction,
        alignment_length_nw,
        gaps_pct_nw,
        alignment_length_sw,
        gaps_pct_sw,
        length_query,
        ref_annotation_density,
        anc2vec_neighbor_cos,
        anc2vec_neighbor_maxcos,
        go_term_frequency,
    )


def _core_session(rows):
    """Mock a Session whose ``execute(stmt).all()`` returns ``rows`` (base tuples)."""
    session = MagicMock()
    result = MagicMock()
    result.all.return_value = list(rows)
    session.execute.return_value = result
    return session


def _real_scoring_config(formula="linear", weights=None, evidence_weights=None):
    from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig

    return ScoringConfig(
        formula=formula,
        weights=weights if weights is not None else {"embedding_similarity": 1.0},
        evidence_weights=evidence_weights,
    )


class TestWritePredictions:
    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path, monkeypatch):
        # Redirect the parquet base-fetch cache to an isolated temp dir so
        # tests neither pollute the repo nor share state.
        monkeypatch.setattr("protea.core.operations._pred_base_cache._PRED_CACHE_DIR", tmp_path)
        self.op = RunCafaEvaluationOperation()

    def _write(self, rows, *, scoring_config=None, max_distance=None):
        session = _core_session(rows)
        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as f:
            path = f.name
        self.op._write_predictions(
            session,
            WritePredictionsContext(
                pred_set_id=uuid.uuid4(),
                delta_proteins={"P1", "P2"},
                max_distance=max_distance,
                path=path,
            ),
            scoring_config=scoring_config,
        )
        try:
            with open(path) as f:
                return f.read().strip()
        finally:
            os.unlink(path)

    def test_write_predictions_without_scoring_config(self):
        out = self._write([_base_row(distance=0.4)])
        # score = max(0, 1 - 0.4/2) = 0.8
        assert out == "P1\tGO:0000001\t0.8000"

    def test_write_predictions_deduplicates(self):
        rows = [
            _base_row(distance=0.6),
            _base_row(distance=0.2),
        ]
        out = self._write(rows)
        # Only the closest (lowest-distance) row survives → 1 - 0.2/2 = 0.9
        assert out == "P1\tGO:0000001\t0.9000"

    def test_write_predictions_with_scoring_config(self):
        rows = [_base_row(distance=0.4)]
        # embedding_similarity = 1 - 0.4/2 = 0.8, sole signal weight 1.0 → 0.8
        out = self._write(rows, scoring_config=_real_scoring_config())
        assert out == "P1\tGO:0000001\t0.8000"

    def test_write_predictions_zero_distance(self):
        out = self._write([_base_row(distance=0.0)])
        assert out == "P1\tGO:0000001\t1.0000"

    def test_write_predictions_with_max_distance(self):
        out = self._write([_base_row(distance=0.3)], max_distance=0.5)
        assert out == "P1\tGO:0000001\t0.8500"

    def test_write_predictions_none_distance_fallback(self):
        out = self._write([_base_row(distance=None)])
        # None distance → 0.0 → 1 - 0/2 = 1.0
        assert out == "P1\tGO:0000001\t1.0000"

    def test_write_predictions_empty_writes_empty_file(self):
        out = self._write([])
        assert out == ""


class TestBaseSelectSql:
    """The Core SELECT / COUNT must carry the max_distance filter when set."""

    def _ctx(self, max_distance):
        return WritePredictionsContext(
            pred_set_id=uuid.uuid4(),
            delta_proteins={"P1"},
            max_distance=max_distance,
            path="x.tsv",
        )

    def test_select_includes_distance_filter_when_set(self):
        from protea.core.operations._run_cafa_artifacts import _base_select, _count_base_rows

        assert "distance <=" in str(_base_select(self._ctx(0.5))).lower()
        # _count_base_rows builds a COUNT(*) statement we can render via the op.
        op = RunCafaEvaluationOperation()
        session = _core_session([])
        session.execute.return_value.scalar_one.return_value = 0
        assert _count_base_rows(session, self._ctx(0.5)) == 0
        assert op is not None

    def test_select_omits_distance_filter_when_unset(self):
        from protea.core.operations._run_cafa_artifacts import _base_select

        assert "distance <=" not in str(_base_select(self._ctx(None))).lower()


class TestVectorizedScoreEquivalence:
    """The vectorised baseline scorer must be byte-for-byte equivalent to the
    OLD per-row ``_score_unranked_pred`` / ``compute_score`` output (same rows,
    same dedup winner, same 4dp TSV)."""

    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path, monkeypatch):
        monkeypatch.setattr("protea.core.operations._pred_base_cache._PRED_CACHE_DIR", tmp_path)
        self.op = RunCafaEvaluationOperation()

    # Raw rows include an intentional (P1, GO:0000001) duplicate to exercise
    # dedup, plus None-valued signals and varied evidence codes.
    _RAW_ROWS = [
        _base_row("P1", "GO:0000001", 0.4, 0.8, 0.9, "IDA", 2, 0.5),
        _base_row("P1", "GO:0000001", 0.6, 0.1, 0.2, "IEA", 5, 0.1),  # dropped (farther)
        _base_row("P1", "GO:0000002", 0.2, None, 0.3, "IEA", None, 0.7),
        _base_row("P2", "GO:0000003", 1.0, 0.5, None, None, 1, None),
        _base_row("P2", "GO:0000001", 0.8, 0.2, 0.2, "ND", 0, 0.0),
    ]

    def _oracle(self, scoring_config):
        """Replicate the OLD path: dedup (min distance), then _score_unranked_pred."""
        from protea.core.operations._run_cafa_artifacts import _score_unranked_pred

        winners: dict[tuple[str, str], tuple] = {}
        for row in sorted(self._RAW_ROWS, key=lambda r: (r[0], r[1], r[2])):
            winners.setdefault((row[0], row[1]), row)
        lines = []
        for (_p, _g), row in sorted(winners.items()):
            pred = SimpleNamespace(
                distance=row[2],
                identity_nw=row[3],
                identity_sw=row[4],
                evidence_code=row[5],
                taxonomic_distance=row[6],
                neighbor_vote_fraction=row[7],
                alignment_length_nw=row[8],
                gaps_pct_nw=row[9],
                alignment_length_sw=row[10],
                gaps_pct_sw=row[11],
                length_query=row[12],
                ref_annotation_density=row[13],
                anc2vec_neighbor_cos=row[14],
                anc2vec_neighbor_maxcos=row[15],
                go_term_frequency=row[16],
            )
            score = _score_unranked_pred(pred, scoring_config)
            lines.append(f"{row[0]}\t{row[1]}\t{score:.4f}")
        return sorted(lines)

    def _new_path(self, scoring_config):
        session = _core_session(self._RAW_ROWS)
        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as f:
            path = f.name
        self.op._write_predictions(
            session,
            WritePredictionsContext(
                pred_set_id=uuid.uuid4(),
                delta_proteins={"P1", "P2"},
                max_distance=None,
                path=path,
            ),
            scoring_config=scoring_config,
        )
        try:
            with open(path) as f:
                return sorted(line for line in f.read().splitlines() if line)
        finally:
            os.unlink(path)

    def test_equivalence_none_config_fallback(self):
        assert self._new_path(None) == self._oracle(None)

    def test_equivalence_linear_multi_signal(self):
        cfg = _real_scoring_config(
            formula="linear",
            weights={
                "embedding_similarity": 0.5,
                "identity_nw": 0.3,
                "identity_sw": 0.0,
                "evidence_weight": 0.2,
                "taxonomic_proximity": 0.1,
                "neighbor_vote_fraction": 0.4,
            },
            evidence_weights={"IEA": 0.3},
        )
        assert self._new_path(cfg) == self._oracle(cfg)

    def test_equivalence_evidence_weighted_formula(self):
        cfg = _real_scoring_config(
            formula="evidence_weighted",
            weights={"embedding_similarity": 1.0, "evidence_weight": 0.0},
            evidence_weights={"IEA": 0.5, "ND": 0.05},
        )
        assert self._new_path(cfg) == self._oracle(cfg)

    # --- A-SCORE rich axes: vectorised path must still match the per-row scorer ---
    _RICH_ROWS = [
        _base_row(
            "P1",
            "GO:0000001",
            0.4,
            0.8,
            0.9,
            "IDA",
            2,
            0.5,
            alignment_length_sw=120.0,
            gaps_pct_sw=0.1,
            length_query=200,
            ref_annotation_density=7,
            anc2vec_neighbor_cos=0.6,
            anc2vec_neighbor_maxcos=0.8,
            go_term_frequency=5000,
        ),
        _base_row(
            "P2",
            "GO:0000003",
            0.2,
            None,
            0.3,
            "IEA",
            None,
            0.7,
            alignment_length_nw=180.0,
            gaps_pct_nw=0.0,
            length_query=200,
            ref_annotation_density=0,
            anc2vec_neighbor_cos=-0.4,
            anc2vec_neighbor_maxcos=None,
            go_term_frequency=10,
        ),
        _base_row(
            "P2",
            "GO:0000005",
            0.8,
            0.2,
            0.2,
            "ND",
            1,
            0.1,
            alignment_length_sw=None,
            length_query=None,
            ref_annotation_density=None,
            go_term_frequency=None,
        ),
    ]

    def _oracle_rich(self, scoring_config):
        from protea.core.operations._run_cafa_artifacts import _score_unranked_pred

        winners: dict[tuple[str, str], tuple] = {}
        for row in sorted(self._RICH_ROWS, key=lambda r: (r[0], r[1], r[2])):
            winners.setdefault((row[0], row[1]), row)
        lines = []
        for (_p, _g), row in sorted(winners.items()):
            pred = SimpleNamespace(
                distance=row[2],
                identity_nw=row[3],
                identity_sw=row[4],
                evidence_code=row[5],
                taxonomic_distance=row[6],
                neighbor_vote_fraction=row[7],
                alignment_length_nw=row[8],
                gaps_pct_nw=row[9],
                alignment_length_sw=row[10],
                gaps_pct_sw=row[11],
                length_query=row[12],
                ref_annotation_density=row[13],
                anc2vec_neighbor_cos=row[14],
                anc2vec_neighbor_maxcos=row[15],
                go_term_frequency=row[16],
            )
            lines.append(f"{row[0]}\t{row[1]}\t{_score_unranked_pred(pred, scoring_config):.4f}")
        return sorted(lines)

    def _new_path_rich(self, scoring_config):
        session = _core_session(self._RICH_ROWS)
        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as f:
            path = f.name
        self.op._write_predictions(
            session,
            WritePredictionsContext(
                pred_set_id=uuid.uuid4(),
                delta_proteins={"P1", "P2"},
                max_distance=None,
                path=path,
            ),
            scoring_config=scoring_config,
        )
        try:
            with open(path) as f:
                return sorted(line for line in f.read().splitlines() if line)
        finally:
            os.unlink(path)

    def test_equivalence_rich_signals(self):
        cfg = _real_scoring_config(
            formula="linear",
            weights={
                "embedding_similarity": 0.4,
                "coverage": 0.3,
                "ref_annotation_density": 0.1,
                "anc2vec_neighbor_cos": 0.1,
                "anc2vec_neighbor_maxcos": 0.1,
            },
        )
        assert self._new_path_rich(cfg) == self._oracle_rich(cfg)

    def test_equivalence_ia_prior_frequency(self):
        cfg = _real_scoring_config(
            formula="linear",
            weights={"embedding_similarity": 1.0, "coverage": 0.5},
        )
        cfg.params = {"ia_prior": {"enabled": True, "gamma": 1.5, "source": "frequency"}}
        assert self._new_path_rich(cfg) == self._oracle_rich(cfg)


# ---------------------------------------------------------------------------
# universal-booster routing in the eval-artifacts path (F-RERANK-UNIVERSAL)
# ---------------------------------------------------------------------------


def _make_reranked_pred_mock():
    """A GOPrediction-shaped mock carrying every column ``_record_from_pred`` reads."""
    pred = MagicMock()
    pred.protein_accession = "P1"
    pred.qualifier = "enables"
    pred.evidence_code = "EXP"
    pred.taxonomic_relation = "same"
    pred.distance = 0.2
    # _record_from_pred uses getattr(pred, col, None) for numeric cols; a
    # MagicMock would auto-create truthy attrs, so force them to plain floats.
    from protea.core.operations._run_cafa_helpers import _NUMERIC_ORM_COLS

    for col in _NUMERIC_ORM_COLS:
        setattr(pred, col, 1.0)
    pred.distance = 0.2
    return pred


def _reranked_session(pred, go_id="GO:0000001", aspect="F"):
    session = MagicMock()
    query = MagicMock()
    session.query.return_value = query
    query.join.return_value = query
    query.filter.return_value = query
    query.yield_per.return_value = [(pred, go_id, aspect)]
    return session


class TestUniversalBoosterRoutingInEval:
    """The eval-artifacts writers must route universal boosters through
    ``score_universal`` (not the generic ``reranker_predict``) and leave the
    per-cell path unchanged (F-RERANK-UNIVERSAL eval wiring)."""

    _UNIVERSAL_CTX = {
        "categorical_codes": {
            "qualifier": ["", "enables"],
            "evidence_code": ["EXP", "IEA"],
            "taxonomic_relation": ["same", "distant"],
            "plm_id": ["prot_t5"],
        },
        "plm_id": "prot_t5",
        "k_context": 10.0,
    }

    def test_write_predictions_reranked_routes_universal(self):
        import numpy as np

        from protea.core.operations import _run_cafa_artifacts as _art

        pred = _make_reranked_pred_mock()
        session = _reranked_session(pred)
        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as f:
            path = f.name
        try:
            with (
                patch("protea.core.reranker.model_from_string", return_value=MagicMock()),
                patch(
                    "protea.core._universal_reranker.score_universal",
                    return_value=np.array([0.7]),
                ) as mock_uni,
                patch("protea.core.reranker.predict") as mock_generic,
            ):
                _art.write_predictions_reranked(
                    session,
                    WritePredictionsContext(
                        pred_set_id=uuid.uuid4(),
                        delta_proteins={"P1"},
                        max_distance=None,
                        path=path,
                    ),
                    reranker_bundle={
                        "model": "ignored-by-mock",
                        "cat_codes": None,
                        "universal": self._UNIVERSAL_CTX,
                    },
                )
            mock_uni.assert_called_once()
            mock_generic.assert_not_called()
            kwargs = mock_uni.call_args.kwargs
            assert kwargs["plm_id"] == "prot_t5"
            assert kwargs["k_context"] == 10.0
            assert kwargs["categorical_codes"] == self._UNIVERSAL_CTX["categorical_codes"]
            with open(path) as fh:
                assert fh.read().strip() == "P1\tGO:0000001\t0.7000"
        finally:
            os.unlink(path)

    def test_write_predictions_reranked_per_cell_unchanged(self):
        import numpy as np

        from protea.core.operations import _run_cafa_artifacts as _art

        pred = _make_reranked_pred_mock()
        session = _reranked_session(pred)
        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as f:
            path = f.name
        try:
            with (
                patch("protea.core.reranker.model_from_string", return_value=MagicMock()),
                patch(
                    "protea.core.reranker.predict", return_value=np.array([0.42])
                ) as mock_generic,
                patch("protea.core._universal_reranker.score_universal") as mock_uni,
            ):
                _art.write_predictions_reranked(
                    session,
                    WritePredictionsContext(
                        pred_set_id=uuid.uuid4(),
                        delta_proteins={"P1"},
                        max_distance=None,
                        path=path,
                    ),
                    reranker_bundle={
                        "model": "ignored-by-mock",
                        "cat_codes": None,
                        "universal": None,
                    },
                )
            mock_generic.assert_called_once()
            mock_uni.assert_not_called()
            with open(path) as fh:
                assert fh.read().strip() == "P1\tGO:0000001\t0.4200"
        finally:
            os.unlink(path)

    def test_apply_per_aspect_scores_routes_universal(self):
        import numpy as np

        from protea.core.operations import _run_cafa_artifacts as _art

        df = pd.DataFrame(
            [
                {
                    "protein_accession": "P1",
                    "go_id": "GO:1",
                    "aspect": "F",
                    "distance": 0.1,
                    "qualifier": "enables",
                    "evidence_code": "EXP",
                    "taxonomic_relation": "same",
                },
                {
                    "protein_accession": "P2",
                    "go_id": "GO:2",
                    "aspect": "P",
                    "distance": 0.3,
                    "qualifier": "enables",
                    "evidence_code": "EXP",
                    "taxonomic_relation": "same",
                },
            ]
        )
        aspect_models = {
            "F": {"model": "m-uni", "cat_codes": None, "universal": self._UNIVERSAL_CTX},
            "P": {"model": "m-cell", "cat_codes": None, "universal": None},
        }
        with (
            patch("protea.core.reranker.model_from_string", return_value=MagicMock()),
            patch(
                "protea.core._universal_reranker.score_universal",
                return_value=np.array([0.9]),
            ) as mock_uni,
            patch("protea.core.reranker.predict", return_value=np.array([0.1])) as mock_generic,
        ):
            _art._apply_per_aspect_scores(df, aspect_models)
        mock_uni.assert_called_once()
        mock_generic.assert_called_once()
        assert df.loc[df["aspect"] == "F", "score"].iloc[0] == 0.9
        assert df.loc[df["aspect"] == "P", "score"].iloc[0] == 0.1


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
        session.get.side_effect = [eval_set, pred_set, snapshot]

        with pytest.raises(ValueError, match="No delta proteins"):
            self.op.execute(
                session,
                {"evaluation_set_id": EVAL_SET_ID, "prediction_set_id": PRED_SET_ID},
                emit=self.emit,
            )

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_missing_scoring_config(self, mock_compute):
        mock_compute.return_value = (_make_eval_data(), uuid.uuid4())
        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        snapshot = _make_snapshot()
        # get calls: eval_set, pred_set, ann_old, snapshot, scoring_config (None)
        session.get.side_effect = [eval_set, pred_set, snapshot, None]

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
        session.get.side_effect = [eval_set, pred_set, snapshot]

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
        session.get.side_effect = [eval_set, pred_set, snapshot]

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
        session.get.side_effect = [eval_set, pred_set, snapshot]

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
    def test_cafa_eval_failure_catches_exception(self, mock_compute):
        """When cafa_eval raises for one setting, it should log warning and continue."""
        mock_compute.return_value = (_make_eval_data(), uuid.uuid4())

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        snapshot = _make_snapshot()
        session.get.side_effect = [eval_set, pred_set, snapshot]

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        with patch("protea.core.operations._run_cafa_artifacts.download_obo"):
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

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_ia_missing_warning(self, mock_compute):
        """When no IA file and no ia_url, a warning should be emitted."""
        mock_compute.return_value = (_make_eval_data(), uuid.uuid4())

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        snapshot = _make_snapshot(ia_url=None)  # no ia_url
        session.get.side_effect = [eval_set, pred_set, snapshot]

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
        session.get.side_effect = [eval_set, pred_set, snapshot]

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
        session.get.side_effect = [eval_set, pred_set, snapshot]

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
        session.get.side_effect = [_make_eval_set(), _make_pred_set(), snapshot]
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
        session.get.side_effect = [eval_set, pred_set, snapshot]

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
        session.get.side_effect = [eval_set, pred_set, snapshot]

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

    @patch("protea.core.operations.run_cafa_evaluation.load_evaluation_data_for_set")
    def test_scoring_config_snapshot(self, mock_compute):
        """When scoring_config_id is provided and found, it snapshots the config."""
        mock_compute.return_value = (_make_eval_data(), uuid.uuid4())

        session = MagicMock()
        eval_set = _make_eval_set()
        pred_set = _make_pred_set()
        snapshot = _make_snapshot()
        scoring_cfg = MagicMock()
        scoring_cfg.formula = "linear"
        scoring_cfg.weights = {"embedding_similarity": 1.0}
        scoring_cfg.params = None
        session.get.side_effect = [eval_set, pred_set, snapshot, scoring_cfg]

        query = MagicMock()
        session.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.yield_per.return_value = []

        with (
            patch("protea.core.operations._run_cafa_artifacts.download_obo"),
            patch(
                "cafaeval.evaluation.cafa_eval",
                return_value=(MagicMock(), _dfs_best_fixture()),
            ),
            patch("protea.core.operations.run_cafa_evaluation.ScoringConfig") as mock_sc_cls,
        ):
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
            params=None,
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
            pred_set_id=uuid.uuid4(),
            delta_proteins=set(),
            max_distance=None,
            artifacts_root=__import__("pathlib").Path("/tmp"),
            has_rerankers=False,
            reranker_models={},
            scoring_config_snapshot=None,
            data=EvaluationData(),
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
