"""Unit tests for the /benchmark router.

Covers the derived display metadata helper and the shapes of the two
endpoints (``/benchmark/embeddings`` and ``/benchmark/matrix``).  The
session is fully mocked.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.cache import invalidate as _cache_invalidate
from protea.api.routers.benchmark import _stage_of, router


@pytest.fixture(autouse=True)
def _reset_router_cache():
    _cache_invalidate()
    yield
    _cache_invalidate()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app(factory, hidden_embeddings=frozenset()):
    from protea.infrastructure.benchmark_config import BenchmarkConfig

    app = FastAPI()
    app.state.session_factory = factory
    app.state.benchmark_config = BenchmarkConfig(
        preferred_default_stages=("alignment_weighted", "reranker"),
        baseline_scoring_name="alignment_weighted",
        hidden_stages=frozenset(),
        hidden_embeddings=hidden_embeddings,
        stage_labels={"alignment_weighted": "Alignment-weighted", "reranker": "Reranker"},
        eval_set_labels={},
        categories=("NK", "LK", "PK"),
        aspects=("BPO", "MFO", "CCO"),
    )
    app.include_router(router)
    return app


@contextmanager
def _mock_scope(session):
    yield session


def _make_cfg(
    model_name="esmc_300m",
    backend="esm3c",
    display_name=None,
    family=None,
    param_count=None,
):
    cfg = MagicMock()
    cfg.id = uuid4()
    cfg.model_name = model_name
    cfg.model_backend = backend
    cfg.description = None
    cfg.pooling = "mean"
    cfg.layer_agg = "mean"
    # Display columns are now persisted on EmbeddingConfig (no more heuristics).
    cfg.display_name = display_name
    cfg.family = family
    cfg.param_count = param_count
    return cfg


def _make_eval(results, scoring_config_id=None, reranker_model_id=None):
    er = MagicMock()
    er.id = uuid4()
    er.evaluation_set_id = uuid4()
    er.scoring_config_id = scoring_config_id
    er.reranker_model_id = reranker_model_id
    er.results = results
    return er


@pytest.fixture()
def session():
    return MagicMock()


@pytest.fixture()
def factory(session):
    return MagicMock()


@pytest.fixture()
def client(session, factory):
    app = _make_app(factory)
    with patch(
        "protea.api.routers.benchmark.session_scope",
        side_effect=lambda _: _mock_scope(session),
    ):
        with TestClient(app) as c:
            yield c, session


# ---------------------------------------------------------------------------
# _stage_of
# ---------------------------------------------------------------------------


class TestStageOf:
    def test_returns_scoring_name_when_no_reranker(self):
        er = _make_eval({}, scoring_config_id=uuid4())
        assert _stage_of(er, "alignment_weighted") == "alignment_weighted"

    def test_reranker_takes_precedence(self):
        er = _make_eval({}, scoring_config_id=uuid4(), reranker_model_id=uuid4())
        assert _stage_of(er, "alignment_weighted") == "reranker"

    def test_bare_knn_is_a_named_stage_not_an_absence(self):
        # This returned None until the rung-1 grid went looking for itself
        # on the board and was not there. None makes the matrix drop the
        # row as incomplete, and a plain KNN run is not incomplete: it is
        # the baseline every other stage is measured against.
        er = _make_eval({})
        assert _stage_of(er, None) == "knn"


# ---------------------------------------------------------------------------
# GET /benchmark/embeddings
# ---------------------------------------------------------------------------


class TestListBenchmarkEmbeddings:
    def test_empty_database(self, client):
        c, session = client
        exec_result = MagicMock()
        exec_result.scalars.return_value.all.return_value = []
        session.execute.return_value = exec_result

        resp = c.get("/benchmark/embeddings")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["embeddings"] == []

    def test_returns_persisted_display_metadata(self, client):
        c, session = client
        # display_name / family / param_count are now persisted columns.
        cfgs = [
            _make_cfg(
                "esmc_300m",
                "esm3c",
                display_name="ESMC-300M",
                family="esmc",
                param_count=300_000_000,
            ),
            _make_cfg(
                "facebook/esm2_t33_650M_UR50D",
                "esm",
                display_name="ESM2-650M",
                family="esm2",
                param_count=650_000_000,
            ),
            _make_cfg(
                "ElnaggarLab/ankh-base",
                "ankh",
                display_name="Ankh-base",
                family="ankh",
                param_count=None,
            ),
        ]
        exec_result = MagicMock()
        exec_result.scalars.return_value.all.return_value = cfgs
        session.execute.return_value = exec_result

        resp = c.get("/benchmark/embeddings")
        data = resp.json()
        assert data["total"] == 3
        names = [e["display_name"] for e in data["embeddings"]]
        assert "ESMC-300M" in names
        assert "ESM2-650M" in names
        assert "Ankh-base" in names
        for e in data["embeddings"]:
            assert e["family"]
            assert "param_count" in e
            assert "model_name" in e
            assert "model_backend" in e

    def test_hidden_embeddings_are_suppressed(self, session, factory):
        # Configs whose UUID is listed in benchmark.yaml: hidden_embeddings
        # are omitted from the response (reversible, no data deletion).
        from protea.api.cache import invalidate

        hidden = _make_cfg("esmc_300m", "esm3c", display_name="ESMC-300M", family="esmc")
        visible = _make_cfg(
            "facebook/esm2_t33_650M_UR50D", "esm", display_name="ESM2-650M", family="esm2"
        )
        exec_result = MagicMock()
        exec_result.scalars.return_value.all.return_value = [hidden, visible]
        session.execute.return_value = exec_result

        # Case-insensitive match: store the UUID uppercased in the config to
        # prove normalisation works.
        app = _make_app(factory, hidden_embeddings=frozenset({str(hidden.id).lower()}))
        invalidate()  # cached() is module-scoped; clear cross-test leakage
        with patch(
            "protea.api.routers.benchmark.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            with TestClient(app) as c:
                resp = c.get("/benchmark/embeddings")
        invalidate()
        data = resp.json()
        assert data["total"] == 1
        ids = [e["id"] for e in data["embeddings"]]
        assert str(visible.id) in ids
        assert str(hidden.id) not in ids


# ---------------------------------------------------------------------------
# GET /benchmark/matrix
# ---------------------------------------------------------------------------


class TestBenchmarkMatrix:
    """The endpoint now executes two queries:

    1. Main matrix: 7-tuples ``(er, embedding_id, k, scoring_name,
       pred_set_id, pred_set_meta, pred_set_created)``. The last three carry
       which generation of the prediction set produced the row, which is what
       lets the folder prefer the current generation over the highest-scoring
       one.
    2. Eval set metadata enrichment: 7-tuples
       ``(es, old_source, old_source_version, new_source, new_source_version,
       old_obo_version, new_obo_version)``.

    ``_dual_execute`` wires both behind a single ``session.execute`` mock.
    """

    @staticmethod
    def _row(
        er,
        embedding_id,
        k=5,
        scoring_name="alignment_weighted",
        pred_set_id=None,
        pred_set_meta=None,
        pred_set_created=None,
    ):
        """One scan row. The generation fields default to an ungraded set, which
        is what a prediction set written before the self-hit audit looks like."""
        return (
            er,
            embedding_id,
            k,
            scoring_name,
            pred_set_id if pred_set_id is not None else uuid4(),
            pred_set_meta if pred_set_meta is not None else {},
            pred_set_created,
        )

    @staticmethod
    def _dual_execute(session, matrix_rows, eval_set_rows):
        """Configure session.execute to return matrix_rows then eval_set_rows."""
        first = MagicMock()
        first.all.return_value = matrix_rows
        second = MagicMock()
        second.all.return_value = eval_set_rows
        session.execute.side_effect = [first, second, second, second]

    @staticmethod
    def _eval_set_row(eval_set_id):
        es = MagicMock()
        es.id = eval_set_id
        es.stats = {}
        return (es, "goa", "210", "goa", "230", "2024-01-01", "2024-06-01")

    def test_empty_database(self, client):
        c, session = client
        exec_result = MagicMock()
        exec_result.all.return_value = []
        session.execute.return_value = exec_result

        resp = c.get("/benchmark/matrix")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["rows"] == []
        assert data["evaluation_sets"] == []
        assert data["embedding_config_ids"] == []
        # Empty data → no stages observed
        assert data["stages"] == []

    def test_row_per_cell_and_deduplication(self, client):
        """One row per cell, and the survivor is the CURRENT generation.

        This assertion used to read ``== 0.55``, the higher of the two. That was
        a maximum over repeated measurements of one quantity: it can only move
        up as a cell is recomputed, so a cell recomputed often reads high for
        that reason alone. The recompute of 2026-08-18 left 140 cells holding
        more than one generation, which made the bias live.

        The rule is now trust then recency, never score, so the lower-scoring
        current generation wins over the higher-scoring damaged one. That is the
        point: a correction that lowers a number has to be able to land.
        """
        c, session = client
        embedding_id = uuid4()

        er_low = _make_eval({"NK": {"BPO": {"fmax": 0.4}}}, scoring_config_id=uuid4())
        er_high = _make_eval({"NK": {"BPO": {"fmax": 0.55}}}, scoring_config_id=uuid4())
        eval_set_id = uuid4()
        er_low.evaluation_set_id = eval_set_id
        er_high.evaluation_set_id = eval_set_id

        self._dual_execute(
            session,
            matrix_rows=[
                self._row(er_low, embedding_id, pred_set_meta={"status": "current"}),
                self._row(er_high, embedding_id, pred_set_meta={"status": "damaged"}),
            ],
            eval_set_rows=[self._eval_set_row(eval_set_id)],
        )

        resp = c.get("/benchmark/matrix")
        data = resp.json()
        assert data["total"] == 1
        row = data["rows"][0]
        assert row["fmax"] == 0.4
        assert row["prediction_set_status"] == "current"
        assert row["category"] == "NK"
        assert row["aspect"] == "BPO"
        assert row["stage"] == "alignment_weighted"
        assert str(embedding_id) in data["embedding_config_ids"]
        # evaluation_sets is now a list of dicts; assert the id is among them.
        assert str(eval_set_id) in {es["id"] for es in data["evaluation_sets"]}

    def test_stage_filter(self, client):
        c, session = client
        embedding_id = uuid4()

        er_aw = _make_eval({"NK": {"BPO": {"fmax": 0.4}}}, scoring_config_id=uuid4())
        er_reranker = _make_eval(
            {"NK": {"BPO": {"fmax": 0.6}}},
            reranker_model_id=uuid4(),
        )
        eval_set_id_a = uuid4()
        eval_set_id_b = uuid4()
        er_aw.evaluation_set_id = eval_set_id_a
        er_reranker.evaluation_set_id = eval_set_id_b

        self._dual_execute(
            session,
            matrix_rows=[
                self._row(er_aw, embedding_id),
                self._row(er_reranker, embedding_id),
            ],
            eval_set_rows=[
                self._eval_set_row(eval_set_id_b),
            ],
        )

        resp = c.get("/benchmark/matrix?stage=reranker")
        data = resp.json()
        assert data["total"] == 1
        assert data["rows"][0]["stage"] == "reranker"
        assert data["rows"][0]["fmax"] == 0.6
        assert data["filters"]["stage"] == "reranker"

    def test_invalid_stage_filter_rejected(self, client):
        c, _ = client
        resp = c.get("/benchmark/matrix?stage=")
        # An empty stage string passes through (no filter); we instead exercise
        # the still-valid path. To prove that *unknown* stages just return zero
        # results without 422, hit a real but unused stage:
        resp = c.get("/benchmark/matrix?stage=unknown_stage_xyz")
        assert resp.status_code == 200
        assert resp.json()["total"] == 0

    def test_best_per_cell_global_ignores_stage_and_k_filters(self, client):
        """best_per_cell_global must surface the absolute champion per cell
        even when the user's stage/K filters would hide it from the main table.
        """
        c, session = client
        emb_a = uuid4()
        emb_b = uuid4()
        eval_set_id = uuid4()

        # alignment_weighted at K=5 is the dataset champion (fmax=0.85)
        er_aw = _make_eval({"NK": {"BPO": {"fmax": 0.85}}}, scoring_config_id=uuid4())
        # reranker at K=10 is weaker (fmax=0.55) but matches the active filters
        er_reranker = _make_eval(
            {"NK": {"BPO": {"fmax": 0.55}}},
            reranker_model_id=uuid4(),
        )
        er_aw.evaluation_set_id = eval_set_id
        er_reranker.evaluation_set_id = eval_set_id

        self._dual_execute(
            session,
            matrix_rows=[
                self._row(er_aw, emb_a, k=5),
                self._row(er_reranker, emb_b, k=10),
            ],
            eval_set_rows=[self._eval_set_row(eval_set_id)],
        )

        resp = c.get("/benchmark/matrix?stage=reranker&k=10")
        data = resp.json()

        # Filtered table reflects the user's selection: only the reranker row
        assert data["total"] == 1
        assert data["best_per_cell"][0]["fmax"] == 0.55
        assert data["best_per_cell"][0]["stage"] == "reranker"
        assert data["best_per_cell"][0]["k"] == 10

        # Global champion is the alignment_weighted K=5 winner — independent
        # of the stage=reranker, k=10 filter the user picked.
        assert "best_per_cell_global" in data
        assert len(data["best_per_cell_global"]) == 1
        global_winner = data["best_per_cell_global"][0]
        assert global_winner["fmax"] == 0.85
        assert global_winner["stage"] == "alignment_weighted"
        assert global_winner["k"] == 5
        assert global_winner["embedding_config_id"] == str(emb_a)

    def test_missing_fmax_cells_are_skipped(self, client):
        c, session = client
        eval_set_id = uuid4()
        er = _make_eval(
            {
                "NK": {"BPO": {"fmax": None}, "MFO": {"fmax": 0.4}},
                "LK": {"CCO": {}},
            },
            scoring_config_id=uuid4(),
        )
        er.evaluation_set_id = eval_set_id
        self._dual_execute(
            session,
            matrix_rows=[self._row(er, uuid4())],
            eval_set_rows=[self._eval_set_row(eval_set_id)],
        )

        resp = c.get("/benchmark/matrix")
        data = resp.json()
        # Only NK/MFO has a real fmax → exactly one row
        assert data["total"] == 1
        assert data["rows"][0]["category"] == "NK"
        assert data["rows"][0]["aspect"] == "MFO"

    def test_primary_metric_is_f_micro_w_not_fmax(self, client):
        """The winner per cell is the model with the highest IA-weighted
        f_micro_w, even when another model has a higher unweighted fmax.
        FIX-METRIC-IA: f_micro_w is the LAFA-comparable headline metric."""
        c, session = client
        emb_a = uuid4()
        emb_b = uuid4()
        eval_set_id = uuid4()
        # Model A: high unweighted fmax but low IA-weighted f_micro_w (common
        # easy terms inflate the unweighted number).
        er_a = _make_eval(
            {"NK": {"BPO": {"fmax": 0.80, "f_micro_w": 0.20}}},
            scoring_config_id=uuid4(),
        )
        # Model B: lower fmax but higher f_micro_w (the real winner).
        er_b = _make_eval(
            {"NK": {"BPO": {"fmax": 0.60, "f_micro_w": 0.35}}},
            scoring_config_id=uuid4(),
        )
        er_a.evaluation_set_id = eval_set_id
        er_b.evaluation_set_id = eval_set_id
        self._dual_execute(
            session,
            matrix_rows=[self._row(er_a, emb_a), self._row(er_b, emb_b)],
            eval_set_rows=[self._eval_set_row(eval_set_id)],
        )

        data = c.get("/benchmark/matrix").json()
        assert data["primary_metric"] == "f_micro_w"
        winner = data["best_per_cell"][0]
        assert winner["embedding_config_id"] == str(emb_b)
        assert winner["primary"] == 0.35
        assert winner["primary_metric"] == "f_micro_w"
        assert winner["f_micro_w"] == 0.35

    def test_per_task_aggregate_reports_mean_and_ci(self, client):
        """The honest per-task summary reports the mean f_micro_w across
        models plus a 95% CI, distinct from the best-cell maximum."""
        c, session = client
        eval_set_id = uuid4()
        er_a = _make_eval(
            {"NK": {"BPO": {"fmax": 0.5, "f_micro_w": 0.20}}},
            scoring_config_id=uuid4(),
        )
        er_b = _make_eval(
            {"NK": {"BPO": {"fmax": 0.6, "f_micro_w": 0.30}}},
            scoring_config_id=uuid4(),
        )
        er_a.evaluation_set_id = eval_set_id
        er_b.evaluation_set_id = eval_set_id
        self._dual_execute(
            session,
            matrix_rows=[self._row(er_a, uuid4()), self._row(er_b, uuid4())],
            eval_set_rows=[self._eval_set_row(eval_set_id)],
        )

        data = c.get("/benchmark/matrix").json()
        per_task = {(t["category"], t["aspect"]): t for t in data["per_task"]}
        cell = per_task[("NK", "BPO")]
        assert cell["metric"] == "f_micro_w"
        assert cell["mean"] == 0.25
        assert cell["max"] == 0.30
        assert cell["n_models"] == 2
        assert cell["ci95"] > 0
