"""Unit tests for the /showcase router.

The router was rewritten 2026-04-10 to emit a single attributed "best" result
instead of the previous three-bucket ``method_comparison`` table.  These tests
cover the new shape: empty state, single-best selection, embedding attribution,
and pipeline-stage counts.

Database is fully mocked — no real infrastructure required.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.showcase import _avg_primary, _flatten_cells, router

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app(session_factory):
    app = FastAPI()
    app.state.session_factory = session_factory
    app.include_router(router)
    return app


@contextmanager
def _mock_scope(session):
    yield session


def _make_eval(results, scoring_config_id=None, reranker_model_id=None):
    er = MagicMock()
    er.id = uuid4()
    er.evaluation_set_id = uuid4()
    er.scoring_config_id = scoring_config_id
    er.reranker_model_id = reranker_model_id
    er.results = results
    return er


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
    cfg.display_name = display_name
    cfg.family = family
    cfg.param_count = param_count
    return cfg


def _install_mock(
    session,
    *,
    approx_counts=(0, 0, 0, 0),
    direct_scalars=(0, 0, 0),
    eval_rows=(),
):
    """Wire up ``session.scalar`` and ``session.execute`` mocks.

    The router uses two distinct count patterns:
    * ``_approx_count(table)`` → ``session.execute(text(...)).scalar()`` for
      protein / sequence / sequence_embedding / go_prediction.
    * ``session.scalar(select(...))`` for canonical_proteins,
      total_prediction_sets, total_rerankers (and a few more).

    ``approx_counts`` feeds the first pattern; ``direct_scalars`` the second.
    ``eval_rows`` is the sequence yielded by the matrix join's ``.all()``.
    """
    direct_iter = iter(direct_scalars)
    session.scalar.side_effect = lambda *a, **kw: next(direct_iter, 0)

    approx_iter = iter(approx_counts)

    def _execute(*args, **kwargs):
        result = MagicMock()
        # Each call gets its own next-approx-count for .scalar(), and the
        # eval_rows for .all() (the matrix call uses .all()).
        result.scalar.return_value = next(approx_iter, 0)
        result.all.return_value = eval_rows
        return result

    session.execute.side_effect = _execute


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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
        "protea.api.routers.showcase.session_scope",
        side_effect=lambda _: _mock_scope(session),
    ):
        with TestClient(app) as c:
            yield c, session


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestAvgPrimary:
    def test_empty_results(self):
        mean, metric = _avg_primary({})
        assert mean is None
        assert metric == "f_micro_w"

    def test_all_cells_populated_fmax_fallback(self):
        # No f_micro_w anywhere -> falls back to fmax and flags metric=fmax.
        results = {
            "NK": {"BPO": {"fmax": 0.3}, "MFO": {"fmax": 0.5}, "CCO": {"fmax": 0.4}},
            "LK": {"BPO": {"fmax": 0.6}, "MFO": {"fmax": 0.7}, "CCO": {"fmax": 0.5}},
            "PK": {"BPO": {"fmax": 0.8}, "MFO": {"fmax": 0.9}, "CCO": {"fmax": 0.7}},
        }
        mean, metric = _avg_primary(results)
        assert mean is not None
        assert round(mean, 4) == round(sum([0.3, 0.5, 0.4, 0.6, 0.7, 0.5, 0.8, 0.9, 0.7]) / 9, 4)
        assert metric == "fmax"

    def test_ia_weighted_preferred_over_fmax(self):
        # Every cell carries f_micro_w -> headline metric is f_micro_w and the
        # mean ignores the (lower) unweighted fmax.
        results = {
            "NK": {"BPO": {"fmax": 0.4, "f_micro_w": 0.6}},
            "LK": {"BPO": {"fmax": 0.5, "f_micro_w": 0.8}},
        }
        mean, metric = _avg_primary(results)
        assert mean == pytest.approx(0.7)
        assert metric == "f_micro_w"

    def test_mixed_ia_and_legacy_flags_fmax(self):
        # One cell IA-weighted, one legacy -> the headline is honestly demoted
        # to fmax so a non-IA cell never rides the f_micro_w label.
        results = {
            "NK": {"BPO": {"fmax": 0.4, "f_micro_w": 0.6}},
            "LK": {"BPO": {"fmax": 0.5}},
        }
        mean, metric = _avg_primary(results)
        assert mean == pytest.approx((0.6 + 0.5) / 2)
        assert metric == "fmax"

    def test_missing_cells_are_ignored(self):
        # Only 2 out of 9 cells populated — should average those two
        results = {
            "NK": {"BPO": {"fmax": 0.4}},
            "LK": {"BPO": {"fmax": 0.6}},
        }
        mean, _ = _avg_primary(results)
        assert mean == pytest.approx(0.5)

    def test_none_fmax_cells_are_ignored(self):
        results = {"NK": {"BPO": {"fmax": None}, "MFO": {"fmax": 0.42}}}
        mean, _ = _avg_primary(results)
        assert mean == pytest.approx(0.42)


class TestFlattenCells:
    def test_flatten_skips_missing(self):
        results = {
            "NK": {"BPO": {"fmax": 0.4, "precision": 0.5, "recall": 0.3}},
            "LK": {"MFO": {"fmax": 0.6}},
        }
        flat = _flatten_cells(results)
        assert len(flat) == 2
        nk_bpo = next(c for c in flat if c["category"] == "NK" and c["aspect"] == "BPO")
        assert nk_bpo["fmax"] == 0.4
        # No f_micro_w -> primary falls back to fmax and is flagged as such.
        assert nk_bpo["primary"] == 0.4
        assert nk_bpo["primary_metric"] == "fmax"
        assert nk_bpo["f_micro_w"] is None
        assert nk_bpo["precision"] == 0.5
        assert nk_bpo["recall"] == 0.3

    def test_ia_weighted_cell_drives_primary(self):
        results = {"NK": {"BPO": {"fmax": 0.4, "f_micro_w": 0.7}}}
        flat = _flatten_cells(results)
        assert flat[0]["primary"] == 0.7
        assert flat[0]["primary_metric"] == "f_micro_w"
        assert flat[0]["f_micro_w"] == 0.7

    def test_none_precision_recall_preserved(self):
        results = {"NK": {"BPO": {"fmax": 0.5}}}
        flat = _flatten_cells(results)
        assert flat[0]["precision"] is None
        assert flat[0]["recall"] is None


# ---------------------------------------------------------------------------
# GET /showcase — empty database
# ---------------------------------------------------------------------------


class TestShowcaseEmpty:
    def test_empty_database_returns_zero_counts_and_null_best(self, client):
        c, session = client
        _install_mock(session, eval_rows=[])

        resp = c.get("/showcase")
        assert resp.status_code == 200
        data = resp.json()

        assert data["best"] is None
        assert data["counts"]["proteins"] == 0
        assert data["counts"]["evaluations"] == 0
        assert len(data["pipeline_stages"]) == 5
        assert {s["name"] for s in data["pipeline_stages"]} == {
            "sequences",
            "embeddings",
            "predictions",
            "reranker_models",
            "evaluations",
        }

    def test_empty_state_still_reports_stage_hrefs(self, client):
        c, session = client
        _install_mock(session, eval_rows=[])

        resp = c.get("/showcase")
        data = resp.json()
        for s in data["pipeline_stages"]:
            assert "href" in s
            assert s["count"] == 0


# ---------------------------------------------------------------------------
# GET /showcase — with evaluation data
# ---------------------------------------------------------------------------


class TestShowcaseBestSelection:
    def test_single_evaluation_becomes_the_best(self, client):
        c, session = client
        cfg = _make_cfg(
            model_name="esmc_300m",
            backend="esm3c",
            display_name="ESMC-300M",
            family="esmc",
            param_count=300_000_000,
        )
        er = _make_eval(results={"NK": {"BPO": {"fmax": 0.5, "precision": 0.6, "recall": 0.4}}})

        _install_mock(
            session,
            approx_counts=[10, 5, 4, 8],
            direct_scalars=[1, 100, 3],
            eval_rows=[(er, cfg, "alignment_weighted")],
        )

        resp = c.get("/showcase")
        assert resp.status_code == 200
        data = resp.json()

        assert data["counts"]["evaluations"] == 1
        assert data["best"] is not None
        assert data["best"]["avg_primary"] == 0.5
        assert data["best"]["stage"] == "alignment_weighted"
        assert data["best"]["embedding"]["display_name"] == "ESMC-300M"
        assert data["best"]["embedding"]["family"] == "esmc"
        assert len(data["best"]["per_cell"]) == 1

    def test_best_picks_highest_avg_fmax(self, client):
        c, session = client
        cfg1 = _make_cfg("esmc_300m", "esm3c", display_name="ESMC-300M", family="esmc")
        cfg2 = _make_cfg("Rostlab/ProstT5", "t5", display_name="ProstT5-XL", family="prostt5")

        # er1 averages 0.40, er2 averages 0.60 → er2 must win
        er1 = _make_eval(results={"NK": {"BPO": {"fmax": 0.40}}})
        er2 = _make_eval(
            results={"NK": {"BPO": {"fmax": 0.60}}},
            reranker_model_id=uuid4(),
        )

        _install_mock(
            session,
            eval_rows=[(er1, cfg1, "alignment_weighted"), (er2, cfg2, None)],
        )

        resp = c.get("/showcase")
        data = resp.json()
        assert data["best"]["avg_primary"] == 0.6
        assert data["best"]["stage"] == "reranker"
        assert data["best"]["embedding"]["display_name"] == "ProstT5-XL"

    def test_eval_result_with_empty_results_blob_is_skipped(self, client):
        c, session = client
        cfg = _make_cfg(display_name="ESMC-300M", family="esmc")
        # er1 has no fmax values at all (empty dict) — must be skipped for best
        # er2 has one cell — becomes the best
        er1 = _make_eval(results={})
        er2 = _make_eval(results={"PK": {"MFO": {"fmax": 0.33}}})

        _install_mock(
            session,
            eval_rows=[(er1, cfg, "alignment_weighted"), (er2, cfg, "alignment_weighted")],
        )

        resp = c.get("/showcase")
        data = resp.json()
        # total_evaluations counts both rows (it's len(rows)), but best came from er2
        assert data["counts"]["evaluations"] == 2
        assert data["best"] is not None
        assert data["best"]["avg_primary"] == 0.33

    def test_all_empty_results_leaves_best_null(self, client):
        c, session = client
        cfg = _make_cfg(display_name="ESMC-300M", family="esmc")
        er = _make_eval(results={})
        _install_mock(session, eval_rows=[(er, cfg, "alignment_weighted")])

        resp = c.get("/showcase")
        data = resp.json()
        assert data["counts"]["evaluations"] == 1
        assert data["best"] is None


class TestShowcasePerTask:
    def test_per_task_headlines_mean_and_ci_across_models(self, client):
        c, session = client
        cfg1 = _make_cfg("esmc_300m", "esm3c", display_name="ESMC-300M", family="esmc")
        cfg2 = _make_cfg("Rostlab/ProstT5", "t5", display_name="ProstT5-XL", family="prostt5")
        # Two models, same (NK, BPO) cell -> mean 0.5, max 0.6 (best-cell).
        er1 = _make_eval(results={"NK": {"BPO": {"fmax": 0.4, "f_micro_w": 0.4}}})
        er2 = _make_eval(results={"NK": {"BPO": {"fmax": 0.6, "f_micro_w": 0.6}}})

        _install_mock(
            session,
            eval_rows=[(er1, cfg1, "alignment_weighted"), (er2, cfg2, "alignment_weighted")],
        )

        resp = c.get("/showcase")
        data = resp.json()
        assert data["primary_metric"] == "f_micro_w"
        per_task = data["per_task"]
        nk_bpo = next(t for t in per_task if t["category"] == "NK" and t["aspect"] == "BPO")
        assert nk_bpo["mean"] == pytest.approx(0.5)
        assert nk_bpo["max"] == pytest.approx(0.6)
        assert nk_bpo["n_models"] == 2
        assert nk_bpo["metric"] == "f_micro_w"
        # The best-cell maximum (0.6) must exceed the headline mean (0.5):
        # this is exactly the winner's-curse gap the front-end must not headline.
        assert nk_bpo["max"] > nk_bpo["mean"]

    def test_per_task_empty_when_no_evaluations(self, client):
        c, session = client
        _install_mock(session, eval_rows=[])
        resp = c.get("/showcase")
        assert resp.json()["per_task"] == []
