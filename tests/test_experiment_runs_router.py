"""Tests for ``protea.api.routers.experiment_runs`` (T4.7)."""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.experiment_runs import router
from protea.infrastructure.orm.models.experiment_run import ExperimentRunStatus


@contextmanager
def _mock_scope(session):
    yield session


def _make_run(
    run_id=None,
    name="ablation-2026-05-09",
    status: ExperimentRunStatus = ExperimentRunStatus.PLANNED,
    started_at=None,
    finished_at=None,
):
    run = MagicMock()
    run.id = run_id or uuid4()
    run.name = name
    run.description = None
    run.hypothesis = None
    run.findings = None
    run.status = status
    run.config = {}
    run.provenance = {}
    run.tags = []
    run.created_at = MagicMock()
    run.created_at.isoformat.return_value = "2024-01-01T00:00:00"
    run.started_at = started_at
    run.finished_at = finished_at
    return run


def _make_app(factory):
    app = FastAPI()
    app.state.session_factory = factory
    app.include_router(router)
    return app


@pytest.fixture()
def session():
    return MagicMock()


# ---------------------------------------------------------------------------
# POST /experiment-runs
# ---------------------------------------------------------------------------


class TestCreate:
    def test_returns_201_and_payload(self, session) -> None:
        run = _make_run()
        # No existing row with the same name.
        session.query.return_value.filter.return_value.first.return_value = None
        session.flush = MagicMock()

        factory = MagicMock()
        app = _make_app(factory)

        with (
            patch(
                "protea.api.routers.experiment_runs.session_scope",
                side_effect=lambda _: _mock_scope(session),
            ),
            patch(
                "protea.api.routers.experiment_runs.ExperimentRun",
                return_value=run,
            ),
        ):
            c = TestClient(app)
            resp = c.post("/experiment-runs", json={"name": "ablation-2026-05-09"})

        assert resp.status_code == 201
        body = resp.json()
        assert body["name"] == "ablation-2026-05-09"
        assert body["status"] == "planned"

    def test_duplicate_name_returns_409(self, session) -> None:
        existing = MagicMock()
        existing.id = uuid4()
        session.query.return_value.filter.return_value.first.return_value = existing

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.experiment_runs.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            c = TestClient(app, raise_server_exceptions=False)
            resp = c.post("/experiment-runs", json={"name": "dup"})

        assert resp.status_code == 409

    def test_blank_name_returns_422(self, session) -> None:
        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.experiment_runs.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            c = TestClient(app, raise_server_exceptions=False)
            resp = c.post("/experiment-runs", json={"name": "   "})

        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /experiment-runs[/{id}]
# ---------------------------------------------------------------------------


class TestRead:
    def test_list_returns_rows(self, session) -> None:
        run = _make_run()
        q = MagicMock()
        q.order_by.return_value.limit.return_value.all.return_value = [run]
        session.query.return_value = q
        q.filter.return_value = q

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.experiment_runs.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            c = TestClient(app)
            resp = c.get("/experiment-runs")

        assert resp.status_code == 200
        rows = resp.json()
        assert isinstance(rows, list)
        assert rows[0]["name"] == run.name

    def test_after_cursor_filters_by_created_at(self, session) -> None:
        """T4.2: ``after`` adds a ``created_at < after`` clause."""
        run = _make_run()
        q = MagicMock()
        q.order_by.return_value.limit.return_value.all.return_value = [run]
        session.query.return_value = q
        q.filter.return_value = q

        factory = MagicMock()
        app = _make_app(factory)

        cursor = "2026-04-01T10:00:00Z"
        with patch(
            "protea.api.routers.experiment_runs.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            c = TestClient(app)
            resp = c.get(f"/experiment-runs?after={cursor}")

        assert resp.status_code == 200
        # The handler calls filter() once for the cursor (no status filter
        # in this test) so call_count is exactly 1.
        assert q.filter.call_count == 1

    def test_get_one_returns_row(self, session) -> None:
        run = _make_run()
        session.get.return_value = run

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.experiment_runs.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            c = TestClient(app)
            resp = c.get(f"/experiment-runs/{run.id}")

        assert resp.status_code == 200
        assert resp.json()["id"] == str(run.id)

    def test_get_missing_returns_404(self, session) -> None:
        session.get.return_value = None

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.experiment_runs.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            c = TestClient(app, raise_server_exceptions=False)
            resp = c.get(f"/experiment-runs/{uuid4()}")

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# PATCH /experiment-runs/{id}
# ---------------------------------------------------------------------------


class TestPatch:
    def test_status_transition_to_running_stamps_started_at(self, session) -> None:
        run = _make_run()
        session.get.return_value = run
        session.flush = MagicMock()

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.experiment_runs.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            c = TestClient(app)
            resp = c.patch(
                f"/experiment-runs/{run.id}",
                json={"status": "running"},
            )

        assert resp.status_code == 200
        assert run.status == ExperimentRunStatus.RUNNING
        assert run.started_at is not None
        assert run.finished_at is None

    def test_status_transition_to_done_stamps_finished_at(self, session) -> None:
        run = _make_run(status=ExperimentRunStatus.RUNNING)
        session.get.return_value = run

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.experiment_runs.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            c = TestClient(app)
            resp = c.patch(
                f"/experiment-runs/{run.id}",
                json={"status": "done", "findings": "Fmax +0.03 over baseline"},
            )

        assert resp.status_code == 200
        assert run.status == ExperimentRunStatus.DONE
        assert run.finished_at is not None
        assert run.findings == "Fmax +0.03 over baseline"

    def test_omitted_field_left_untouched(self, session) -> None:
        run = _make_run()
        run.description = "original"
        session.get.return_value = run

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.experiment_runs.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            c = TestClient(app)
            # Only ``hypothesis`` in the payload — description must stay.
            resp = c.patch(
                f"/experiment-runs/{run.id}",
                json={"hypothesis": "lineage helps PK"},
            )

        assert resp.status_code == 200
        assert run.description == "original"
        assert run.hypothesis == "lineage helps PK"

    def test_missing_run_returns_404(self, session) -> None:
        session.get.return_value = None

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.experiment_runs.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            c = TestClient(app, raise_server_exceptions=False)
            resp = c.patch(f"/experiment-runs/{uuid4()}", json={"description": "x"})

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /experiment-runs/{id}
# ---------------------------------------------------------------------------


class TestDelete:
    def test_delete_returns_204(self, session) -> None:
        run = _make_run()
        session.get.return_value = run

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.experiment_runs.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            c = TestClient(app)
            resp = c.delete(f"/experiment-runs/{run.id}")

        assert resp.status_code == 204
        session.delete.assert_called_once_with(run)

    def test_missing_run_returns_404(self, session) -> None:
        session.get.return_value = None

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.experiment_runs.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            c = TestClient(app, raise_server_exceptions=False)
            resp = c.delete(f"/experiment-runs/{uuid4()}")

        assert resp.status_code == 404
