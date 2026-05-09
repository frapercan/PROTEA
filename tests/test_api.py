"""
Unit tests for the FastAPI jobs router.
Database and pika are fully mocked — no real infrastructure required.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.jobs import router

# ---------------------------------------------------------------------------
# App fixture — minimal FastAPI app wired with mock state
# ---------------------------------------------------------------------------

FAKE_AMQP = "amqp://guest:guest@localhost/"


def _make_job(job_id=None, operation="ping", queue_name="test.q", status="queued"):
    job = MagicMock()
    job.id = job_id or uuid4()
    job.operation = operation
    job.queue_name = queue_name
    job.status = MagicMock()
    job.status.value = status
    job.payload = {}
    job.meta = {}
    job.created_at = MagicMock()
    job.created_at.isoformat.return_value = "2024-01-01T00:00:00"
    job.started_at = None
    job.finished_at = None
    job.progress_current = None
    job.progress_total = None
    job.error_code = None
    job.error_message = None
    return job


def _make_app(session_factory, amqp_url=FAKE_AMQP):
    app = FastAPI()
    app.state.session_factory = session_factory
    app.state.amqp_url = amqp_url
    app.state.operation_registry = MagicMock()
    app.include_router(router)
    return app


@contextmanager
def _mock_scope(session):
    """Replaces session_scope so the mock session is used directly."""
    yield session


@pytest.fixture()
def session():
    s = MagicMock()
    return s


@pytest.fixture()
def client(session):
    factory = MagicMock()
    app = _make_app(factory)

    with patch("protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)):
        yield TestClient(app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# POST /jobs
# ---------------------------------------------------------------------------


class TestCreateJob:
    def test_returns_job_id_and_status(self, session):
        job = _make_job()
        session.flush = MagicMock()
        # session.add is called for job and for JobEvent

        factory = MagicMock()
        app = _make_app(factory)

        with (
            patch(
                "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
            ),
            patch("protea.api.routers.jobs.publish_job"),
            patch("protea.api.routers.jobs.Job", return_value=job),
        ):
            c = TestClient(app)
            resp = c.post("/jobs", json={"operation": "ping", "queue_name": "test.q"})

        assert resp.status_code == 200
        body = resp.json()
        assert "id" in body
        assert body["status"] == "queued"

    def test_missing_operation_returns_422(self, session):
        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app, raise_server_exceptions=False)
            resp = c.post("/jobs", json={"queue_name": "test.q"})

        assert resp.status_code == 422

    def test_missing_queue_name_returns_422(self, session):
        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app, raise_server_exceptions=False)
            resp = c.post("/jobs", json={"operation": "ping"})

        assert resp.status_code == 422

    def test_publish_job_called_after_commit(self, session):
        job = _make_job()
        factory = MagicMock()
        app = _make_app(factory)

        with (
            patch(
                "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
            ),
            patch("protea.api.routers.jobs.publish_job") as mock_publish,
            patch("protea.api.routers.jobs.Job", return_value=job),
        ):
            c = TestClient(app)
            c.post("/jobs", json={"operation": "ping", "queue_name": "test.q"})

        mock_publish.assert_called_once_with(FAKE_AMQP, "test.q", job.id)

    def test_publish_failure_returns_500(self, session):
        job = _make_job()
        factory = MagicMock()
        app = _make_app(factory)

        with (
            patch(
                "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
            ),
            patch("protea.api.routers.jobs.publish_job", side_effect=RuntimeError("broker down")),
            patch("protea.api.routers.jobs.Job", return_value=job),
        ):
            c = TestClient(app, raise_server_exceptions=False)
            resp = c.post("/jobs", json={"operation": "ping", "queue_name": "test.q"})

        assert resp.status_code == 500


# ---------------------------------------------------------------------------
# GET /jobs
# ---------------------------------------------------------------------------


class TestListJobs:
    def test_returns_list(self, session):
        job = _make_job()
        q = MagicMock()
        q.order_by.return_value.limit.return_value.all.return_value = [job]
        session.query.return_value = q
        q.filter.return_value = q

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app)
            resp = c.get("/jobs")

        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_unknown_status_returns_400(self, session):
        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app, raise_server_exceptions=False)
            resp = c.get("/jobs?status=not_a_status")

        assert resp.status_code == 400

    def test_after_cursor_filters_by_created_at(self, session):
        """T4.2: ``after`` query param adds ``created_at < after`` to the query."""
        job = _make_job()
        q = MagicMock()
        q.order_by.return_value.limit.return_value.all.return_value = [job]
        session.query.return_value = q
        q.filter.return_value = q

        factory = MagicMock()
        app = _make_app(factory)

        cursor = "2026-04-01T10:00:00Z"
        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app)
            resp = c.get(f"/jobs?after={cursor}")

        assert resp.status_code == 200
        # The last filter() invocation carries the ``Job.created_at <
        # after`` clause; assert filter was called with at least one
        # extra clause beyond the parent_job_id default.
        assert q.filter.call_count >= 2

    def test_invalid_after_cursor_returns_422(self, session):
        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app, raise_server_exceptions=False)
            resp = c.get("/jobs?after=not-a-date")

        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /jobs/{id}
# ---------------------------------------------------------------------------


class TestGetJob:
    def test_returns_job(self, session):
        job = _make_job()
        session.get.return_value = job

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app)
            resp = c.get(f"/jobs/{job.id}")

        assert resp.status_code == 200
        assert resp.json()["id"] == str(job.id)

    def test_not_found_returns_404(self, session):
        session.get.return_value = None

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app, raise_server_exceptions=False)
            resp = c.get(f"/jobs/{uuid4()}")

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /jobs/{id}/events
# ---------------------------------------------------------------------------


class TestGetJobEvents:
    def test_returns_events(self, session):
        job = _make_job()
        session.get.return_value = job

        event = MagicMock()
        event.id = 1
        event.ts = MagicMock()
        event.ts.isoformat.return_value = "2024-01-01T00:00:00"
        event.level = "info"
        event.event = "job.created"
        event.message = None
        event.fields = {}

        q = MagicMock()
        q.filter.return_value.order_by.return_value.limit.return_value.all.return_value = [event]
        session.query.return_value = q

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app)
            resp = c.get(f"/jobs/{job.id}/events")

        assert resp.status_code == 200
        assert resp.json()[0]["event"] == "job.created"

    def test_not_found_returns_404(self, session):
        session.get.return_value = None

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app, raise_server_exceptions=False)
            resp = c.get(f"/jobs/{uuid4()}/events")

        assert resp.status_code == 404

    def test_after_cursor_filters_by_ts(self, session):
        """T4.2: ``after`` adds a ``ts < after`` clause for the newest-first stream."""
        job = _make_job()
        session.get.return_value = job

        q = MagicMock()
        q.filter.return_value = q
        q.order_by.return_value.limit.return_value.all.return_value = []
        session.query.return_value = q

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app)
            resp = c.get(f"/jobs/{job.id}/events?after=2026-04-01T10:00:00Z")

        assert resp.status_code == 200
        # Filter called twice: once for job_id == ..., once for ts < after.
        assert q.filter.call_count == 2


# ---------------------------------------------------------------------------
# POST /jobs/{id}/comments + GET /jobs/{id}/comments  (T3.10)
# ---------------------------------------------------------------------------


def _make_job_comment(comment_id=1, job_id=None, body="note", author=None):
    c = MagicMock()
    c.id = comment_id
    c.job_id = job_id or uuid4()
    c.author = author
    c.body = body
    c.created_at = MagicMock()
    c.created_at.isoformat.return_value = "2024-01-01T00:00:00"
    return c


class TestJobCommentsCreate:
    def test_create_comment_returns_201_and_payload(self, session):
        job = _make_job()
        session.get.return_value = job
        session.flush = MagicMock()

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app)
            resp = c.post(
                f"/jobs/{job.id}/comments",
                json={"body": "looks good", "author": "frapercan"},
            )

        assert resp.status_code == 201
        body = resp.json()
        assert body["body"] == "looks good"
        assert body["author"] == "frapercan"
        assert body["job_id"] == str(job.id)

    def test_missing_job_returns_404(self, session):
        session.get.return_value = None

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app, raise_server_exceptions=False)
            resp = c.post(f"/jobs/{uuid4()}/comments", json={"body": "x"})

        assert resp.status_code == 404

    def test_empty_body_returns_422(self, client):
        resp = client.post(f"/jobs/{uuid4()}/comments", json={"body": "  "})
        assert resp.status_code == 422


class TestJobCommentsList:
    def test_returns_comments_in_order(self, session):
        job = _make_job()
        session.get.return_value = job

        c1 = _make_job_comment(1, job.id, "first")
        c2 = _make_job_comment(2, job.id, "second", author="anpha")

        q = MagicMock()
        # Updated chain after T4.2 added ``.limit(...)`` between order_by
        # and all() so the cursor pagination path can cap each page.
        q.filter.return_value.order_by.return_value.limit.return_value.all.return_value = (
            [c1, c2]
        )
        session.query.return_value = q

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            cli = TestClient(app)
            resp = cli.get(f"/jobs/{job.id}/comments")

        assert resp.status_code == 200
        rows = resp.json()
        assert [r["body"] for r in rows] == ["first", "second"]
        assert rows[1]["author"] == "anpha"

    def test_missing_job_returns_404(self, session):
        session.get.return_value = None

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app, raise_server_exceptions=False)
            resp = c.get(f"/jobs/{uuid4()}/comments")

        assert resp.status_code == 404

    def test_after_cursor_filters_by_created_at(self, session):
        """T4.2: ``after`` adds ``created_at > after`` for the oldest-first stream."""
        job = _make_job()
        session.get.return_value = job

        q = MagicMock()
        q.filter.return_value = q
        q.order_by.return_value.limit.return_value.all.return_value = []
        session.query.return_value = q

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app)
            resp = c.get(f"/jobs/{job.id}/comments?after=2026-04-01T10:00:00Z")

        assert resp.status_code == 200
        # Filter called twice: once for job_id, once for created_at > after.
        assert q.filter.call_count == 2


# ---------------------------------------------------------------------------
# POST /jobs/{id}/cancel
# ---------------------------------------------------------------------------


class TestCancelJob:
    def test_cancels_queued_job(self, session):
        from protea.infrastructure.orm.models.job import JobStatus

        job = _make_job()
        job.status = JobStatus.QUEUED
        session.get.return_value = job

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app)
            resp = c.post(f"/jobs/{job.id}/cancel")

        assert resp.status_code == 200
        assert job.status == JobStatus.CANCELLED

    def test_cancel_succeeded_job_is_noop(self, session):
        from protea.infrastructure.orm.models.job import JobStatus

        job = _make_job()
        job.status = JobStatus.SUCCEEDED
        session.get.return_value = job

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app)
            resp = c.post(f"/jobs/{job.id}/cancel")

        assert resp.status_code == 200
        assert job.status == JobStatus.SUCCEEDED  # unchanged

    def test_cancel_not_found_returns_404(self, session):
        session.get.return_value = None

        factory = MagicMock()
        app = _make_app(factory)

        with patch(
            "protea.api.routers.jobs.session_scope", side_effect=lambda _: _mock_scope(session)
        ):
            c = TestClient(app, raise_server_exceptions=False)
            resp = c.post(f"/jobs/{uuid4()}/cancel")

        assert resp.status_code == 404
