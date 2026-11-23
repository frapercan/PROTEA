"""Each result of a window names the job that computed it and the code that job ran.

The window page printed a result's id and nothing to follow from it: the job that
computed its numbers and the revision of the code that job ran were one query
away and on no screen. The list of a window's results now carries the revision
each job recorded when it started (``job.started``), the latest start of a job
that was retried, and the page links the job and the commit.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from protea.infrastructure.orm.base import Base
from protea.services._annotations_evaluation_helpers import (
    evaluation_result_to_dict,
    list_evaluation_results_data,
)


def test_a_result_serialised_alone_names_no_revision() -> None:
    class _Row:
        id = uuid.uuid4()
        evaluation_set_id = uuid.uuid4()
        prediction_set_id = uuid.uuid4()
        scoring_config_id = None
        reranker_model_id = None
        reranker_config = None
        job_id = None
        created_at = datetime(2026, 11, 21, tzinfo=UTC)
        frame = frame_digest = temporal_window = arms_enabled = leakage_role = None
        results: dict = {}

    assert evaluation_result_to_dict(_Row())["code_revision"] is None  # type: ignore[arg-type]


@pytest.mark.integration
class TestTheListNamesTheRevision:
    @pytest.fixture()
    def window(self, postgres_url: str):
        engine = create_engine(postgres_url, future=True)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        window, retried, silent = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        started = datetime(2026, 11, 21, 3, 0, tzinfo=UTC)
        with Session(engine, future=True) as session:
            session.execute(text("SET session_replication_role = 'replica'"))
            ids = {"o": uuid.uuid4(), "n": uuid.uuid4(), "s": uuid.uuid4()}
            session.execute(
                text(
                    "INSERT INTO evaluation_set (id, old_annotation_set_id, new_annotation_set_id, "
                    "pivot_snapshot_id, old_native_snapshot_id, new_native_snapshot_id, stats) "
                    "VALUES (:w, :o, :n, :s, :s, :s, '{}'::jsonb)"
                ),
                {"w": window, **ids},
            )
            for job in (retried, silent):
                session.execute(
                    text(
                        "INSERT INTO job (id, operation, queue_name, status, payload, meta, tags) "
                        "VALUES (:j, 'run_cafa_evaluation', 'protea.evaluations', 'SUCCEEDED', "
                        "'{}'::jsonb, '{}'::jsonb, '{}')"
                    ),
                    {"j": job},
                )
            for minutes, revision in ((0, "a" * 40), (5, "b" * 40)):
                session.execute(
                    text(
                        "INSERT INTO job_event (job_id, ts, level, event, fields) "
                        "VALUES (:j, :ts, 'info', 'job.started', CAST(:f AS jsonb))"
                    ),
                    {
                        "j": retried,
                        "ts": started + timedelta(minutes=minutes),
                        "f": json.dumps({"code_revision": revision}),
                    },
                )
            for job, offset in ((retried, 2), (silent, 1), (None, 0)):
                session.execute(
                    text(
                        "INSERT INTO evaluation_result (id, evaluation_set_id, prediction_set_id, "
                        "job_id, results, created_at) VALUES (:r, :w, :p, :j, '{}'::jsonb, :c)"
                    ),
                    {
                        "r": uuid.uuid4(),
                        "w": window,
                        "p": uuid.uuid4(),
                        "j": job,
                        "c": started + timedelta(hours=offset),
                    },
                )
            session.commit()
        yield engine, window, retried, silent
        Base.metadata.drop_all(engine)
        engine.dispose()

    def test_each_result_carries_its_jobs_latest_start(self, window) -> None:
        engine, window_id, retried, silent = window
        with Session(engine, future=True) as session:
            rows = list_evaluation_results_data(session, window_id)
        by_job = {r["job_id"]: r["code_revision"] for r in rows}
        assert by_job == {str(retried): "b" * 40, str(silent): None, None: None}
