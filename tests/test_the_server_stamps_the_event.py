"""A shared history is stamped by the server, not by whoever writes to it.

The test that matters here is not that a server default is declared. That
is what a reviewer sees and it is not what fails: SQLAlchemy resolves a
Python ``default=`` at flush and puts the column in the INSERT, so a
``server_default`` sitting beside one never fires, and the pair applies
cleanly while changing nothing.

So these move a process's clock and check what lands. A row written by a
process that believes it is two hours in the future must still carry the
server's time. That is the property, and it is the only one that would
have caught 2026-08-29, when a node booted two hours ahead for twenty
seconds and its three events made two readers reconstruct a stall that
never happened.
"""

from __future__ import annotations

import datetime as real_datetime
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.job import Job, JobEvent

_SKEW = real_datetime.timedelta(hours=2)


class TestTheColumnDoesNotCarryAPythonDefault:
    """Cheap and database-free, and it is the trap, not the property."""

    @pytest.mark.parametrize(
        ("model", "column"),
        [(JobEvent, "ts"), (Job, "created_at")],
    )
    def test_no_python_default_survives_beside_the_server_one(
        self, model: type, column: str
    ) -> None:
        col = model.__table__.c[column]
        assert col.server_default is not None, f"{column} has no server default"
        assert col.default is None, (
            f"{column} still has a Python default. SQLAlchemy resolves it at "
            f"flush and includes the column in the INSERT, so the server "
            f"default never fires and the change is inert."
        )


@pytest.mark.integration
class TestAWrongClockCannotWriteTheWrongTime:
    def test_an_event_from_a_future_process_lands_at_the_server_s_time(
        self, postgres_url: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The whole point. Two hours of client skew must not reach the row."""
        engine = create_engine(postgres_url, future=True)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

        # Every stamp this process could produce is now two hours ahead.
        import protea.core.utils as utils

        monkeypatch.setattr(utils, "utcnow", lambda: real_datetime.datetime.now(
            real_datetime.UTC) + _SKEW)

        with Session(engine, future=True) as session:
            server_before = session.execute(text("SELECT now()")).scalar_one()
            job = Job(
                id=uuid.uuid4(), operation="ping", queue_name="protea.ping",
                payload={}, meta={},
            )
            session.add(job)
            session.flush()
            session.add(JobEvent(job_id=job.id, event="probe", level="info"))
            session.flush()
            server_after = session.execute(text("SELECT now()")).scalar_one()

            stamped = session.execute(
                text("SELECT ts FROM job_event WHERE job_id = :j"), {"j": job.id}
            ).scalar_one()
            created = session.execute(
                text("SELECT created_at FROM job WHERE id = :j"), {"j": job.id}
            ).scalar_one()

        for name, got in (("job_event.ts", stamped), ("job.created_at", created)):
            assert server_before <= got <= server_after, (
                f"{name} landed at {got}, outside the server's own window "
                f"[{server_before}, {server_after}]. The client's clock reached "
                f"the row."
            )
            assert got < server_after + _SKEW / 2, (
                f"{name} carries the two-hour skew this test injected."
            )
