import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.job import Job, JobEvent


@pytest.mark.integration
def test_jobs_postgres_roundtrip(postgres_url: str):
    engine = create_engine(postgres_url, future=True)

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    with Session(engine, future=True) as session:
        job = Job(operation="insert_proteins", queue_name="protea.insert_proteins")
        session.add(job)
        session.flush()

        job_id = job.id  # <- IMPORTANT: capture while still in-session

        ev = JobEvent(job_id=job_id, event="job.created", fields={"x": 1})
        session.add(ev)
        session.commit()

    with Session(engine, future=True) as session:
        j = session.get(Job, job_id)  # <- use the plain id
        assert j is not None
        assert len(j.events) == 1
        assert j.events[0].fields["x"] == 1
