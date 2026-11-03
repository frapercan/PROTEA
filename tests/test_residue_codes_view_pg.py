"""The residue code summaries against real rows (ADR-D51).

The companion file pins the route and the arithmetic; this one pins the counts,
which are SQL: a GROUP BY over ``residue_code`` with a filtered count, a distinct
count over the source corpus, and a JSONB path into the result the worker writes
on a coordinator's ``job.succeeded`` (or ``job.dispatched``) event. Each of those
can be silently wrong on a mock and only Postgres says so.

The scene: a source corpus of three sequences; a residue vocabulary whose codes
reach two of them, one written in the last hour and one before it, in two shards;
and two coordinators that wrote under that vocabulary, of which the page must
show the newer.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.residue_code import ResidueCode
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.job import Job, JobEvent, JobStatus
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.services.residue_codes_service import residue_code_summaries

NOW = datetime(2026, 11, 3, 9, 0, tzinfo=UTC)


def _fresh(postgres_url: str) -> Any:
    from protea.infrastructure.orm.base import Base

    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    return engine


def _sequences(session: Session, n: int) -> list[Sequence]:
    rows = [Sequence(sequence="MKV" * (i + 1), sequence_hash=f"{i:032x}") for i in range(n)]
    session.add_all(rows)
    session.flush()
    return rows


def _coordinator(
    session: Session, target: uuid.UUID, created: datetime, status: JobStatus
) -> uuid.UUID:
    job = Job(
        id=uuid.uuid4(),
        operation="encode_residue_vocabulary",
        queue_name="protea.embeddings",
        status=status,
        payload={},
        meta={},
        progress_total=2,
        created_at=created,
    )
    session.add(job)
    session.flush()
    event = "job.succeeded" if status == JobStatus.SUCCEEDED else "job.dispatched"
    session.add(
        JobEvent(
            job_id=job.id,
            event=event,
            fields={"result": {"embedding_config_id": str(target), "batches": 2}},
            level="info",
        )
    )
    session.flush()
    return job.id


def _scene(session: Session) -> dict[str, Any]:
    seqs = _sequences(session, 3)
    source = EmbeddingConfig(
        model_name="ElnaggarLab/ankh-base", model_backend="ankh", layer_indices=[0]
    )
    session.add(source)
    session.flush()
    for s in seqs:
        session.add(
            SequenceEmbedding(
                sequence_id=s.id,
                embedding_config_id=source.id,
                embedding=[0.0, 1.0],
                embedding_dim=2,
            )
        )
    target = EmbeddingConfig(
        model_name="residue-vocabulary:0123456789abcdef:f3a7fe16",
        model_backend="residue-vocabulary",
        layer_indices=[2, 10, 22, 40, 48],
        layer_agg="concat",
        pooling="per-residue",
        normalize=False,
        kind="learned",
        derived_from_embedding_config_id=source.id,
    )
    session.add(target)
    session.flush()
    session.add_all(
        [
            ResidueCode(
                sequence_id=seqs[0].id,
                embedding_config_id=target.id,
                shard_key="residue-codes/t/0-127-a.bin",
                shard_sha256="a" * 64,
                position=0,
                residue_count=300,
                entry_count=36_000,
                created_at=NOW - timedelta(hours=2),
            ),
            ResidueCode(
                sequence_id=seqs[1].id,
                embedding_config_id=target.id,
                shard_key="residue-codes/t/128-255-b.bin",
                shard_sha256="b" * 64,
                position=0,
                residue_count=120,
                entry_count=14_000,
                created_at=NOW - timedelta(minutes=10),
            ),
        ]
    )
    older = _coordinator(session, target.id, NOW - timedelta(days=1), JobStatus.SUCCEEDED)
    newer = _coordinator(session, target.id, NOW - timedelta(hours=3), JobStatus.RUNNING)
    session.flush()
    return {"source": source.id, "target": target.id, "older": older, "newer": newer}


def test_the_summary_counts_the_index_against_the_source_corpus(postgres_url: str) -> None:
    engine = _fresh(postgres_url)
    with Session(engine) as session:
        scene = _scene(session)
        summaries = residue_code_summaries(session, now=NOW)

    assert [s["embedding_config_id"] for s in summaries] == [str(scene["target"])], (
        "only residue-vocabulary configurations are summarised; the source corpus is not one"
    )
    (summary,) = summaries
    index = summary["index"]
    assert index["sequences"] == 2
    assert index["residues"] == 420
    assert index["entries"] == 50_000
    assert index["shards"] == 2
    assert index["sequences_last_hour"] == 1, (
        "the row written two hours ago is outside the pace window"
    )
    assert summary["source"]["embedding_config_id"] == str(scene["source"])
    assert summary["source"]["sequences"] == 3
    assert summary["coverage"] == 2 / 3
    assert summary["remaining"] == 1
    assert summary["eta_hours"] == 1.0
    assert summary["layer_indices"] == [2, 10, 22, 40, 48]


def test_the_page_shows_the_newest_coordinator_that_wrote_the_codes(postgres_url: str) -> None:
    """The newer one only dispatched (a coordinator that waits for its batches); it still names its target."""
    engine = _fresh(postgres_url)
    with Session(engine) as session:
        scene = _scene(session)
        (summary,) = residue_code_summaries(session, now=NOW)

    assert summary["job"]["id"] == str(scene["newer"])
    assert summary["job"]["status"] == "RUNNING"
    assert summary["job"]["progress_total"] == 2


def test_no_residue_vocabulary_gives_an_empty_list(postgres_url: str) -> None:
    engine = _fresh(postgres_url)
    with Session(engine) as session:
        session.add(
            EmbeddingConfig(
                model_name="facebook/esm2_t6_8M_UR50D", model_backend="esm", layer_indices=[0]
            )
        )
        session.flush()
        assert residue_code_summaries(session, now=NOW) == []
