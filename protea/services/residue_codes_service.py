"""How far each residue vocabulary's codes reach, for the embeddings page (ADR-D51).

A residue vocabulary's codes live as shards in the artifact store, indexed by
``residue_code``; the configuration they are indexed under carries
``model_backend == "residue-vocabulary"`` and points at the corpus it was
computed from through ``derived_from_embedding_config_id``.

This module answers, per such configuration: how many sequences, residues and
entries are indexed, in how many shards, when the first and the last rows
landed, how many landed in the last hour, how large the source corpus is, what
is left of it and, at the last hour's pace, how long that takes, and which job
wrote them. It reads and never writes, and every number it returns is a count
over rows: none is estimated from a sample.

Why it exists: an ``encode_residue_vocabulary`` pass runs for hours on a GPU
node, and its coordinator's state says how the batches were dispatched, not how
much of the corpus has a code. The index is the only place that knows.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import distinct, func, select
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.residue_code import ResidueCode
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.job import Job, JobEvent

#: The backend of the configurations residue codes are indexed under.
RESIDUE_BACKEND = "residue-vocabulary"

#: The operation that writes them.
ENCODE_OPERATION = "encode_residue_vocabulary"

#: The events on which the worker records a coordinator's result: ``job.dispatched``
#: for a coordinator that waits for its batches, ``job.succeeded`` for one that closed
#: when it published them. Both carry ``result.embedding_config_id``.
RESULT_EVENTS = ("job.dispatched", "job.succeeded")

#: The window the pace is read over.
PACE_WINDOW = timedelta(hours=1)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def index_counts(session: Session, since: datetime) -> dict[uuid.UUID, dict[str, Any]]:
    """Per configuration: rows, residues, entries, shards, first and last write, rows since ``since``.

    One row of ``residue_code`` is one sequence under one configuration, so the row
    count is the number of sequences that have a code. Shards are counted as the rows
    at position 0, one per shard, because ``count(DISTINCT shard_key)`` forces a
    sorted aggregate over every row: measured on the live index at 168,912 rows, 168 ms
    against 20 ms for the parallel hash aggregate this keeps.
    """
    rows = session.execute(
        select(
            ResidueCode.embedding_config_id,
            func.count(ResidueCode.id),
            func.coalesce(func.sum(ResidueCode.residue_count), 0),
            func.coalesce(func.sum(ResidueCode.entry_count), 0),
            func.count(ResidueCode.id).filter(ResidueCode.position == 0),
            func.min(ResidueCode.created_at),
            func.max(ResidueCode.created_at),
            func.count(ResidueCode.id).filter(ResidueCode.created_at >= since),
        ).group_by(ResidueCode.embedding_config_id)
    ).all()
    return {
        config_id: {
            "sequences": int(sequences),
            "residues": int(residues),
            "entries": int(entries),
            "shards": int(shards),
            "first_written_at": _iso(first),
            "last_written_at": _iso(last),
            "sequences_last_hour": int(recent),
        }
        for config_id, sequences, residues, entries, shards, first, last, recent in rows
    }


def source_sizes(session: Session, source_ids: set[uuid.UUID]) -> dict[uuid.UUID, int]:
    """Distinct sequences with an embedding under each source configuration.

    Distinct, because a chunked configuration stores several rows per sequence, and
    the pass codes sequences, not rows.
    """
    if not source_ids:
        return {}
    rows = session.execute(
        select(
            SequenceEmbedding.embedding_config_id,
            func.count(distinct(SequenceEmbedding.sequence_id)),
        )
        .where(SequenceEmbedding.embedding_config_id.in_(source_ids))
        .group_by(SequenceEmbedding.embedding_config_id)
    ).all()
    return {config_id: int(n) for config_id, n in rows}


def writing_jobs(session: Session, config_ids: list[uuid.UUID]) -> dict[uuid.UUID, dict[str, Any]]:
    """The latest ``encode_residue_vocabulary`` job whose result names each configuration."""
    if not config_ids:
        return {}
    target = JobEvent.fields["result"]["embedding_config_id"].astext
    rows = session.execute(
        select(
            target,
            Job.id,
            Job.status,
            Job.progress_current,
            Job.progress_total,
            Job.created_at,
            Job.finished_at,
        )
        .join(Job, Job.id == JobEvent.job_id)
        .where(
            Job.operation == ENCODE_OPERATION,
            JobEvent.event.in_(RESULT_EVENTS),
            target.in_([str(c) for c in config_ids]),
        )
        .order_by(Job.created_at.desc())
    ).all()
    jobs: dict[uuid.UUID, dict[str, Any]] = {}
    for config_id, job_id, status, current, total, created, finished in rows:
        jobs.setdefault(
            uuid.UUID(config_id),
            {
                "id": str(job_id),
                "status": getattr(status, "value", status),
                "progress_current": current,
                "progress_total": total,
                "created_at": _iso(created),
                "finished_at": _iso(finished),
            },
        )
    return jobs


def _reach(index: dict[str, Any], size: int | None) -> dict[str, Any]:
    """Coverage, what is left and, at the last hour's pace, the hours it takes."""
    if not size:
        return {"coverage": None, "remaining": None, "eta_hours": None}
    remaining = max(size - index["sequences"], 0)
    pace = index["sequences_last_hour"]
    return {
        "coverage": index["sequences"] / size,
        "remaining": remaining,
        "eta_hours": (remaining / pace) if pace and remaining else None,
    }


_EMPTY_INDEX: dict[str, Any] = {
    "sequences": 0,
    "residues": 0,
    "entries": 0,
    "shards": 0,
    "first_written_at": None,
    "last_written_at": None,
    "sequences_last_hour": 0,
}


def _summary(
    config: EmbeddingConfig,
    index: dict[str, Any] | None,
    sources: dict[uuid.UUID, EmbeddingConfig],
    sizes: dict[uuid.UUID, int],
    job: dict[str, Any] | None,
) -> dict[str, Any]:
    index = index or dict(_EMPTY_INDEX)
    source_id = config.derived_from_embedding_config_id
    source = sources.get(source_id) if source_id else None
    size = sizes.get(source_id) if source_id else None
    return {
        "embedding_config_id": str(config.id),
        "name": config.display_name or config.model_name,
        "description": config.description,
        "layer_indices": config.layer_indices,
        "created_at": _iso(config.created_at),
        "source": (
            {
                "embedding_config_id": str(source_id),
                "name": (source.display_name or source.model_name) if source else None,
                "sequences": size,
            }
            if source_id
            else None
        ),
        "index": index,
        **_reach(index, size),
        "job": job,
    }


def residue_configs(session: Session) -> list[EmbeddingConfig]:
    """The residue-vocabulary configurations, newest first."""
    return list(
        session.scalars(
            select(EmbeddingConfig)
            .where(EmbeddingConfig.model_backend == RESIDUE_BACKEND)
            .order_by(EmbeddingConfig.created_at.desc())
        )
    )


def source_ids_of(configs: list[EmbeddingConfig]) -> set[uuid.UUID]:
    return {
        c.derived_from_embedding_config_id
        for c in configs
        if c.derived_from_embedding_config_id is not None
    }


def residue_source_sizes(session: Session) -> dict[uuid.UUID, int]:
    """The source corpus size of every residue-vocabulary configuration.

    Separate because it is most of the cost (an index-only scan over the whole bank,
    752 ms of about 920 measured live) and a constant: a pass reads a frozen bank, so
    the caller can cache it far longer than the counts that move.
    """
    return source_sizes(session, source_ids_of(residue_configs(session)))


def residue_code_summaries(
    session: Session,
    now: datetime | None = None,
    sizes: dict[uuid.UUID, int] | None = None,
) -> list[dict[str, Any]]:
    """One summary per residue vocabulary configuration, newest configuration first.

    ``sizes`` is the source corpus size per source configuration; when it is not
    given it is counted here.
    """
    configs = residue_configs(session)
    if not configs:
        return []
    now = now or datetime.now(UTC)
    source_ids = source_ids_of(configs)
    sources = {
        s.id: s
        for s in session.scalars(select(EmbeddingConfig).where(EmbeddingConfig.id.in_(source_ids)))
    }
    counts = index_counts(session, now - PACE_WINDOW)
    if sizes is None:
        sizes = source_sizes(session, source_ids)
    jobs = writing_jobs(session, [c.id for c in configs])
    return [_summary(c, counts.get(c.id), sources, sizes, jobs.get(c.id)) for c in configs]
