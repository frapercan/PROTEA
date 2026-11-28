"""Compute the label-free statistics of a configuration's embedding space at a cut.

The population is the photo's distinct member sequences with a vector under the
configuration, whatever the donor policy of a later search, so every breadth
level of a pool shares one geometry; its size ``n`` is recorded. The vectors are
read from the shards of the configuration's loaded bank on this node
(``PROTEA_EMBEDDING_BANK_DIR``, the folder the search reads), in two streaming
passes in float64 (:mod:`protea.core.embedding_space.statistics`).

It refuses before reading a shard unless the configuration and the photo exist,
the bank the search reads for the configuration (its latest loaded one) covers
the photo, this node names a bank folder, and the manifest there hashes to the
sha256 the store recorded for that bank. Each shard is checked against its
manifest's sha256 as it is read, and every member sequence must be in a shard
exactly once: a sequence the bank lacks is a refusal, never a gap.

The artifact goes to the store under
``embedding-space-statistics/<config>/<photo>/<sha256>.npz`` and one
``embedding_space_statistics`` row names it. A configuration and a photo have
one row: asking again is a no-op that returns it.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from pydantic import field_validator
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from protea.core._artifact_identity import sha256_of_bytes
from protea.core.code_revision import code_revision
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.embedding_space.statistics import (
    BankShard,
    Progress,
    compute_space_statistics,
    population_array,
    statistics_artifact,
)
from protea.core.operations.load_embedding_bank import (
    EmbeddingBankRefused,
    read_bank_manifest,
    shard_path,
)
from protea.core.operations.predict_go_terms._bank_shards import BANK_DIR_ENV, bank_dir
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto, CutPhotoMember
from protea.infrastructure.orm.models.embedding.embedding_bank import EmbeddingBank
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.embedding_space_statistics import (
    EmbeddingSpaceStatistics,
)
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

__all__ = [
    "ComputeEmbeddingSpaceStatisticsOperation",
    "ComputeEmbeddingSpaceStatisticsPayload",
    "EmbeddingSpaceStatisticsRefused",
    "statistics_key_for",
]

_EVENT = "compute_embedding_space_statistics"


class EmbeddingSpaceStatisticsRefused(ValueError):
    """Statistics the operation will not compute, with the reason."""


class ComputeEmbeddingSpaceStatisticsPayload(ProteaPayload, frozen=True):
    """The configuration whose space is measured, and the photo whose members are its population."""

    embedding_config_id: str
    cut_photo_id: str

    @field_validator("embedding_config_id", "cut_photo_id", mode="before")
    @classmethod
    def must_be_a_uuid(cls, v: Any) -> str:
        return str(uuid.UUID(str(v)))

    @property
    def config_id(self) -> uuid.UUID:
        return uuid.UUID(self.embedding_config_id)

    @property
    def photo_id(self) -> uuid.UUID:
        return uuid.UUID(self.cut_photo_id)


@dataclass(frozen=True)
class _Inputs:
    """What one run reads, resolved and checked before a shard is opened."""

    config_id: uuid.UUID
    model_name: str
    photo_id: uuid.UUID
    cut_release: int
    bank_id: uuid.UUID
    bank_short_name: str
    bank_manifest_sha256: str
    dimension: int
    shards: tuple[BankShard, ...]
    population: np.ndarray
    members: dict[str, int]


def statistics_key_for(config_id: uuid.UUID, photo_id: uuid.UUID, sha256: str) -> str:
    """Where an artifact of statistics lives in the store: the one spelling."""
    return f"embedding-space-statistics/{config_id}/{photo_id}/{sha256}.npz"


def store_statistics(key: str, blob: bytes) -> str:
    """Put the artifact's bytes in the store; return its URI."""
    project_root = Path(__file__).resolve().parents[3]
    store = get_artifact_store(load_settings(project_root))
    return str(store.put(key, blob))


def searched_bank(session: Session, config_id: uuid.UUID) -> EmbeddingBank | None:
    """The bank the search reads for a configuration: its latest loaded one."""
    return session.execute(
        select(EmbeddingBank)
        .where(EmbeddingBank.embedding_config_id == config_id)
        .where(EmbeddingBank.status == "loaded")
        .order_by(EmbeddingBank.loaded_at.desc())
        .limit(1)
    ).scalar_one_or_none()


def photo_population(session: Session, photo_id: uuid.UUID) -> tuple[np.ndarray, dict[str, int]]:
    """The photo's distinct member md5s, sorted, and how many members it has with and without one."""
    members, with_sequence = session.execute(
        select(func.count(), func.count(CutPhotoMember.sequence_md5)).where(
            CutPhotoMember.photo_id == photo_id
        )
    ).one()
    md5s = session.execute(
        select(CutPhotoMember.sequence_md5)
        .where(CutPhotoMember.photo_id == photo_id)
        .where(CutPhotoMember.sequence_md5.is_not(None))
        .distinct()
    ).scalars()
    rows = [str(md5) for md5 in md5s]
    if len(rows) < 2:
        raise EmbeddingSpaceStatisticsRefused(
            f"the photo has {len(rows)} distinct member sequences: a covariance needs two"
        )
    population = population_array(rows)
    return population, {
        "members": int(members),
        "members_with_sequence": int(with_sequence),
        "distinct_sequences": len(population),
    }


def _bank_shards(bank: EmbeddingBank, config_id: uuid.UUID) -> tuple[int, tuple[BankShard, ...]]:
    """The dimension and the shards of the bank's manifest on this node, checked against the store."""
    folder = bank_dir()
    if folder is None:
        raise EmbeddingSpaceStatisticsRefused(
            f"{BANK_DIR_ENV} names no folder on this node: the statistics read the bank's shards"
        )
    try:
        manifest = read_bank_manifest(folder, config_id, bank.manifest_sha256)
    except EmbeddingBankRefused as exc:
        raise EmbeddingSpaceStatisticsRefused(
            f"the bank's manifest in {folder} is not the one the store loaded: {exc}"
        ) from exc
    shards = tuple(
        BankShard(shard_path(folder, config_id, int(s["shard"])), str(s["sha256"]), int(s["rows"]))
        for s in manifest["shards"]
    )
    return int(manifest["dim"]), shards


def resolve_inputs(session: Session, p: ComputeEmbeddingSpaceStatisticsPayload) -> _Inputs:
    """Every input of a run, refused before a shard is read when one is missing or wrong."""
    config = session.get(EmbeddingConfig, p.config_id)
    if config is None:
        raise EmbeddingSpaceStatisticsRefused(f"no embedding configuration {p.config_id}")
    photo = session.get(CutPhoto, p.photo_id)
    if photo is None:
        raise EmbeddingSpaceStatisticsRefused(f"no cut photo {p.photo_id}")
    bank = searched_bank(session, config.id)
    if bank is None:
        raise EmbeddingSpaceStatisticsRefused(f"configuration {config.id} has no loaded bank")
    if photo.manifest_sha256 not in {named["manifest_sha256"] for named in bank.photos}:
        raise EmbeddingSpaceStatisticsRefused(
            f"bank {bank.short_name}, the one the search reads, does not cover the photo of cut "
            f"{photo.cut_release}"
        )
    dimension, shards = _bank_shards(bank, config.id)
    population, members = photo_population(session, photo.id)
    return _Inputs(
        config_id=config.id,
        model_name=config.model_name,
        photo_id=photo.id,
        cut_release=photo.cut_release,
        bank_id=bank.id,
        bank_short_name=bank.short_name,
        bank_manifest_sha256=bank.manifest_sha256,
        dimension=dimension,
        shards=shards,
        population=population,
        members=members,
    )


def describe(inputs: _Inputs, document: dict[str, Any]) -> str:
    """The row's description, derived from what was measured, never typed."""
    spectrum = document["spectrum"]
    return (
        f"Label-free statistics of the space of {inputs.model_name} ({document['d']} "
        f"dimensions) at cut {inputs.cut_release}: the {document['n']:,} distinct member "
        f"sequences of the photo, read from bank {inputs.bank_short_name} in two float64 "
        f"passes. Ledoit-Wolf shrinkage {spectrum['shrinkage']:.3g}; "
        f"{spectrum['removed_directions']} directions removed by the profile-likelihood "
        f"elbow ({spectrum['removed_directions_share']:.1%} of the spectrum; broken stick "
        f"{spectrum['broken_stick_count']}, participation ratio "
        f"{spectrum['participation_ratio']:.1f}); fixed count {spectrum['fixed_count']} "
        f"({spectrum['fixed_count_share']:.1%}); {spectrum['n_below_shrinkage']} eigenvalues "
        f"below the shrinkage floor and {spectrum['n_below_storage']} below the float16 "
        f"rounding variance {spectrum['nu16']:.3g}."
    )


def _record(inputs: _Inputs) -> dict[str, Any]:
    """What the artifact's document says it measured."""
    return {
        "embedding_config_id": str(inputs.config_id),
        "cut_photo_id": str(inputs.photo_id),
        "cut_release": inputs.cut_release,
        "embedding_bank_id": str(inputs.bank_id),
        "bank_manifest_sha256": inputs.bank_manifest_sha256,
        "population": inputs.members,
    }


def _progress_events(emit: EmitFn) -> Progress:
    """An event at each tenth of each pass, not one per shard."""
    reported: dict[int, int] = {}

    def report(pass_number: int, done: int, total: int) -> None:
        tenth = done * 10 // max(total, 1)
        if reported.get(pass_number, -1) < tenth:
            reported[pass_number] = tenth
            fields = {"pass": pass_number, "shards_done": done, "shards": total}
            emit(f"{_EVENT}.pass", None, fields, "info")

    return report


def _summary(row: EmbeddingSpaceStatistics, document: dict[str, Any]) -> dict[str, Any]:
    spectrum = document["spectrum"]
    keys = (
        "shrinkage",
        "nu16",
        "n_below_shrinkage",
        "n_below_storage",
        "removed_directions",
        "broken_stick_count",
        "participation_ratio",
        "fixed_count",
        "fixed_count_share",
    )
    return {
        "embedding_space_statistics_id": str(row.id),
        "n": row.n,
        "d": row.d,
        "artifact_uri": row.artifact_uri,
        "artifact_sha256": row.artifact_sha256,
        **{key: spectrum[key] for key in keys},
    }


class ComputeEmbeddingSpaceStatisticsOperation:
    """Measure one configuration's space over one photo's member sequences, without labels."""

    name = "compute_embedding_space_statistics"
    description = (
        "Measure the space of an embedding configuration at a cut without labels: two "
        "streaming float64 passes over the loaded bank's shards for the photo's distinct "
        "member sequences give the mean, the covariance, Ledoit-Wolf shrinkage, the "
        "eigendecomposition, the scree elbow's removed directions, the float16 rounding "
        "variance and the diagnostics of every search-space transform."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        config = str(payload.get("embedding_config_id", ""))[:8]
        photo = str(payload.get("cut_photo_id", ""))[:8]
        return f"space of {config} at photo {photo}" if config and photo else ""

    def _write(
        self, session: Session, inputs: _Inputs, artifact: tuple[bytes, dict[str, Any]], job: Any
    ) -> EmbeddingSpaceStatistics:
        blob, document = artifact
        digest = sha256_of_bytes(blob)
        uri = store_statistics(statistics_key_for(inputs.config_id, inputs.photo_id, digest), blob)
        row = EmbeddingSpaceStatistics(
            embedding_config_id=inputs.config_id,
            cut_photo_id=inputs.photo_id,
            embedding_bank_id=inputs.bank_id,
            n=int(document["n"]),
            d=int(document["d"]),
            artifact_uri=uri,
            artifact_sha256=digest,
            description=describe(inputs, document),
            job_id=job,
            code_revision=code_revision(),
        )
        session.add(row)
        session.flush()
        return row

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ComputeEmbeddingSpaceStatisticsPayload.model_validate(contract_payload(payload))
        existing = session.execute(
            select(EmbeddingSpaceStatistics).where(
                EmbeddingSpaceStatistics.embedding_config_id == p.config_id,
                EmbeddingSpaceStatistics.cut_photo_id == p.photo_id,
            )
        ).scalar_one_or_none()
        if existing is not None:
            found = {"embedding_space_statistics_id": str(existing.id), "n": existing.n}
            emit(f"{_EVENT}.already_computed", None, found, "info")
            return OperationResult(result={**found, "skipped": True})
        inputs = resolve_inputs(session, p)
        emit(f"{_EVENT}.population", None, {**inputs.members, "shards": len(inputs.shards)}, "info")
        statistics = compute_space_statistics(
            inputs.shards,
            inputs.population,
            inputs.dimension,
            progress=_progress_events(emit),
        )
        artifact = statistics_artifact(statistics, _record(inputs))
        row = self._write(session, inputs, artifact, job_id_from_payload(payload))
        result = _summary(row, artifact[1])
        emit(f"{_EVENT}.done", None, result, "info")
        return OperationResult(result=result)
