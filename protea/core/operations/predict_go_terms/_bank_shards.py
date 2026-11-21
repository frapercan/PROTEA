"""A reference pool's vectors read from the bank's own shards on this node.

The bank of ADR-D62 is computed on the cluster and arrives as parquet shards, one
manifest per configuration naming each shard's sha256 (``load_embedding_bank``),
and the store records that manifest's sha256 in ``embedding_bank``. The database
holds the same vectors, loaded from those shards, and streams a pool at about
8.5 MB/s: 7 to 9 minutes for one bank of 1.5 M donors, once per configuration
and cut before the disk cache holds it (via 1, decision 6, 2026-11-21).

Here the database still says which donors make the pool, in which order and with
which sequence, by the same query as before with the sequence's md5 in place of
the vector. The vectors come from the shards beside the node, each shard checked
against the manifest the store recorded before a byte of it is used. A donor whose
sequence no shard holds is a refusal, never a gap: the store says the bank holds it.

Opt-in. ``PROTEA_EMBEDDING_BANK_DIR`` names the folder the builder wrote
(``bank-<config>.json`` and ``vectors/<config>/shard-*.parquet``). With it unset,
or without a manifest there that is the one the store recorded, the pool streams
from the database exactly as it did.
"""

from __future__ import annotations

import io
import os
import uuid
from collections import defaultdict
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core._artifact_identity import sha256_of_bytes
from protea.core.operations.load_embedding_bank import (
    EmbeddingBankRefused,
    read_bank_manifest,
    shard_path,
)
from protea.infrastructure.orm.models.embedding.embedding_bank import EmbeddingBank
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.sequence.sequence import Sequence

__all__ = ["BANK_DIR_ENV", "bank_dir", "fill_from_shards", "gather_from_shards"]

BANK_DIR_ENV = "PROTEA_EMBEDDING_BANK_DIR"

#: Shards read at once. Decoding a shard is bound by the parquet reader, about
#: 80 MB/s per thread on the development node, and releases the interpreter lock.
_READERS = 8


def bank_dir() -> Path | None:
    """The folder holding this node's bank shards, or None when the node names none."""
    value = os.environ.get(BANK_DIR_ENV, "").strip()
    return Path(value) if value else None


def recorded_manifest(session: Session, config_id: uuid.UUID) -> str | None:
    """The sha256 of the manifest the store loaded for ``config_id``, or None."""
    return session.execute(
        select(EmbeddingBank.manifest_sha256)
        .where(EmbeddingBank.embedding_config_id == config_id)
        .where(EmbeddingBank.status == "loaded")
        .order_by(EmbeddingBank.loaded_at.desc())
        .limit(1)
    ).scalar_one_or_none()


def gather_from_shards(
    folder: Path, config_id: uuid.UUID, manifest: Mapping[str, Any], md5s: Iterable[str]
) -> np.ndarray:
    """The vector of each md5, in order, from the manifest's shards, each shard verified first.

    Several donors may share one sequence, and each gets its vector. Refused when a
    shard does not hash as the manifest says, or when no shard holds one of the md5s.
    """
    order = list(md5s)
    dim = int(manifest["dim"])
    out = np.empty((len(order), dim), dtype=np.float16)
    positions: dict[str, list[int]] = defaultdict(list)
    for i, md5 in enumerate(order):
        positions[md5].append(i)
    filled = np.zeros(len(order), dtype=bool)

    def read(shard: Mapping[str, Any]) -> None:
        path = shard_path(folder, config_id, int(shard["shard"]))
        blob = path.read_bytes()
        got = sha256_of_bytes(blob)
        if got != shard["sha256"]:
            raise EmbeddingBankRefused(f"{path.name} hashes to {got}, not to {shard['sha256']}")
        table = pq.read_table(io.BytesIO(blob), columns=["md5", "vector"])
        names = table.column("md5").to_pylist()
        vectors = (
            table.column("vector")
            .combine_chunks()
            .flatten()
            .to_numpy(zero_copy_only=False)
            .reshape(len(names), dim)
        )
        for row, md5 in enumerate(names):
            where = positions.get(md5)
            if where is not None:
                out[where] = vectors[row]
                filled[where] = True

    with ThreadPoolExecutor(max_workers=_READERS) as pool:
        list(pool.map(read, manifest["shards"]))
    if not filled.all():
        missing = sorted({order[i] for i in np.flatnonzero(~filled)})
        raise EmbeddingBankRefused(
            f"{len(missing)} sequences of the pool are in no shard of the bank "
            f"(first {missing[:3]}), though the store holds their vectors"
        )
    return out


def fill_from_shards(
    session: Session, accession_q: Any, config_id: uuid.UUID
) -> tuple[list[str], np.ndarray] | None:
    """The pool of ``accession_q``, its vectors read from the shards, or None for the database route.

    None when the node names no bank folder, when the folder holds no manifest for
    the configuration, or when the manifest there is not the one the store
    recorded. The pool's donors and their order are the query's, unchanged.
    """
    folder = bank_dir()
    recorded = recorded_manifest(session, config_id) if folder is not None else None
    if folder is None or recorded is None:
        return None
    try:
        manifest = read_bank_manifest(folder, config_id, recorded)
    except EmbeddingBankRefused:
        return None
    rows = accession_q.add_columns(Sequence.sequence_hash).join(
        Sequence, Sequence.id == SequenceEmbedding.sequence_id
    )
    accessions: list[str] = []
    md5s: list[str] = []
    for accession, md5 in rows.yield_per(100_000):
        accessions.append(accession)
        md5s.append(md5)
    return accessions, gather_from_shards(folder, config_id, manifest, md5s)
