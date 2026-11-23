"""Where each sequence's per-residue codes live, NAMED and never copied.

NOT ONE BYTE OF CODE IS COPIED. This is a query over ``residue_code``: the
shard key, that shard's sha256 and the position of the sequence inside it. The
90,4 GB of shards stay where they are, and the reader fetches the shard it
needs and verifies it before believing it.

IT COVERS THE WHOLE BANK AND NOT A BUNDLE'S POPULATION, which is what lets it
be written before the windows are chosen: a superset covers any selection, and
the loader reads only the ``sequence_id`` values of its own batch. Per-window
coverage (``sequences_without_residue_codes``) is still computed when a bundle
is exported, against its real population, because that number is about the
bundle and not about the bank.

THE CONFIG THAT KEYS THESE ROWS IS NOT THE ONE THEY ARE NAMED AFTER, and that
is the trap this module exists to make loud. Measured 2026-11-08: all 683.887
rows carry ``43193409-de70-526b-ae8d-43892a744049``, whose model name is
``residue-vocabulary:06fed75ca76d17be:f3a7fe16``. The ankh config
``f3a7fe16-eaf5-5083-b459-7c5fa7a9a9e0`` that the vocabulary was DERIVED from
has **zero** rows. So a reader filtering by "the f3a7fe16 bank" finds nothing,
and an empty ``residue_refs`` looks exactly like a bank with no per-residue
codes. The refusal below is what makes a wrong id fail loudly instead.

Shape, measured on the same day: 683.887 rows over 5.344 shards, one distinct
sha per shard, positions 0 to 127.
"""

from __future__ import annotations

import uuid
from typing import NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.residue_code import ResidueCode

#: The five columns the laboratory's loader reads.
SCHEMA = pa.schema(
    [
        ("embedding_config_id", pa.string()),
        ("sequence_id", pa.int64()),
        ("shard_key", pa.string()),
        ("shard_sha256", pa.string()),
        ("position", pa.int32()),
    ]
)


class Refs(NamedTuple):
    """The bank's references, with the counts that make a wrong id visible."""

    embedding_config_id: str
    model_name: str
    rows: list[tuple[int, str, str, int]]
    shards: int


def refs_of(session: Session, embedding_config_id: uuid.UUID) -> Refs:
    """Every per-residue reference of one embedding config, or a refusal.

    AN EMPTY ANSWER IS REFUSED, because it is indistinguishable from a bank
    with no codes at all. The ankh config that the vocabulary was derived from
    has zero rows while the vocabulary config has 683.887, so naming the wrong
    one is a plausible mistake that would otherwise publish a table that joins
    to nothing and says so to nobody.
    """
    config = session.get(EmbeddingConfig, embedding_config_id)
    if config is None:
        raise ValueError(f"EmbeddingConfig {embedding_config_id} does not exist")
    filas = session.execute(
        select(
            ResidueCode.sequence_id,
            ResidueCode.shard_key,
            ResidueCode.shard_sha256,
            ResidueCode.position,
        ).where(ResidueCode.embedding_config_id == embedding_config_id)
    ).all()
    if not filas:
        raise ValueError(
            f"EmbeddingConfig {embedding_config_id} ({config.model_name!r}) has no "
            "residue_code rows, so this table would be empty and an empty "
            "residue_refs is indistinguishable from a bank with no per-residue codes. "
            "The rows are keyed by the VOCABULARY config, not by the model config the "
            "vocabulary was derived from"
        )
    return Refs(
        embedding_config_id=str(embedding_config_id),
        model_name=config.model_name,
        rows=[(int(s), k, h, int(p)) for s, k, h, p in filas],
        shards=len({k for _, k, _, _ in filas}),
    )


def write_table(refs: Refs, path: str) -> int:
    """Write the five columns sorted by ``sequence_id``, and say how many rows.

    Sorted for the reason its siblings are: the sha of this file is what a
    consumer quotes, and a digest over whatever order the query plan produced
    moves without the data moving. That is a measured hazard here, not a worry:
    this project's reproducibility floor lives in retrieval order.
    """
    ordenadas = sorted(refs.rows)
    pq.write_table(
        pa.table(
            {
                "embedding_config_id": [refs.embedding_config_id] * len(ordenadas),
                "sequence_id": [s for s, _, _, _ in ordenadas],
                "shard_key": [k for _, k, _, _ in ordenadas],
                "shard_sha256": [h for _, _, h, _ in ordenadas],
                "position": [p for _, _, _, p in ordenadas],
            },
            schema=SCHEMA,
        ),
        path,
    )
    return len(ordenadas)
