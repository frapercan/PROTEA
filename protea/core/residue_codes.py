"""Reading per-residue codes back from their shards (ADR-D51).

A sequence's code is located by its ``residue_code`` row: the shard's key, its sha256
and the sequence's position in it. Reading groups the rows by shard, so each shard is
fetched and verified once however many of its sequences are asked for.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from collections.abc import Callable, Iterable
from typing import Any

from protea.core.residue_code_shards import unpack_shard
from protea.core.residue_vocabulary import ResidueCode


def codes_from_index(
    rows: Iterable[tuple[int, str, str, int]], fetch: Callable[[str], bytes]
) -> dict[int, ResidueCode]:
    """Codes by sequence id from index rows ``(sequence_id, shard_key, shard_sha256, position)``.

    Parameters
    ----------
    rows : iterable of tuple
        One per sequence, as the ``residue_code`` table holds them.
    fetch : callable
        Returns a shard's bytes from its key, typically ``ArtifactStore.get``.

    Returns
    -------
    dict of int to ResidueCode
    """
    by_shard: dict[tuple[str, str], list[tuple[int, int]]] = defaultdict(list)
    for sequence_id, key, sha, position in rows:
        by_shard[(key, sha)].append((int(sequence_id), int(position)))
    out: dict[int, ResidueCode] = {}
    for (key, sha), members in by_shard.items():
        shard = unpack_shard(fetch(key), sha)
        for sequence_id, position in members:
            if int(shard.sequence_ids[position]) != sequence_id:
                raise ValueError(
                    f"shard {key} holds sequence {int(shard.sequence_ids[position])} at "
                    f"position {position}, the index says {sequence_id}"
                )
            out[sequence_id] = shard.code(position)
    return out


def load_residue_codes(
    session: Any, store: Any, embedding_config_id: uuid.UUID, sequence_ids: Iterable[int]
) -> dict[int, ResidueCode]:
    """The per-residue codes of some sequences under one residue vocabulary configuration.

    Sequences without a code are absent from the result rather than an error, so a caller
    can tell what has been encoded from what it asked for.
    """
    from sqlalchemy import select

    from protea.infrastructure.orm.models.embedding.residue_code import ResidueCode as Row

    ids = [int(i) for i in sequence_ids]
    if not ids:
        return {}
    rows = session.execute(
        select(Row.sequence_id, Row.shard_key, Row.shard_sha256, Row.position).where(
            Row.embedding_config_id == embedding_config_id, Row.sequence_id.in_(ids)
        )
    ).all()
    return codes_from_index(rows, store.get)
