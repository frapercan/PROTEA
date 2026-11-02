"""Shards of per-residue codes in the artifact store (ADR-D51).

A GPU batch writes the codes of its sequences as ONE object: the arrays below in an
uncompressed ``.npz`` (so it loads without pickle), compressed as a whole with zlib at
level 1. Level 1 was chosen by measurement on the lab's codes: 292 bytes per residue in
1.4 s per 212 thousand residues, against 286 bytes in 5.2 s at level 6.

Arrays, in residue order within each sequence and sequence order within the shard:

* ``sequence_ids`` (int64, sequences): the platform's sequence ids;
* ``residue_offsets`` (int64, sequences + 1): where each sequence's residues start;
* ``entry_offsets`` (int64, residues + 1): where each residue's entries start;
* ``feature_deltas`` (uint16, entries): each residue's ascending feature indices, the
  first one absolute and the rest as differences, which compress better than indices;
* ``levels`` (uint8, entries);
* ``header_json``: the producing vocabulary's manifest hash, the backend's precision
  policy and anything else the writer declares.

The index row of a sequence names the shard's key, its sha256 and the sequence's
position, so a reader verifies the bytes before trusting them.
"""

from __future__ import annotations

import hashlib
import io
import json
import zlib
from dataclasses import dataclass
from typing import Any

import numpy as np

from protea.core.residue_vocabulary import ResidueCode

#: Format tag written into every header and checked on read.
SHARD_FORMAT = "residue-code-shard/1"

#: zlib level, chosen by measurement (see the module docstring).
ZLIB_LEVEL = 1


@dataclass(frozen=True)
class Shard:
    """A decoded shard: its header and the code of every sequence, by position."""

    header: dict[str, Any]
    sequence_ids: np.ndarray
    residue_offsets: np.ndarray
    entry_offsets: np.ndarray
    features: np.ndarray
    levels: np.ndarray

    def __len__(self) -> int:
        return int(self.sequence_ids.shape[0])

    def code(self, position: int) -> ResidueCode:
        """The code of the sequence at ``position`` in the shard."""
        r0, r1 = int(self.residue_offsets[position]), int(self.residue_offsets[position + 1])
        e0, e1 = int(self.entry_offsets[r0]), int(self.entry_offsets[r1])
        return ResidueCode(
            features=self.features[e0:e1],
            levels=self.levels[e0:e1],
            offsets=self.entry_offsets[r0 : r1 + 1] - e0,
        )

    def position_of(self, sequence_id: int) -> int:
        """Position of a sequence id in the shard."""
        hits = np.flatnonzero(self.sequence_ids == sequence_id)
        if hits.size != 1:
            raise KeyError(f"sequence {sequence_id} appears {hits.size} times in the shard")
        return int(hits[0])


def _delta_encode(features: np.ndarray, entry_offsets: np.ndarray) -> np.ndarray:
    f = features.astype(np.int64)
    d = np.diff(f, prepend=0)
    starts = entry_offsets[:-1][np.diff(entry_offsets) > 0]
    d[starts] = f[starts]
    if d.size and (d.min() < 0 or d.max() > np.iinfo(np.uint16).max):
        raise ValueError("features are not ascending within a residue, or exceed uint16")
    return d.astype(np.uint16)


def _delta_decode(deltas: np.ndarray, entry_offsets: np.ndarray) -> np.ndarray:
    c = np.cumsum(deltas.astype(np.int64))
    counts = np.diff(entry_offsets)
    before = np.concatenate([[0], c])[entry_offsets[:-1]]
    return (c - np.repeat(before, counts)).astype(np.int16)


def pack_shard(codes: list[tuple[int, ResidueCode]], header: dict[str, Any]) -> tuple[bytes, str]:
    """Serialise the codes of a batch of sequences as one compressed object.

    Parameters
    ----------
    codes : list of (int, ResidueCode)
        Sequence id and code, in the order the shard keeps them.
    header : dict
        What the writer declares (the vocabulary's manifest hash, the precision policy).

    Returns
    -------
    (bytes, str)
        The object and its sha256.
    """
    ids = np.array([int(i) for i, _ in codes], dtype=np.int64)
    if np.unique(ids).size != ids.size:
        raise ValueError("a sequence appears twice in one shard")
    residue_counts = np.array([c.residues for _, c in codes], dtype=np.int64)
    residue_offsets = np.concatenate([[0], np.cumsum(residue_counts)]).astype(np.int64)
    entry_offsets = [np.zeros(1, dtype=np.int64)]
    base = 0
    for _, c in codes:
        entry_offsets.append(c.offsets[1:].astype(np.int64) + base)
        base += c.entries
    eo = np.concatenate(entry_offsets)
    features = np.concatenate([c.features for _, c in codes]) if codes else np.zeros(0, np.int16)
    levels = np.concatenate([c.levels for _, c in codes]) if codes else np.zeros(0, np.uint8)
    full_header = {**header, "format": SHARD_FORMAT}
    buf = io.BytesIO()
    np.savez(
        buf,
        sequence_ids=ids,
        residue_offsets=residue_offsets,
        entry_offsets=eo,
        feature_deltas=_delta_encode(features, eo),
        levels=levels.astype(np.uint8),
        header_json=np.array(json.dumps(full_header, sort_keys=True)),
    )
    blob = zlib.compress(buf.getvalue(), ZLIB_LEVEL)
    return blob, hashlib.sha256(blob).hexdigest()


def unpack_shard(blob: bytes, sha256: str | None = None) -> Shard:
    """Decode a shard, verifying its sha256 first when one is given.

    Raises
    ------
    ValueError
        When the bytes do not match the expected sha256 or the format is unknown.
    """
    if sha256 is not None:
        got = hashlib.sha256(blob).hexdigest()
        if got != sha256:
            raise ValueError(f"shard sha256 is {got}, the index says {sha256}")
    with np.load(io.BytesIO(zlib.decompress(blob)), allow_pickle=False) as data:
        header = json.loads(str(data["header_json"]))
        if header.get("format") != SHARD_FORMAT:
            raise ValueError(f"unknown shard format {header.get('format')!r}")
        eo = np.asarray(data["entry_offsets"])
        return Shard(
            header=header,
            sequence_ids=np.asarray(data["sequence_ids"]),
            residue_offsets=np.asarray(data["residue_offsets"]),
            entry_offsets=eo,
            features=_delta_decode(np.asarray(data["feature_deltas"]), eo),
            levels=np.asarray(data["levels"]),
        )
