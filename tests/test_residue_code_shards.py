"""Shards of per-residue codes round trip exactly and refuse what they cannot vouch for (ADR-D51)."""

from __future__ import annotations

import numpy as np
import pytest

from protea.core.residue_code_shards import SHARD_FORMAT, pack_shard, unpack_shard
from protea.core.residue_vocabulary import ResidueCode


def _random_code(rng: np.random.Generator, residues: int, dict_dim: int = 8192) -> ResidueCode:
    feats, levs, offs = [], [], [0]
    for _ in range(residues):
        k = int(rng.integers(0, 160))
        f = np.sort(rng.choice(dict_dim, size=k, replace=False)).astype(np.int16)
        feats.append(f)
        levs.append(rng.integers(0, 256, size=k).astype(np.uint8))
        offs.append(offs[-1] + k)
    return ResidueCode(
        features=np.concatenate(feats) if feats else np.zeros(0, np.int16),
        levels=np.concatenate(levs) if levs else np.zeros(0, np.uint8),
        offsets=np.array(offs, dtype=np.int64),
    )


def _equal(a: ResidueCode, b: ResidueCode) -> bool:
    return (
        np.array_equal(a.features, b.features)
        and np.array_equal(a.levels, b.levels)
        and np.array_equal(a.offsets, b.offsets)
    )


def test_a_shard_round_trips_every_code_exactly():
    rng = np.random.default_rng(0)
    codes = [(1000 + i, _random_code(rng, int(n))) for i, n in enumerate([1, 40, 0, 7, 120])]
    blob, sha = pack_shard(codes, {"vocabulary": "toy"})
    shard = unpack_shard(blob, sha)
    assert len(shard) == len(codes)
    for position, (sequence_id, code) in enumerate(codes):
        assert shard.position_of(sequence_id) == position
        assert _equal(shard.code(position), code)


def test_the_header_survives_and_carries_the_format():
    blob, sha = pack_shard([(1, _random_code(np.random.default_rng(1), 3))], {"policy": "bf16"})
    header = unpack_shard(blob, sha).header
    assert header == {"policy": "bf16", "format": SHARD_FORMAT}


def test_bytes_that_do_not_match_their_sha256_are_refused():
    blob, sha = pack_shard([(1, _random_code(np.random.default_rng(2), 3))], {})
    with pytest.raises(ValueError, match="sha256"):
        unpack_shard(blob, "0" * 64)
    assert unpack_shard(blob, sha) is not None


def test_features_out_of_order_within_a_residue_are_refused():
    code = ResidueCode(
        features=np.array([5, 3], dtype=np.int16),
        levels=np.array([1, 2], dtype=np.uint8),
        offsets=np.array([0, 2], dtype=np.int64),
    )
    with pytest.raises(ValueError, match="ascending"):
        pack_shard([(1, code)], {})


def test_a_sequence_written_twice_in_one_shard_is_refused():
    code = _random_code(np.random.default_rng(3), 2)
    with pytest.raises(ValueError, match="twice"):
        pack_shard([(7, code), (7, code)], {})


def test_an_unknown_sequence_is_not_found():
    blob, sha = pack_shard([(1, _random_code(np.random.default_rng(4), 2))], {})
    with pytest.raises(KeyError):
        unpack_shard(blob, sha).position_of(2)
