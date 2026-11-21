"""A reference pool reads its vectors from the bank's own shards on the node.

Streaming a pool's vectors from the store runs at about 8.5 MB/s, 7 to 9 minutes
per bank. With ``PROTEA_EMBEDDING_BANK_DIR`` set, the store still names the
donors, their order and their sequences, and the vectors come from the shards the
bank was loaded from, each one verified against the manifest the store recorded.
On the development store the pool of ESM-2 650M over the bank of cut 211
(1,507,676 donors) came out identical bit for bit to the database route's, in
115 s.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from protea.core.operations.load_embedding_bank import EmbeddingBankRefused, shard_path
from protea.core.operations.predict_go_terms import _bank_shards as shards

CONFIG = uuid.uuid4()
DIM = 4
VECTORS = {
    "a" * 32: [1.0, 2.0, 3.0, 4.0],
    "b" * 32: [0.5, 0.25, 0.125, 0.0625],
    "c" * 32: [-1.0, 0.0, 1.0, 2.0],
}


def _write_shard(folder: Path, number: int, md5s: list[str]) -> dict[str, Any]:
    values = pa.array(np.asarray([VECTORS[m] for m in md5s], dtype=np.float16).ravel())
    table = pa.table(
        {"md5": pa.array(md5s), "vector": pa.FixedSizeListArray.from_arrays(values, DIM)}
    )
    path = shard_path(folder, CONFIG, number)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return {"shard": number, "rows": len(md5s), "sha256": digest}


@pytest.fixture()
def bank(tmp_path: Path) -> tuple[Path, dict[str, Any], str]:
    listed = [
        _write_shard(tmp_path, 0, ["a" * 32, "b" * 32]),
        _write_shard(tmp_path, 1, ["c" * 32]),
    ]
    manifest = {
        "status": "complete",
        "config_id": str(CONFIG),
        "dim": DIM,
        "model_revision": "r",
        "software": {"torch": "x"},
        "card": "card",
        "shards": listed,
    }
    path = tmp_path / f"bank-{CONFIG}.json"
    path.write_text(json.dumps(manifest))
    return tmp_path, manifest, hashlib.sha256(path.read_bytes()).hexdigest()


def test_each_donor_gets_its_sequences_vector_in_the_pools_order(bank) -> None:
    folder, manifest, _ = bank
    # Two donors share the first sequence; each gets its vector.
    order = ["c" * 32, "a" * 32, "a" * 32, "b" * 32]
    out = shards.gather_from_shards(folder, CONFIG, manifest, order)
    assert out.dtype == np.float16 and out.shape == (4, DIM)
    expected = np.asarray([VECTORS[m] for m in order], dtype=np.float16)
    assert np.array_equal(out.view(np.uint16), expected.view(np.uint16))


def test_a_shard_that_does_not_hash_as_the_manifest_says_is_refused(bank) -> None:
    folder, manifest, _ = bank
    path = shard_path(folder, CONFIG, 1)
    path.write_bytes(path.read_bytes() + b"\0")
    with pytest.raises(EmbeddingBankRefused, match="hashes to"):
        shards.gather_from_shards(folder, CONFIG, manifest, ["c" * 32])


def test_a_donor_no_shard_holds_is_refused_not_left_empty(bank) -> None:
    folder, manifest, _ = bank
    with pytest.raises(EmbeddingBankRefused, match="1 sequences of the pool are in no shard"):
        shards.gather_from_shards(folder, CONFIG, manifest, ["a" * 32, "d" * 32])


class _Query:
    """The pool query: donors in the bank's order with their sequence's md5."""

    def add_columns(self, *_a: Any) -> _Query:
        return self

    def join(self, *_a: Any, **_k: Any) -> _Query:
        return self

    def yield_per(self, _n: int) -> list[tuple[str, str]]:
        return [("P2", "b" * 32), ("P1", "a" * 32)]


def test_the_pool_comes_from_the_shards_only_when_the_store_recorded_that_manifest(
    bank, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder, _, digest = bank
    session = SimpleNamespace()
    monkeypatch.delenv(shards.BANK_DIR_ENV, raising=False)
    monkeypatch.setattr(shards, "recorded_manifest", lambda _s, _c: digest)
    assert shards.fill_from_shards(session, _Query(), CONFIG) is None  # the node names no folder

    monkeypatch.setenv(shards.BANK_DIR_ENV, str(folder))
    accessions, vectors = shards.fill_from_shards(session, _Query(), CONFIG)
    assert accessions == ["P2", "P1"]
    assert np.array_equal(vectors, np.asarray([VECTORS["b" * 32], VECTORS["a" * 32]], np.float16))

    monkeypatch.setattr(shards, "recorded_manifest", lambda _s, _c: "0" * 64)
    assert shards.fill_from_shards(session, _Query(), CONFIG) is None  # another manifest
    monkeypatch.setattr(shards, "recorded_manifest", lambda _s, _c: None)
    assert shards.fill_from_shards(session, _Query(), CONFIG) is None  # no bank loaded
