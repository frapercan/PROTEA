"""encode_residue_vocabulary and the read path, through their pure pieces (ADR-D51).

The states come from the fixture the lab's reference encoder produced, served by a fake
backend, so the codes written to a shard and read back are compared with the reference
codes exactly.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import pytest

torch = pytest.importorskip("torch")

from protea.core._evaluation_leakage import FITTED_BACKENDS  # noqa: E402
from protea.core.operations.encode_residue_vocabulary import (  # noqa: E402
    TARGET_BACKEND,
    EncodeResidueVocabularyPayload,
    ModelPass,
    build_messages,
    calls_by_residues,
    encode_sequences,
    index_rows,
    put_with_retries,
    read_vocabulary_meta,
    refuse_backend_without_layers,
    shard_key,
)
from protea.core.residue_code_shards import pack_shard  # noqa: E402
from protea.core.residue_codes import codes_from_index  # noqa: E402
from protea.core.residue_vocabulary import REQUIRED_ARRAYS, ResidueVocabulary  # noqa: E402
from protea.infrastructure.orm.models.embedding.embedding_config import (  # noqa: E402
    VALID_BACKENDS,
)

FIXTURE = Path(__file__).parent / "fixtures" / "residue_vocabulary_vectors.npz"


@pytest.fixture(scope="module")
def vectors() -> dict[str, np.ndarray]:
    with np.load(FIXTURE, allow_pickle=False) as data:
        return {k: np.asarray(data[k]) for k in data.files}


def _vocabulary(vectors: dict[str, np.ndarray]) -> ResidueVocabulary:
    meta = json.loads(str(vectors["meta_json"]))
    return ResidueVocabulary({k: vectors[k] for k in REQUIRED_ARRAYS}, meta)


def _sequences(vectors: dict[str, np.ndarray]) -> list[tuple[int, str, np.ndarray, int]]:
    """(sequence id, a sequence of the right length, its raw states, its first residue)."""
    out, start = [], 0
    for i, n in enumerate(vectors["lengths"].tolist()):
        out.append((100 + i, "ACDEFGHIKL"[i % 10] * n, vectors["states"][start : start + n], start))
        start += n
    return out


class _Backend:
    """Serves the fixture's raw states as ``embed_batch_per_layer`` would."""

    def __init__(self, by_sequence: dict[str, np.ndarray]):
        self.by_sequence = by_sequence
        self.calls: list[tuple[list[str], list[int]]] = []

    def embed_batch_per_layer(
        self,
        model: Any,
        tokenizer: Any,
        sequences: list[str],
        *,
        emit: Any,
        hidden_state_indices: list[int],
    ) -> list[np.ndarray]:
        self.calls.append((list(sequences), list(hidden_state_indices)))
        return [self.by_sequence[s] for s in sequences]


def _reference_codes(vectors: dict[str, np.ndarray]) -> list[tuple[np.ndarray, ...]]:
    """(features, levels, offsets) per sequence, as the reference encoder wrote them."""
    out, entry, at = [], 0, 0
    for n in vectors["lengths"].tolist():
        offsets = vectors["code_offsets"][at : at + n + 1]
        at += n + 1
        k = int(offsets[-1])
        out.append(
            (
                vectors["code_features"][entry : entry + k],
                vectors["code_levels"][entry : entry + k],
                offsets,
            )
        )
        entry += k
    return out


def test_the_payload_needs_exactly_one_address() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        EncodeResidueVocabularyPayload.model_validate({"source_embedding_config_id": "x"})
    with pytest.raises(ValueError, match="exactly one"):
        EncodeResidueVocabularyPayload.model_validate(
            {
                "source_embedding_config_id": "x",
                "vocabulary_artifact_uri": "a",
                "vocabulary_artifact_path": "b",
            }
        )
    with pytest.raises(ValueError, match="non-empty"):
        EncodeResidueVocabularyPayload.model_validate(
            {"source_embedding_config_id": "  ", "vocabulary_artifact_uri": "a"}
        )


def test_messages_partition_the_ids_and_go_to_the_card_queue() -> None:
    p = EncodeResidueVocabularyPayload.model_validate(
        {
            "source_embedding_config_id": "s",
            "vocabulary_artifact_uri": "residue-vocabulary/v1-0123456789abcdef.npz",
            "sequences_per_job": 256,
        }
    )
    parent, source, target = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    msgs = build_messages(p, parent, source, target, list(range(600)))
    assert [len(m[1]["payload"]["sequence_ids"]) for m in msgs] == [256, 256, 88]
    assert {q for q, _m in msgs} == {"protea.embeddings.batch"}
    first = msgs[0][1]
    assert first["operation"] == "encode_residue_vocabulary_batch"
    assert first["payload"]["target_embedding_config_id"] == str(target)
    assert first["payload"]["vocabulary_artifact_uri"] == p.vocabulary_artifact_uri


def test_calls_keep_the_residue_limit_and_the_order() -> None:
    pending = [(1, "A" * 10), (2, "A" * 30), (3, "A" * 100), (4, "A" * 5), (5, "A" * 5)]
    calls = calls_by_residues(pending, residues_per_call=40)
    assert [[i for i, _s in c] for c in calls] == [[1, 2], [3], [4, 5]]


def test_the_codes_are_the_reference_codes(vectors: dict[str, np.ndarray]) -> None:
    seqs = _sequences(vectors)
    backend = _Backend({s: st for _i, s, st, _a in seqs})
    voc = _vocabulary(vectors)
    codes = encode_sequences(
        ModelPass(backend, None, None),
        voc,
        [(i, s) for i, s, _st, _a in seqs],
        emit=lambda *a, **k: None,
        residues_per_call=40,
    )
    assert [i for i, _c in codes] == [i for i, _s, _st, _a in seqs]
    for (_i, code), (f, lev, off) in zip(codes, _reference_codes(vectors), strict=True):
        assert np.array_equal(code.features, f)
        assert np.array_equal(code.levels, lev)
        assert np.array_equal(code.offsets, off)
    assert all(c[1] == voc.meta["hidden_state_indices"] for c in backend.calls)
    assert len(backend.calls) > 1


def test_a_shard_and_its_index_read_back_every_code(vectors: dict[str, np.ndarray]) -> None:
    seqs = _sequences(vectors)
    voc = _vocabulary(vectors)
    codes = encode_sequences(
        ModelPass(_Backend({s: st for _i, s, st, _a in seqs}), None, None),
        voc,
        [(i, s) for i, s, _st, _a in seqs],
        emit=lambda *a, **k: None,
        residues_per_call=1000,
    )
    blob, sha = pack_shard(codes, {"vocabulary": "toy"})
    target = uuid.uuid4()
    key = shard_key(target, codes, sha)
    assert key.startswith(f"residue-codes/{target}/100-105-") and key.endswith(".bin")
    store = {key: blob}
    rows = index_rows(target, key, sha, codes)
    assert [r["position"] for r in rows] == list(range(len(codes)))
    got = codes_from_index(
        [(r["sequence_id"], r["shard_key"], r["shard_sha256"], r["position"]) for r in rows],
        store.__getitem__,
    )
    for sequence_id, code in codes:
        assert np.array_equal(got[sequence_id].features, code.features)
        assert np.array_equal(got[sequence_id].levels, code.levels)
        assert np.array_equal(got[sequence_id].offsets, code.offsets)
    with pytest.raises(ValueError, match="the index says"):
        codes_from_index([(999, key, sha, 0)], store.__getitem__)


def test_a_backend_without_per_layer_states_is_refused() -> None:
    with pytest.raises(ValueError, match="embed_batch_per_layer"):
        refuse_backend_without_layers(object(), "esm")


def test_the_meta_is_read_without_the_arrays(
    vectors: dict[str, np.ndarray], tmp_path: Path
) -> None:
    good = tmp_path / "vocabulary.npz"
    np.savez(good, meta_json=vectors["meta_json"], **{k: vectors[k] for k in REQUIRED_ARRAYS})
    assert read_vocabulary_meta(str(good))["kind"] == "residue-vocabulary"
    meta = json.loads(str(vectors["meta_json"]))
    meta["kind"] = "residue-sparse"
    bad = tmp_path / "other.npz"
    np.savez(bad, meta_json=np.array(json.dumps(meta)))
    with pytest.raises(ValueError, match="kind"):
        read_vocabulary_meta(str(bad))


def test_the_backend_tag_is_registered_and_declares_a_cut() -> None:
    assert TARGET_BACKEND in VALID_BACKENDS
    assert TARGET_BACKEND in FITTED_BACKENDS


def test_a_shard_put_is_retried_until_the_store_takes_it() -> None:
    class Flaky:
        def __init__(self, failures: int):
            self.failures, self.kept = failures, {}

        def put(self, key: str, blob: bytes) -> str:
            if self.failures:
                self.failures -= 1
                raise ConnectionError("the link dropped")
            self.kept[key] = blob
            return key

    store = Flaky(2)
    put_with_retries(store, "k", b"x", waits=(0.0, 0.0, 0.0))
    assert store.kept == {"k": b"x"}
    with pytest.raises(ConnectionError):
        put_with_retries(Flaky(5), "k", b"x", waits=(0.0, 0.0))
