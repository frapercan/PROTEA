"""Unit tests for on-the-fly learned-code serve embedding (no DB, no GPU).

Covers base-config resolution, head-artifact env resolution, the end-to-end
base -> head -> code path (with a mocked base embed), and the clear-error
failure modes. The standard-PLM embed path is exercised elsewhere
(``test_compute_embeddings.py``) and is untouched by this module.
"""
from __future__ import annotations

import uuid

import numpy as np
import pytest
import torch
import torch.nn as nn
from protea_backends._chunk_helpers import ChunkEmbedding

from protea.core.operations._learned_code_embed import (
    _ARTIFACT_ENV,
    _DIR_ENV,
    _base_config_prefix,
    embed_learned_code,
    is_learned_code_config,
    resolve_base_config,
    resolve_encoder_artifact,
)
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig


def _config(model_name: str, backend: str, cfg_id: uuid.UUID | None = None) -> EmbeddingConfig:
    c = EmbeddingConfig(model_name=model_name, model_backend=backend, layer_indices=[0])
    c.id = cfg_id or uuid.uuid4()
    return c


class _FakeResult:
    def __init__(self, rows: list) -> None:
        self._rows = rows

    def scalars(self) -> _FakeResult:
        return self

    def all(self) -> list:
        return self._rows


class _FakeSession:
    """Minimal stand-in: ``execute(stmt).scalars().all()`` returns fixed rows."""

    def __init__(self, rows: list) -> None:
        self._rows = rows

    def execute(self, _stmt: object) -> _FakeResult:
        return _FakeResult(self._rows)


def _save_linear_head(path, in_dim: int, dict_dim: int, top_k: int) -> None:
    enc = nn.Linear(in_dim, dict_dim)
    torch.save(
        {"state_dict": enc.state_dict(),
         "meta": {"in_dim": in_dim, "dict_dim": dict_dim, "top_k": top_k, "objective": "hard-neg"}},
        path,
    )


class TestDetection:
    def test_learned_code_detected(self) -> None:
        assert is_learned_code_config(_config("learned-code:hard-neg:08234f06", "learned-code"))

    def test_standard_plm_not_learned(self) -> None:
        assert not is_learned_code_config(_config("ElnaggarLab/ankh-base", "ankh"))


class TestBaseConfigResolution:
    def test_prefix_is_trailing_segment(self) -> None:
        cfg = _config("learned-code:hard-neg:08234f06", "learned-code")
        assert _base_config_prefix(cfg) == "08234f06"

    def test_resolves_base_config(self) -> None:
        base_id = uuid.UUID("08234f06-0000-4000-8000-000000000000")
        base = _config("ElnaggarLab/ankh-base", "ankh", base_id)
        learned = _config("learned-code:hard-neg:08234f06", "learned-code")
        assert resolve_base_config(_FakeSession([base]), learned) is base

    def test_ignores_self_learned_match(self) -> None:
        # only a learned-code row matches the prefix -> treated as "no base"
        learned = _config("learned-code:hard-neg:08234f06", "learned-code")
        with pytest.raises(ValueError, match="no base EmbeddingConfig"):
            resolve_base_config(_FakeSession([learned]), learned)

    def test_missing_base_raises(self) -> None:
        learned = _config("learned-code:hard-neg:08234f06", "learned-code")
        with pytest.raises(ValueError, match="no base EmbeddingConfig"):
            resolve_base_config(_FakeSession([]), learned)

    def test_ambiguous_base_raises(self) -> None:
        a = _config("ankh-base", "ankh")
        b = _config("esm-other", "esm")
        learned = _config("learned-code:hard-neg:08234f06", "learned-code")
        with pytest.raises(ValueError, match="ambiguous"):
            resolve_base_config(_FakeSession([a, b]), learned)

    def test_no_prefix_raises(self) -> None:
        learned = _config("learnedcode", "learned-code")  # no ':' segment
        # trailing segment is the whole name -> no config matches -> clear error
        with pytest.raises(ValueError, match="no base EmbeddingConfig"):
            resolve_base_config(_FakeSession([]), learned)


class TestArtifactResolution:
    def test_explicit_artifact_env(self, tmp_path, monkeypatch) -> None:
        art = tmp_path / "head.pt"
        art.write_bytes(b"x")
        monkeypatch.setenv(_ARTIFACT_ENV, str(art))
        cfg = _config("learned-code:hard-neg:08234f06", "learned-code")
        assert resolve_encoder_artifact(cfg) == str(art)

    def test_explicit_artifact_missing_raises(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setenv(_ARTIFACT_ENV, str(tmp_path / "nope.pt"))
        monkeypatch.delenv(_DIR_ENV, raising=False)
        with pytest.raises(ValueError, match="does not exist"):
            resolve_encoder_artifact(_config("learned-code:hard-neg:08234f06", "learned-code"))

    def test_dir_env_by_config_id(self, tmp_path, monkeypatch) -> None:
        cfg_id = uuid.uuid4()
        cfg = _config("learned-code:hard-neg:08234f06", "learned-code", cfg_id)
        (tmp_path / f"{cfg_id}.pt").write_bytes(b"x")
        monkeypatch.delenv(_ARTIFACT_ENV, raising=False)
        monkeypatch.setenv(_DIR_ENV, str(tmp_path))
        assert resolve_encoder_artifact(cfg) == str(tmp_path / f"{cfg_id}.pt")

    def test_dir_env_by_short_prefix(self, tmp_path, monkeypatch) -> None:
        cfg_id = uuid.uuid4()
        cfg = _config("learned-code:hard-neg:08234f06", "learned-code", cfg_id)
        (tmp_path / f"{str(cfg_id)[:8]}.pt").write_bytes(b"x")
        monkeypatch.delenv(_ARTIFACT_ENV, raising=False)
        monkeypatch.setenv(_DIR_ENV, str(tmp_path))
        assert resolve_encoder_artifact(cfg) == str(tmp_path / f"{str(cfg_id)[:8]}.pt")

    def test_unset_env_raises(self, monkeypatch) -> None:
        monkeypatch.delenv(_ARTIFACT_ENV, raising=False)
        monkeypatch.delenv(_DIR_ENV, raising=False)
        with pytest.raises(ValueError, match="needs a head artifact"):
            resolve_encoder_artifact(_config("learned-code:hard-neg:08234f06", "learned-code"))


class TestEmbedLearnedCode:
    def _wire(self, tmp_path, monkeypatch, in_dim: int, dict_dim: int, top_k: int):
        base = _config("ElnaggarLab/ankh-base", "ankh")
        learned = _config("learned-code:hard-neg:08234f06", "learned-code")
        art = tmp_path / "head.pt"
        _save_linear_head(art, in_dim, dict_dim, top_k)
        monkeypatch.setenv(_ARTIFACT_ENV, str(art))
        return base, learned

    def test_end_to_end_produces_expected_dim_codes(self, tmp_path, monkeypatch) -> None:
        in_dim, dict_dim, top_k = 768, 2048, 200
        base, learned = self._wire(tmp_path, monkeypatch, in_dim, dict_dim, top_k)
        rng = np.random.RandomState(0)
        calls: list[int] = []

        def embed_base(base_config, seq_batch):
            assert base_config is base
            calls.append(len(seq_batch))
            return [[ChunkEmbedding(0, None, rng.randn(in_dim).astype(np.float32))]
                    for _ in seq_batch]

        out = embed_learned_code(
            _FakeSession([base]), learned, ["MKT", "ACDEF", "WW"], lambda *a, **k: None,
            embed_base=embed_base, batch_size=2,
        )
        # aligned one code per query, each a single 2048-d ChunkEmbedding
        assert len(out) == 3
        for chunks in out:
            assert len(chunks) == 1
            vec = chunks[0].vector
            assert vec.shape == (dict_dim,)
            assert int((vec != 0).sum()) == top_k  # top-k real code
        assert calls == [2, 1]  # batched 2 then 1

    def test_multichunk_base_is_mean_pooled(self, tmp_path, monkeypatch) -> None:
        in_dim, dict_dim, top_k = 6, 20, 4
        base, learned = self._wire(tmp_path, monkeypatch, in_dim, dict_dim, top_k)
        rng = np.random.RandomState(1)
        multi = rng.randn(3, in_dim).astype(np.float32)

        def embed_base_multi(base_config, seq_batch):
            return [[ChunkEmbedding(i, None, multi[i]) for i in range(3)]]

        def embed_base_mean(base_config, seq_batch):
            return [[ChunkEmbedding(0, None, multi.mean(axis=0))]]

        a = embed_learned_code(_FakeSession([base]), learned, ["X"], lambda *a, **k: None,
                               embed_base=embed_base_multi)
        b = embed_learned_code(_FakeSession([base]), learned, ["X"], lambda *a, **k: None,
                               embed_base=embed_base_mean)
        assert np.allclose(a[0][0].vector, b[0][0].vector, atol=1e-5)

    def test_missing_head_raises_clear_error(self, tmp_path, monkeypatch) -> None:
        base = _config("ElnaggarLab/ankh-base", "ankh")
        learned = _config("learned-code:hard-neg:08234f06", "learned-code")
        monkeypatch.delenv(_ARTIFACT_ENV, raising=False)
        monkeypatch.delenv(_DIR_ENV, raising=False)
        with pytest.raises(ValueError, match="needs a head artifact"):
            embed_learned_code(_FakeSession([base]), learned, ["X"], lambda *a, **k: None,
                               embed_base=lambda *a, **k: [])


class TestBatchOperationRouting:
    """The batch op routes learned-code configs to the on-the-fly path and
    standard-PLM configs to the unchanged model path."""

    def _seq(self, seq_id: int, seq: str):
        from protea.infrastructure.orm.models.sequence.sequence import Sequence

        s = Sequence(sequence=seq, sequence_hash=Sequence.compute_hash(seq))
        s.id = seq_id
        return s

    def test_learned_code_config_routes_to_on_the_fly(self, monkeypatch) -> None:
        from unittest import mock

        from protea.core.operations.compute_embeddings import (
            ComputeEmbeddingsBatchOperation,
            ComputeEmbeddingsBatchPayload,
        )

        cfg = _config("learned-code:hard-neg:08234f06", "learned-code")
        seqs = [self._seq(1, "MKT"), self._seq(2, "ACDEF")]
        codes = [[ChunkEmbedding(0, None, np.ones(2048, np.float32))] for _ in seqs]

        captured: dict = {}

        def fake_embed_learned_code(session, config, sequences, emit, *, embed_base,
                                    batch_size):
            captured["config"] = config
            captured["sequences"] = sequences
            return codes

        monkeypatch.setattr(
            "protea.core.operations._learned_code_embed.embed_learned_code",
            fake_embed_learned_code,
        )
        op = ComputeEmbeddingsBatchOperation()
        p = ComputeEmbeddingsBatchPayload(
            embedding_config_id=str(cfg.id), sequence_ids=[1, 2],
            parent_job_id=str(uuid.uuid4()), device="cpu",
        )
        # standard-PLM model loader must NOT be touched on the learned path
        with mock.patch(
            "protea.core.operations.compute_embeddings._get_or_load_model"
        ) as loader:
            rows = op._infer_all_learned_code(_FakeSession([cfg]), cfg, seqs, p, lambda *a, **k: None)
        loader.assert_not_called()
        assert captured["config"] is cfg
        assert captured["sequences"] == ["MKT", "ACDEF"]
        assert [r["sequence_id"] for r in rows] == [1, 2]
        assert rows[0]["chunks"][0]["embedding_dim"] == 2048
