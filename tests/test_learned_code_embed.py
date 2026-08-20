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
    _SCALER_ENV,
    _base_config_prefix,
    embed_learned_code,
    is_learned_code_config,
    resolve_base_config,
    resolve_encoder_artifact,
    resolve_scaler_artifact,
)
from protea.core.operations.apply_learned_encoder import _load_encoder, _load_scaler
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
         "meta": {"in_dim": in_dim, "dict_dim": dict_dim, "top_k": top_k,
                  "objective": "hard-neg", "order": "pool-then-select", "training_release": "220"}},
        path,
    )


def _save_scaler(path, mu, sigma) -> None:
    np.savez(path, mu=np.asarray(mu, np.float32), sigma=np.asarray(sigma, np.float32))


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


class TestScalerLoad:
    """``_load_scaler`` reads the optional per-dim z-score sidecar or returns None."""

    def test_absent_scaler_is_none(self, tmp_path) -> None:
        assert _load_scaler(None, 8) is None
        assert _load_scaler(str(tmp_path / "missing.scaler.npz"), 8) is None

    def test_loads_mu_sigma(self, tmp_path) -> None:
        p = tmp_path / "h.scaler.npz"
        _save_scaler(p, np.arange(8), np.arange(1, 9))
        mu, sigma = _load_scaler(str(p), 8)
        assert mu.shape == (8,) and sigma.shape == (8,)
        assert np.allclose(mu, np.arange(8))
        assert np.allclose(sigma, np.arange(1, 9))

    def test_shape_mismatch_raises(self, tmp_path) -> None:
        p = tmp_path / "h.scaler.npz"
        _save_scaler(p, np.zeros(4), np.ones(4))
        with pytest.raises(ValueError, match="shape mismatch"):
            _load_scaler(str(p), 8)

    def test_non_positive_sigma_raises(self, tmp_path) -> None:
        p = tmp_path / "h.scaler.npz"
        _save_scaler(p, np.zeros(8), np.array([1, 1, 0, 1, 1, 1, 1, 1]))
        with pytest.raises(ValueError, match="non-positive sigma"):
            _load_scaler(str(p), 8)

    def test_missing_keys_raises(self, tmp_path) -> None:
        p = tmp_path / "h.scaler.npz"
        np.savez(p, mean=np.zeros(8))  # wrong key names
        with pytest.raises(ValueError, match="must contain arrays"):
            _load_scaler(str(p), 8)


def _reference_code(vec, weight, bias, top_k, mu=None, sigma=None):
    """Numpy re-implementation of the apply math (z-score -> L2 -> Linear -> top-k)."""
    x = vec.astype(np.float64)
    if mu is not None:
        x = (x - mu) / sigma
    n = np.linalg.norm(x)
    xn = x / (n if n else 1.0)
    z = weight.astype(np.float64) @ xn + bias.astype(np.float64)
    out = np.zeros_like(z)
    idx = np.argsort(-np.abs(z))[:top_k]
    out[idx] = z[idx]
    return out


class TestScalerApply:
    """A sibling scaler standardises the base before the in-head L2 + Linear; its
    absence leaves the legacy math byte-for-byte unchanged."""

    def test_sibling_scaler_applied_matches_reference(self, tmp_path) -> None:
        in_dim, dict_dim, top_k = 6, 20, 5
        art = tmp_path / "head.pt"
        _save_linear_head(art, in_dim, dict_dim, top_k)
        rng = np.random.RandomState(3)
        mu = rng.randn(in_dim).astype(np.float32)
        sigma = (np.abs(rng.randn(in_dim)) + 0.5).astype(np.float32)
        _save_scaler(tmp_path / "head.scaler.npz", mu, sigma)

        apply, meta = _load_encoder(str(art))  # sibling auto-discovered
        blob = torch.load(str(art), map_location="cpu", weights_only=False)
        w = blob["state_dict"]["weight"].numpy()
        b = blob["state_dict"]["bias"].numpy()
        vec = rng.randn(in_dim).astype(np.float32)

        got = apply([vec[None, :]])[0]
        exp = _reference_code(vec, w, b, top_k, mu, sigma)
        assert np.allclose(got, exp, atol=1e-4)
        assert int((got != 0).sum()) == top_k

    def test_no_scaler_matches_legacy_reference(self, tmp_path) -> None:
        in_dim, dict_dim, top_k = 6, 20, 5
        art = tmp_path / "head.pt"
        _save_linear_head(art, in_dim, dict_dim, top_k)
        apply, _ = _load_encoder(str(art))  # no sibling scaler present
        blob = torch.load(str(art), map_location="cpu", weights_only=False)
        w = blob["state_dict"]["weight"].numpy()
        b = blob["state_dict"]["bias"].numpy()
        rng = np.random.RandomState(4)
        vec = rng.randn(in_dim).astype(np.float32)
        got = apply([vec[None, :]])[0]
        exp = _reference_code(vec, w, b, top_k)  # no mu/sigma
        assert np.allclose(got, exp, atol=1e-4)

    def test_scaler_changes_the_code(self, tmp_path) -> None:
        in_dim, dict_dim, top_k = 6, 20, 5
        art = tmp_path / "head.pt"
        _save_linear_head(art, in_dim, dict_dim, top_k)
        rng = np.random.RandomState(5)
        vec = rng.randn(in_dim).astype(np.float32)
        legacy, _ = _load_encoder(str(art))
        code_legacy = legacy([vec[None, :]])[0]
        _save_scaler(tmp_path / "head.scaler.npz",
                     rng.randn(in_dim).astype(np.float32),
                     (np.abs(rng.randn(in_dim)) + 0.5).astype(np.float32))
        scaled, _ = _load_encoder(str(art))
        code_scaled = scaled([vec[None, :]])[0]
        assert not np.allclose(code_legacy, code_scaled)


class TestScalerResolution:
    def _cfg(self, cfg_id=None):
        return _config("learned-code:hard-neg:08234f06", "learned-code", cfg_id)

    def test_none_when_no_scaler(self, tmp_path, monkeypatch) -> None:
        monkeypatch.delenv(_SCALER_ENV, raising=False)
        monkeypatch.delenv(_DIR_ENV, raising=False)
        art = tmp_path / "head.pt"
        art.write_bytes(b"x")
        assert resolve_scaler_artifact(self._cfg(), str(art)) is None

    def test_explicit_env(self, tmp_path, monkeypatch) -> None:
        s = tmp_path / "explicit.scaler.npz"
        s.write_bytes(b"x")
        monkeypatch.setenv(_SCALER_ENV, str(s))
        assert resolve_scaler_artifact(self._cfg(), str(tmp_path / "head.pt")) == str(s)

    def test_explicit_env_missing_raises(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setenv(_SCALER_ENV, str(tmp_path / "nope.scaler.npz"))
        with pytest.raises(ValueError, match="does not exist"):
            resolve_scaler_artifact(self._cfg(), str(tmp_path / "head.pt"))

    def test_dir_by_config_id(self, tmp_path, monkeypatch) -> None:
        cfg_id = uuid.uuid4()
        cfg = self._cfg(cfg_id)
        s = tmp_path / f"{cfg_id}.scaler.npz"
        s.write_bytes(b"x")
        monkeypatch.delenv(_SCALER_ENV, raising=False)
        monkeypatch.setenv(_DIR_ENV, str(tmp_path))
        assert resolve_scaler_artifact(cfg, str(tmp_path / f"{cfg_id}.pt")) == str(s)

    def test_sibling_fallback(self, tmp_path, monkeypatch) -> None:
        monkeypatch.delenv(_SCALER_ENV, raising=False)
        monkeypatch.delenv(_DIR_ENV, raising=False)
        art = tmp_path / "head.pt"
        art.write_bytes(b"x")
        s = tmp_path / "head.scaler.npz"
        s.write_bytes(b"x")
        assert resolve_scaler_artifact(self._cfg(), str(art)) == str(s)


class TestEmbedLearnedCodeWithScaler:
    def test_scaler_changes_end_to_end_codes(self, tmp_path, monkeypatch) -> None:
        in_dim, dict_dim, top_k = 6, 20, 4
        base = _config("ElnaggarLab/ankh-base", "ankh")
        learned = _config("learned-code:hard-neg:08234f06", "learned-code")
        art = tmp_path / "head.pt"
        _save_linear_head(art, in_dim, dict_dim, top_k)
        monkeypatch.setenv(_ARTIFACT_ENV, str(art))
        monkeypatch.delenv(_SCALER_ENV, raising=False)
        monkeypatch.delenv(_DIR_ENV, raising=False)
        rng = np.random.RandomState(6)
        vecs = rng.randn(in_dim).astype(np.float32)

        def embed_base(base_config, seq_batch):
            return [[ChunkEmbedding(0, None, vecs)] for _ in seq_batch]

        before = embed_learned_code(_FakeSession([base]), learned, ["X"], lambda *a, **k: None,
                                    embed_base=embed_base)
        _save_scaler(tmp_path / "head.scaler.npz",
                     rng.randn(in_dim).astype(np.float32),
                     (np.abs(rng.randn(in_dim)) + 0.5).astype(np.float32))
        after = embed_learned_code(_FakeSession([base]), learned, ["X"], lambda *a, **k: None,
                                   embed_base=embed_base)
        assert not np.allclose(before[0][0].vector, after[0][0].vector)
