"""Real-model inference tests.

Each test loads actual pretrained weights and runs a short forward pass.
All tests are marked ``slow`` — run with:

    pytest -m slow tests/test_real_models.py -v

Model used: facebook/esm2_t6_8M_UR50D (~30 MB, CPU — fast).
Large models (650M, T5) are covered by the synthetic tests in test_compute_embeddings.py.
"""
from __future__ import annotations

import numpy as np
import pytest

from protea.core.operations.compute_embeddings import _embed_esm

SEQS = ["ACDEFGHIK", "LMNPQRSTVW", "ACDE", "GHIKLM"]


def _esm_cfg(model_name: str, *, normalize: bool = True):
    from unittest.mock import MagicMock
    cfg = MagicMock()
    cfg.model_name = model_name
    cfg.model_backend = "esm"
    cfg.layer_indices = [0]
    cfg.layer_agg = "mean"
    cfg.pooling = "mean"
    cfg.normalize_residues = False
    cfg.normalize = normalize
    cfg.max_length = 1022
    cfg.use_chunking = False
    cfg.chunk_size = 512
    cfg.chunk_overlap = 0
    return cfg


@pytest.mark.slow
class TestESM2_8M:
    """ESM-2 8M on CPU — ~30 MB, completes in seconds."""

    MODEL = "facebook/esm2_t6_8M_UR50D"
    DEVICE = "cpu"
    DIM = 320

    @pytest.fixture(scope="class")
    def model_and_tokenizer(self):
        from transformers import AutoTokenizer, EsmModel
        tokenizer = AutoTokenizer.from_pretrained(self.MODEL)
        model = EsmModel.from_pretrained(self.MODEL, output_hidden_states=True)
        model.eval()
        yield model, tokenizer

    def test_output_shape_and_finite(self, model_and_tokenizer):
        model, tokenizer = model_and_tokenizer
        cfg = _esm_cfg(self.MODEL, normalize=False)
        results = _embed_esm(model, tokenizer, SEQS, cfg, self.DEVICE)
        assert len(results) == len(SEQS)
        for i, chunks in enumerate(results):
            vec = chunks[0].vector
            assert vec.shape == (self.DIM,), f"seq{i}: wrong shape {vec.shape}"
            assert np.all(np.isfinite(vec)), f"seq{i}: NaN/Inf in vector"

    def test_normalize_produces_unit_norm(self, model_and_tokenizer):
        model, tokenizer = model_and_tokenizer
        cfg = _esm_cfg(self.MODEL, normalize=True)
        results = _embed_esm(model, tokenizer, SEQS, cfg, self.DEVICE)
        for i, chunks in enumerate(results):
            norm = float(np.linalg.norm(chunks[0].vector))
            assert abs(norm - 1.0) < 1e-4, f"seq{i}: expected unit norm, got {norm:.6f}"

    def test_deterministic(self, model_and_tokenizer):
        model, tokenizer = model_and_tokenizer
        cfg = _esm_cfg(self.MODEL, normalize=False)
        first  = [_embed_esm(model, tokenizer, [s], cfg, self.DEVICE)[0][0].vector for s in SEQS]
        second = [_embed_esm(model, tokenizer, [s], cfg, self.DEVICE)[0][0].vector for s in SEQS]
        for i in range(len(SEQS)):
            np.testing.assert_array_equal(first[i], second[i], err_msg=f"seq{i}: non-deterministic")

    def test_batch_size_consistency(self, model_and_tokenizer):
        model, tokenizer = model_and_tokenizer
        cfg = _esm_cfg(self.MODEL, normalize=False)
        ref = [_embed_esm(model, tokenizer, [s], cfg, self.DEVICE)[0][0].vector for s in SEQS]
        for batch_size in (2, 4):
            results = []
            for i in range(0, len(SEQS), batch_size):
                for chunks in _embed_esm(model, tokenizer, SEQS[i:i + batch_size], cfg, self.DEVICE):
                    results.append(chunks[0].vector)
            for i in range(len(SEQS)):
                np.testing.assert_allclose(
                    results[i], ref[i], rtol=1e-5, atol=1e-6,
                    err_msg=f"batch_size={batch_size}: mismatch at seq {i}",
                )
