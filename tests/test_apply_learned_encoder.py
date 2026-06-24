"""Unit tests for apply_learned_encoder (payload + encoder apply math; no DB)."""
from __future__ import annotations

import uuid

import numpy as np
import pytest

from protea.core.operations.apply_learned_encoder import (
    ApplyLearnedEncoderPayload,
    _load_encoder,
)


class TestPayload:
    def test_valid(self):
        p = ApplyLearnedEncoderPayload(
            source_embedding_config_id=str(uuid.uuid4()), encoder_artifact_path="/tmp/e.pt")
        assert p.batch_size == 4000 and p.skip_existing is True and p.sequence_id_limit is None

    def test_empty_source_raises(self):
        with pytest.raises(ValueError):
            ApplyLearnedEncoderPayload(source_embedding_config_id="  ",
                                       encoder_artifact_path="/tmp/e.pt")

    def test_empty_artifact_raises(self):
        with pytest.raises(ValueError):
            ApplyLearnedEncoderPayload(source_embedding_config_id=str(uuid.uuid4()),
                                       encoder_artifact_path="")


def test_load_encoder_apply_produces_topk_real_code(tmp_path):
    import torch
    import torch.nn as nn

    in_dim, dict_dim, top_k = 8, 32, 5
    enc = nn.Linear(in_dim, dict_dim)
    art = tmp_path / "enc.pt"
    torch.save({"state_dict": enc.state_dict(),
                "meta": {"in_dim": in_dim, "dict_dim": dict_dim, "top_k": top_k,
                         "objective": "hard-neg"}}, art)

    apply, meta = _load_encoder(str(art))
    assert meta["dict_dim"] == dict_dim
    X = np.random.RandomState(0).randn(4, in_dim).astype(np.float32)
    codes = apply(X)
    assert codes.shape == (4, dict_dim)
    # each row keeps exactly top_k non-zero entries (top-k real)
    for row in codes:
        assert int((row != 0).sum()) == top_k


def test_load_encoder_zero_row_is_safe(tmp_path):
    import torch
    import torch.nn as nn

    enc = nn.Linear(4, 16)
    art = tmp_path / "enc.pt"
    torch.save({"state_dict": enc.state_dict(),
                "meta": {"in_dim": 4, "dict_dim": 16, "top_k": 3, "objective": "cosine-lin"}}, art)
    apply, _ = _load_encoder(str(art))
    codes = apply(np.zeros((1, 4), dtype=np.float32))  # zero embedding must not div-by-zero
    assert codes.shape == (1, 16) and np.isfinite(codes).all()
