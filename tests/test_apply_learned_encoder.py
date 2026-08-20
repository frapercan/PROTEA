"""Unit tests for apply_learned_encoder (payload + encoder apply math; no DB)."""
from __future__ import annotations

import uuid

import numpy as np
import pytest

from protea.core.operations.apply_learned_encoder import (
    IMPLEMENTED_ORDER,
    ApplyLearnedEncoderOperation,
    ApplyLearnedEncoderPayload,
    _load_encoder,
    refuse_wrong_order,
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
                         "objective": "hard-neg", "order": "pool-then-select", "training_release": "220"}}, art)

    apply, meta = _load_encoder(str(art))
    assert meta["dict_dim"] == dict_dim
    # apply takes a list of per-sequence chunk-vector matrices; single-chunk groups here
    X = np.random.RandomState(0).randn(4, in_dim).astype(np.float32)
    groups = [X[i : i + 1] for i in range(4)]
    codes = apply(groups)
    assert codes.shape == (4, dict_dim)
    # each row keeps exactly top_k non-zero entries (top-k real)
    for row in codes:
        assert int((row != 0).sum()) == top_k


def test_load_encoder_mean_pools_multi_chunk_groups(tmp_path):
    import torch
    import torch.nn as nn

    in_dim, dict_dim, top_k = 6, 20, 4
    enc = nn.Linear(in_dim, dict_dim)
    art = tmp_path / "enc.pt"
    torch.save({"state_dict": enc.state_dict(),
                "meta": {"in_dim": in_dim, "dict_dim": dict_dim, "top_k": top_k,
                         "order": "pool-then-select", "training_release": "220"}}, art)
    apply, _ = _load_encoder(str(art))
    rng = np.random.RandomState(1)
    multi = rng.randn(3, in_dim).astype(np.float32)
    # a 3-chunk group must equal the single-chunk group of its mean
    a = apply([multi])
    b = apply([multi.mean(axis=0, keepdims=True)])
    assert np.allclose(a, b, atol=1e-5)


def test_load_encoder_zero_row_is_safe(tmp_path):
    import torch
    import torch.nn as nn

    enc = nn.Linear(4, 16)
    art = tmp_path / "enc.pt"
    torch.save({"state_dict": enc.state_dict(),
                "meta": {"in_dim": 4, "dict_dim": 16, "top_k": 3, "objective": "cosine-lin",
                         "order": "pool-then-select", "training_release": "220"}}, art)
    apply, _ = _load_encoder(str(art))
    codes = apply([np.zeros((1, 4), dtype=np.float32)])  # zero embedding must not div-by-zero
    assert codes.shape == (1, 16) and np.isfinite(codes).all()


def _save_attn_artifact(path, in_dim, dict_dim, att_dim, heads, top_k, cap_chunks):
    import torch

    sd = {
        "W.weight": torch.randn(att_dim, in_dim),
        "W.bias": torch.randn(att_dim),
        "v.weight": torch.randn(heads, att_dim),
        "lin.weight": torch.randn(dict_dim, in_dim * heads),
        "lin.bias": torch.randn(dict_dim),
    }
    torch.save({"state_dict": sd,
                "meta": {"in_dim": in_dim, "dict_dim": dict_dim, "top_k": top_k,
                         "att_dim": att_dim, "heads": heads, "cap_chunks": cap_chunks,
                         "pooling": "attention", "objective": "hard-neg",
                         "order": "pool-then-select", "training_release": "220"}}, path)


def test_attention_apply_produces_topk_real_code(tmp_path):
    in_dim, dict_dim, att_dim, heads, top_k, cap = 6, 24, 8, 1, 5, 4
    art = tmp_path / "attn.pt"
    _save_attn_artifact(art, in_dim, dict_dim, att_dim, heads, top_k, cap)
    apply, meta = _load_encoder(str(art))
    assert meta["pooling"] == "attention"
    rng = np.random.RandomState(0)
    groups = [rng.randn(n, in_dim).astype(np.float32) for n in (1, 3, 2)]
    codes = apply(groups)
    assert codes.shape == (3, dict_dim)
    for row in codes:
        assert int((row != 0).sum()) == top_k


def test_attention_apply_truncates_to_cap(tmp_path):
    in_dim, dict_dim, att_dim, heads, top_k, cap = 5, 16, 8, 2, 4, 3
    art = tmp_path / "attn.pt"
    _save_attn_artifact(art, in_dim, dict_dim, att_dim, heads, top_k, cap)
    apply, _ = _load_encoder(str(art))
    rng = np.random.RandomState(2)
    long_group = rng.randn(10, in_dim).astype(np.float32)  # > cap chunks
    codes = apply([long_group])
    assert codes.shape == (1, dict_dim) and np.isfinite(codes).all()


# --------------------------------------------------------------------------- the order

# The distinction cannot be recovered from the weights: both orders produce the same tensor
# shapes and declare the same in_dim and dict_dim. An artifact fitted for per-residue
# selection runs happily through this path and yields a complete code computed the wrong
# way, sharing 12 of 128 atoms with the intended one at cosine 0.08. encode_residue_sparse
# refuses this order by name; this refuses that one, so neither is the special case.


def test_this_operation_pools_before_selecting():
    assert IMPLEMENTED_ORDER == "pool-then-select"


def test_an_artifact_that_does_not_declare_its_order_is_refused():
    """Silence is not allowed to mean this one, which is the whole point of the field."""
    with pytest.raises(ValueError, match="declares no order"):
        refuse_wrong_order({"in_dim": 8, "dict_dim": 32}, "e.pt")


def test_the_other_order_is_refused_and_the_message_says_where_it_belongs():
    with pytest.raises(ValueError, match="encode_residue_sparse"):
        refuse_wrong_order({"order": "select-then-pool"}, "e.pt")


def test_an_unknown_order_is_refused_rather_than_guessed():
    with pytest.raises(ValueError, match="not one of"):
        refuse_wrong_order({"order": "whichever"}, "e.pt")


def test_the_implemented_order_with_a_declared_cut_passes():
    refuse_wrong_order({"order": IMPLEMENTED_ORDER, "training_release": "220"}, "e.pt")


def test_a_select_then_pool_artifact_cannot_be_loaded_here(tmp_path):
    """Through the loader, not only through the helper.

    A test on the helper alone passes while the loader never calls it, which is the failure
    mode of a suite that inspects rather than executes.
    """
    import torch
    import torch.nn as nn

    art = tmp_path / "wrong-order.pt"
    torch.save({"state_dict": nn.Linear(8, 32).state_dict(),
                "meta": {"in_dim": 8, "dict_dim": 32, "top_k": 5,
                         "order": "select-then-pool"}}, art)

    with pytest.raises(ValueError, match="encode_residue_sparse"):
        _load_encoder(str(art))


# ------------------------------------------------------------ addressing the artifact

# This operation runs inline rather than fanning out, so it runs wherever the operations
# queue is consumed, which is not the machine an artifact fitted on the compute node was
# written to. A local path cannot mean the same thing on both; the store address can.


def test_a_uri_alone_is_accepted():
    p = ApplyLearnedEncoderPayload(
        source_embedding_config_id="cfg", encoder_artifact_uri="encoders/e.pt"
    )

    assert p.encoder_artifact_uri == "encoders/e.pt"
    assert p.encoder_artifact_path is None


def test_both_addresses_are_refused():
    """Two addresses can disagree and nothing downstream could say which was meant."""
    with pytest.raises(ValueError, match="exactly one"):
        ApplyLearnedEncoderPayload(
            source_embedding_config_id="cfg",
            encoder_artifact_path="/tmp/e.pt",
            encoder_artifact_uri="encoders/e.pt",
        )


def test_neither_address_is_refused():
    with pytest.raises(ValueError, match="exactly one"):
        ApplyLearnedEncoderPayload(source_embedding_config_id="cfg")


def test_the_summary_names_the_artifact_whichever_address_carried_it():
    """It used to read the path only, so a uri dispatch summarised as having none."""
    op = ApplyLearnedEncoderOperation()

    assert "e.pt" in op.summarize_payload(
        {"source_embedding_config_id": "cfg", "encoder_artifact_uri": "encoders/e.pt"}
    )
    assert "e.pt" in op.summarize_payload(
        {"source_embedding_config_id": "cfg", "encoder_artifact_path": "/tmp/e.pt"}
    )


# ------------------------------------------------------------------- the training cut


def test_an_artifact_that_does_not_say_when_it_was_fitted_is_refused():
    """NULL in that column means NOT FITTED, so silence would claim something false."""
    with pytest.raises(ValueError, match="training_release"):
        refuse_wrong_order({"order": IMPLEMENTED_ORDER}, "e.pt")


def test_the_order_is_checked_before_the_cut():
    """A wrong-order artifact hears about the order, not about a field it also lacks."""
    with pytest.raises(ValueError, match="encode_residue_sparse"):
        refuse_wrong_order({"order": "select-then-pool"}, "e.pt")
