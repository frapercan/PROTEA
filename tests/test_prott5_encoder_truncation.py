"""Regression test: the ProtT5 query encoder must truncate to ``max_length``.

Without truncation the encoder OOMs on long proteins (4000+ residues do not
fit on a 12 GB GPU) and, worse, produces full-length vectors that are
inconsistent with the reference bundle, which was embedded with
``max_length=2048`` (``use_chunking=False``). This guards that
``batch_encode_plus`` receives ``truncation=True`` + ``max_length`` and that
pooling never runs past the truncated hidden state.

Skips cleanly when torch / transformers are not installed (the rest of the
method-runtime suite is deliberately torch-free).
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

torch = pytest.importorskip("torch")
pytest.importorskip("transformers")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "apps" / "method_runtime"))

import prott5_encoder  # noqa: E402


def test_embed_sequences_passes_truncation_and_max_length() -> None:
    captured: dict[str, object] = {}

    fake_tokenizer = MagicMock()

    def _fake_encode(_seqs: list[str], **kwargs: object) -> dict[str, list[list[int]]]:
        captured.update(kwargs)
        return {"input_ids": [[1, 2, 3]], "attention_mask": [[1, 1, 1]]}

    fake_tokenizer.batch_encode_plus.side_effect = _fake_encode

    fake_model = MagicMock()
    fake_model.return_value.last_hidden_state = torch.zeros((1, 3, 4))

    with patch.object(
        prott5_encoder, "_load_model",
        return_value=(fake_model, fake_tokenizer, torch.device("cpu")),
    ):
        out = prott5_encoder.embed_sequences({"P12345": "M" * 5000}, max_length=2048)

    assert captured.get("truncation") is True
    assert captured.get("max_length") == 2048
    # vector produced (dim = fake hidden size 4), pooling did not crash on the
    # 5000-residue input despite the 3-token truncated hidden state.
    assert "P12345" in out
    assert out["P12345"].shape == (4,)
