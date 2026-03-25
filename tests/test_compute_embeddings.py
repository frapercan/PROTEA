from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from protea.core.operations.compute_embeddings import (
    ChunkEmbedding,
    ComputeEmbeddingsBatchOperation,
    ComputeEmbeddingsOperation,
    ComputeEmbeddingsPayload,
    StoreEmbeddingsOperation,
    _aggregate_1d,
    _aggregate_residue_layers,
    _chunk_and_pool,
    _compute_chunk_spans,
    _validate_layers,
)
from protea.infrastructure.orm.models.job import JobStatus

_noop_emit = lambda *_: None  # noqa: E731


def _mock_config(
    config_id=None,
    *,
    backend="esm",
    layer_indices=None,
    layer_agg="mean",
    pooling="mean",
    normalize_residues=False,
    normalize=True,
    use_chunking=False,
    chunk_size=512,
    chunk_overlap=0,
):
    cfg = MagicMock()
    cfg.id = config_id or uuid.uuid4()
    cfg.model_name = "facebook/esm2_t6_8M_UR50D"
    cfg.model_backend = backend
    cfg.layer_indices = layer_indices if layer_indices is not None else [0]
    cfg.layer_agg = layer_agg
    cfg.pooling = pooling
    cfg.normalize_residues = normalize_residues
    cfg.normalize = normalize
    cfg.max_length = 1022
    cfg.use_chunking = use_chunking
    cfg.chunk_size = chunk_size
    cfg.chunk_overlap = chunk_overlap
    return cfg


# ---------------------------------------------------------------------------
# Payload validation
# ---------------------------------------------------------------------------

class TestComputeEmbeddingsPayload:
    def test_minimal_valid(self) -> None:
        cid = str(uuid.uuid4())
        p = ComputeEmbeddingsPayload.model_validate({"embedding_config_id": cid})
        assert p.embedding_config_id == cid
        assert p.batch_size == 8
        assert p.skip_existing is True
        assert p.device == "cuda"

    def test_empty_embedding_config_id_raises(self) -> None:
        with pytest.raises(ValueError):
            ComputeEmbeddingsPayload.model_validate({"embedding_config_id": ""})

    def test_whitespace_embedding_config_id_raises(self) -> None:
        with pytest.raises(ValueError):
            ComputeEmbeddingsPayload.model_validate({"embedding_config_id": "   "})

    def test_optional_fields_override(self) -> None:
        p = ComputeEmbeddingsPayload.model_validate({
            "embedding_config_id": str(uuid.uuid4()),
            "batch_size": 16,
            "skip_existing": False,
            "device": "cuda",
        })
        assert p.batch_size == 16
        assert p.skip_existing is False
        assert p.device == "cuda"


# ---------------------------------------------------------------------------
# _validate_layers
# ---------------------------------------------------------------------------

class TestValidateLayers:
    def test_valid_reverse_index(self) -> None:
        # 7 layers → indices 0..6 are valid (0=last)
        hidden = [None] * 7
        result = _validate_layers([0, 2], hidden, "ESM", "seq1")
        assert result == [0, 2]

    def test_invalid_index_raises(self) -> None:
        hidden = [None] * 4
        with pytest.raises(ValueError, match="invalid layer_indices"):
            _validate_layers([4], hidden, "ESM", "seq1")

    def test_deduplicates_and_sorts(self) -> None:
        hidden = [None] * 10
        result = _validate_layers([2, 0, 2, 1], hidden, "T5", "seq")
        assert result == [0, 1, 2]


# ---------------------------------------------------------------------------
# _aggregate helpers
# ---------------------------------------------------------------------------

class TestAggregateResidues:
    def test_mean(self) -> None:
        import torch
        a = torch.ones(4, 8)
        b = torch.ones(4, 8) * 3
        result = _aggregate_residue_layers([a, b], "mean")
        assert result.shape == (4, 8)
        assert float(result[0, 0]) == pytest.approx(2.0)

    def test_concat(self) -> None:
        import torch
        a = torch.ones(4, 8)
        b = torch.ones(4, 8)
        result = _aggregate_residue_layers([a, b], "concat")
        assert result.shape == (4, 16)

    def test_unknown_raises(self) -> None:
        import torch
        with pytest.raises(ValueError):
            _aggregate_residue_layers([torch.zeros(4, 8)], "unknown")


class TestAggregate1d:
    def test_mean(self) -> None:
        import torch
        a = torch.ones(8)
        b = torch.ones(8) * 3
        result = _aggregate_1d([a, b], "mean")
        assert float(result[0]) == pytest.approx(2.0)

    def test_concat(self) -> None:
        import torch
        a = torch.ones(8)
        b = torch.ones(8)
        result = _aggregate_1d([a, b], "concat")
        assert result.shape == (16,)

    def test_unknown_raises(self) -> None:
        import torch
        with pytest.raises(ValueError):
            _aggregate_1d([torch.zeros(8)], "unknown")


# ---------------------------------------------------------------------------
# _compute_chunk_spans
# ---------------------------------------------------------------------------

class TestComputeChunkSpans:
    def test_no_overlap(self) -> None:
        spans = _compute_chunk_spans(10, 4, 0)
        assert spans == [(0, 4), (4, 8), (8, 10)]

    def test_with_overlap(self) -> None:
        spans = _compute_chunk_spans(10, 6, 2)
        # step = 4: (0,6), (4,10), (8,10)
        assert spans == [(0, 6), (4, 10), (8, 10)]

    def test_shorter_than_chunk_size(self) -> None:
        spans = _compute_chunk_spans(3, 10, 0)
        assert spans == [(0, 3)]

    def test_overlap_equal_to_chunk_size_raises(self) -> None:
        with pytest.raises(ValueError, match="chunk_overlap"):
            _compute_chunk_spans(10, 5, 5)

    def test_overlap_greater_than_chunk_size_raises(self) -> None:
        with pytest.raises(ValueError, match="chunk_overlap"):
            _compute_chunk_spans(10, 4, 6)


# ---------------------------------------------------------------------------
# _chunk_and_pool
# ---------------------------------------------------------------------------

class TestChunkAndPool:
    def test_no_chunking_mean(self) -> None:
        import torch
        cfg = _mock_config(pooling="mean", normalize=False)
        residues = torch.ones(5, 8)
        chunks = _chunk_and_pool(residues, cfg)
        assert len(chunks) == 1
        assert chunks[0].chunk_index_s == 0
        assert chunks[0].chunk_index_e is None
        assert chunks[0].vector.shape == (8,)

    def test_chunking_produces_multiple_results(self) -> None:
        import torch
        cfg = _mock_config(
            pooling="mean", normalize=False,
            use_chunking=True, chunk_size=4, chunk_overlap=0,
        )
        residues = torch.ones(10, 8)
        chunks = _chunk_and_pool(residues, cfg)
        assert len(chunks) == 3  # (0,4), (4,8), (8,10)
        assert chunks[0].chunk_index_e == 4
        assert chunks[-1].chunk_index_s == 8

    def test_mean_max_doubles_dim(self) -> None:
        import torch
        cfg = _mock_config(pooling="mean_max", normalize=False)
        residues = torch.ones(5, 8)
        chunks = _chunk_and_pool(residues, cfg)
        assert chunks[0].vector.shape == (16,)

    def test_normalize_produces_unit_norm(self) -> None:
        import torch
        cfg = _mock_config(pooling="mean", normalize=True)
        residues = torch.rand(5, 8) + 0.1
        chunks = _chunk_and_pool(residues, cfg)
        norm = np.linalg.norm(chunks[0].vector)
        assert abs(norm - 1.0) < 1e-5


# ---------------------------------------------------------------------------
# Special-token stripping (ESM and ESM3c)
# ---------------------------------------------------------------------------

class TestSpecialTokenStripping:
    """Verify that CLS / EOS / BOS tokens are excluded from residue pooling.

    Each test constructs a hidden-state tensor where:
      - position 0 (CLS/BOS): all zeros
      - positions 1..L-2 (content): all ones
      - position -1 (EOS): all tens

    With mean pooling and no normalisation the output must be ≈ 1.0 per dim,
    proving that only the content tokens contributed.
    """

    def _op(self) -> ComputeEmbeddingsOperation:
        return ComputeEmbeddingsOperation()

    def test_esm_strips_cls_and_eos(self) -> None:
        import torch

        op = self._op()
        cfg = _mock_config(layer_indices=[0], layer_agg="mean", pooling="mean", normalize=False)

        dim = 8
        seq_len = 7  # CLS + 5 content + EOS

        hidden = torch.zeros(1, seq_len, dim)
        hidden[0, 1:6, :] = 1.0   # content (positions 1–5)
        hidden[0, 6, :] = 10.0    # EOS at position 6 — must be excluded

        tokens_dict = {
            "input_ids": torch.zeros(1, seq_len, dtype=torch.long),
            "attention_mask": torch.ones(1, seq_len, dtype=torch.long),
        }

        mock_outputs = MagicMock()
        mock_outputs.hidden_states = [hidden]  # 1 layer → index 0 = last = first

        mock_model = MagicMock()
        mock_model.return_value = mock_outputs
        mock_tokenizer = MagicMock()
        mock_tokenizer.return_value = tokens_dict

        with patch("torch.no_grad"):
            result = op._embed_batch(mock_model, mock_tokenizer, ["ACDEF"], cfg, "cpu")

        vec = result[0][0].vector
        # Mean of 5 content tokens (all 1.0) — CLS (0) and EOS (10) excluded
        assert vec == pytest.approx([1.0] * dim, abs=1e-5)

    def test_esm_residue_count_matches_content_only(self) -> None:
        """Output dim and chunk boundaries reflect content tokens, not full seq_len."""
        import torch

        op = self._op()
        cfg = _mock_config(
            layer_indices=[0], pooling="mean", normalize=False,
            use_chunking=True, chunk_size=3, chunk_overlap=0,
        )

        dim = 4
        seq_len = 7  # CLS + 5 content + EOS

        hidden = torch.ones(1, seq_len, dim)
        hidden[0, 0, :] = 99.0   # CLS — must be excluded
        hidden[0, 6, :] = 99.0   # EOS — must be excluded

        tokens_dict = {
            "input_ids": torch.zeros(1, seq_len, dtype=torch.long),
            "attention_mask": torch.ones(1, seq_len, dtype=torch.long),
        }

        mock_outputs = MagicMock()
        mock_outputs.hidden_states = [hidden]

        mock_model = MagicMock()
        mock_model.return_value = mock_outputs
        mock_tokenizer = MagicMock()
        mock_tokenizer.return_value = tokens_dict

        with patch("torch.no_grad"):
            result = op._embed_batch(mock_model, mock_tokenizer, ["ACDEF"], cfg, "cpu")

        chunks = result[0]
        # 5 content tokens, chunk_size=3 → chunks (0,3) and (3,5)
        assert len(chunks) == 2
        assert chunks[0].chunk_index_e == 3
        assert chunks[1].chunk_index_s == 3
        # All content values are 1.0 — CLS/EOS (99) must not appear in mean
        for c in chunks:
            assert c.vector == pytest.approx([1.0] * dim, abs=1e-5)

    def test_esm3c_strips_bos_and_eos(self) -> None:
        """ESM3c [1:-1] slicing excludes BOS and EOS from residue pooling."""
        import sys

        import torch

        from protea.core.operations.compute_embeddings import _embed_esm3c

        dim = 8
        seq_len = 7  # BOS + 5 content + EOS

        layer = torch.zeros(1, seq_len, dim)
        layer[0, 1:6, :] = 1.0   # content
        layer[0, 6, :] = 10.0    # EOS — must be excluded

        cfg = _mock_config(layer_indices=[0], layer_agg="mean", pooling="mean", normalize=False)

        class FakeLogitsOutput:
            hidden_states = [layer]

        class FakeModel:
            def encode(self, protein):
                return object()

            def logits(self, tensor, lc):
                return FakeLogitsOutput()

        # Patch the ESM SDK so the import inside _embed_esm3c resolves
        esm_api_mock = MagicMock()
        esm_mock = MagicMock()
        with patch.dict(sys.modules, {
            "esm": esm_mock,
            "esm.sdk": esm_mock,
            "esm.sdk.api": esm_api_mock,
        }):
            result = _embed_esm3c(FakeModel(), ["ACDEF"], cfg, "cpu")

        vec = result[0][0].vector
        # Mean of 5 content tokens (1.0) — BOS (0) and EOS (10) excluded
        assert vec == pytest.approx([1.0] * dim, abs=1e-5)

    def test_t5_includes_eos_token(self) -> None:
        """T5 keeps EOS in the residue tensor (PIS convention)."""
        import torch

        from protea.core.operations.compute_embeddings import _embed_t5

        dim = 8
        # T5: 4 content tokens + EOS = 5 valid tokens; 3 padding tokens
        batch_len = 8
        actual_len = 5

        hidden = torch.zeros(1, batch_len, dim)
        hidden[0, :actual_len, :] = 2.0   # valid tokens (content + EOS)
        # padding positions remain 0.0

        cfg = _mock_config(
            layer_indices=[0], layer_agg="mean", pooling="mean", normalize=False
        )
        cfg.model_name = "Rostlab/prot_t5_xl_uniref50"  # not prostt5

        mock_outputs = MagicMock()
        mock_outputs.hidden_states = [hidden]

        mock_model = MagicMock()
        mock_model.return_value = mock_outputs

        attention_mask = torch.zeros(1, batch_len, dtype=torch.long)
        attention_mask[0, :actual_len] = 1

        mock_tokenizer = MagicMock()
        mock_tokenizer.batch_encode_plus.return_value = {
            "input_ids": torch.zeros(1, batch_len, dtype=torch.long),
            "attention_mask": attention_mask,
        }

        with patch("torch.no_grad"):
            result = _embed_t5(mock_model, mock_tokenizer, ["ACDE"], cfg, "cpu")

        vec = result[0][0].vector
        # Mean of actual_len=5 tokens (all 2.0) including EOS, excluding padding (0)
        assert vec == pytest.approx([2.0] * dim, abs=1e-5)


# ---------------------------------------------------------------------------
# _embed_batch dispatch (mocked model)
# ---------------------------------------------------------------------------

class TestEmbedBatch:
    def _op(self) -> ComputeEmbeddingsOperation:
        return ComputeEmbeddingsOperation()

    def test_returns_one_list_per_sequence(self) -> None:
        import torch

        op = self._op()
        cfg = _mock_config(layer_indices=[0], layer_agg="mean", pooling="mean", normalize=False)

        dim = 320
        seq_len = 7  # CLS + 5 content + EOS

        # Tokenizer returns a real dict so that .items() and dict lookup work
        tokens_dict = {
            "input_ids": torch.zeros(1, seq_len, dtype=torch.long),
            "attention_mask": torch.ones(1, seq_len, dtype=torch.long),
        }

        hidden = torch.randn(1, seq_len, dim)
        mock_outputs = MagicMock()
        mock_outputs.hidden_states = [hidden] * 7  # 7 layers; index 0 = last

        mock_model = MagicMock()
        mock_model.return_value = mock_outputs
        mock_tokenizer = MagicMock()
        mock_tokenizer.return_value = tokens_dict

        with patch("torch.no_grad"):
            result = op._embed_batch(mock_model, mock_tokenizer, ["ACDEF", "FGHIK"], cfg, "cpu")

        assert len(result) == 2
        assert all(isinstance(chunks, list) for chunks in result)
        assert all(isinstance(c, ChunkEmbedding) for chunks in result for c in chunks)

    def test_normalized_vectors_have_unit_norm(self) -> None:
        import torch

        op = self._op()
        cfg = _mock_config(layer_indices=[0], pooling="mean", normalize=True)

        dim = 64
        seq_len = 5  # CLS + 3 content + EOS

        tokens_dict = {
            "input_ids": torch.zeros(1, seq_len, dtype=torch.long),
            "attention_mask": torch.ones(1, seq_len, dtype=torch.long),
        }

        hidden = torch.rand(1, seq_len, dim) + 0.1
        mock_outputs = MagicMock()
        mock_outputs.hidden_states = [hidden] * 3

        mock_model = MagicMock()
        mock_model.return_value = mock_outputs
        mock_tokenizer = MagicMock()
        mock_tokenizer.return_value = tokens_dict

        with patch("torch.no_grad"):
            result = op._embed_batch(mock_model, mock_tokenizer, ["ACD"], cfg, "cpu")

        vec = result[0][0].vector
        norm = np.linalg.norm(vec)
        assert abs(norm - 1.0) < 1e-5, f"Expected unit norm, got {norm}"


# ---------------------------------------------------------------------------
# execute() — coordinator (ComputeEmbeddingsOperation)
# ---------------------------------------------------------------------------

class TestComputeEmbeddingsCoordinator:
    """Tests for the coordinator operation that dispatches child batch jobs."""

    def _op(self) -> ComputeEmbeddingsOperation:
        return ComputeEmbeddingsOperation()

    def _make_session(self, config, seq_ids: list[int]):
        session = MagicMock()
        session.get.return_value = config
        # _load_sequence_ids returns a list of ints via q.all()
        rows = [(sid,) for sid in seq_ids]
        session.query.return_value.filter.return_value.all.return_value = rows
        session.query.return_value.all.return_value = rows
        # GPU mutex: no other running compute_embeddings job
        session.query.return_value.filter.return_value.first.return_value = None
        return session

    def test_no_sequences_returns_zero_child_jobs(self) -> None:
        op = self._op()
        cfg = _mock_config()
        session = self._make_session(cfg, seq_ids=[])

        with patch.object(op, "_load_sequence_ids", return_value=[]):
            result = op.execute(
                session,
                {"embedding_config_id": str(cfg.id), "_job_id": str(uuid.uuid4())},
                emit=_noop_emit,
            )

        assert result.result["batches"] == 0
        assert result.deferred is False

    def test_dispatches_correct_number_of_child_jobs(self) -> None:
        op = self._op()
        cfg = _mock_config()
        job_id = str(uuid.uuid4())
        seq_ids = list(range(10))  # 10 sequences
        session = MagicMock()
        session.get.return_value = cfg
        # GPU mutex: no other running compute_embeddings job
        session.query.return_value.filter.return_value.first.return_value = None

        with patch.object(op, "_load_sequence_ids", return_value=seq_ids):
            result = op.execute(
                session,
                {"embedding_config_id": str(cfg.id), "_job_id": job_id, "sequences_per_job": 4},
                emit=_noop_emit,
            )

        # ceil(10/4) = 3 batches
        assert result.result["batches"] == 3
        assert result.result["sequences"] == 10
        assert result.progress_total == 3
        assert result.deferred is True
        assert len(result.publish_operations) == 3


# ---------------------------------------------------------------------------
# execute() — batch operation (ComputeEmbeddingsBatchOperation)
# ---------------------------------------------------------------------------

class TestComputeEmbeddingsBatchExecute:
    def _op(self) -> ComputeEmbeddingsBatchOperation:
        return ComputeEmbeddingsBatchOperation()

    def _make_sequence(self, seq_id: int, sequence: str = "ACDEFGHIK"):
        s = MagicMock()
        s.id = seq_id
        s.sequence = sequence
        return s

    def _make_session(self, config, sequences, no_existing=True):
        session = MagicMock()
        session.get.return_value = config
        session.query.return_value.filter.return_value.all.return_value = sequences
        if no_existing:
            session.query.return_value.filter_by.return_value.first.return_value = None
        else:
            session.query.return_value.filter_by.return_value.first.return_value = MagicMock()
        # Atomic parent progress update
        row = MagicMock()
        row.progress_current = 1
        row.progress_total = 2
        session.execute.return_value.fetchone.return_value = row
        return session

    def _fake_chunks(self, vec: np.ndarray) -> list[ChunkEmbedding]:
        return [ChunkEmbedding(chunk_index_s=0, chunk_index_e=None, vector=vec)]

    def _base_payload(self, cfg) -> dict:
        return {
            "embedding_config_id": str(cfg.id),
            "sequence_ids": [1, 2],
            "parent_job_id": str(uuid.uuid4()),
            "_job_id": str(uuid.uuid4()),
        }

    def test_inference_publishes_write_operation(self) -> None:
        op = self._op()
        cfg = _mock_config()
        seqs = [self._make_sequence(1, "ACDEF"), self._make_sequence(2, "GHIKL")]
        session = self._make_session(cfg, seqs)

        fake_vec = np.array([0.1, 0.2, 0.3], dtype=np.float32)
        fake_batch = [self._fake_chunks(fake_vec), self._fake_chunks(fake_vec)]

        with patch.object(op, "_load_model", return_value=(MagicMock(), MagicMock())), \
             patch.object(op, "_embed_batch", return_value=fake_batch):
            result = op.execute(session, self._base_payload(cfg), emit=_noop_emit)

        assert result.result["sequences_inferred"] == 2
        assert len(result.publish_operations) == 1
        queue, msg = result.publish_operations[0]
        assert queue == "protea.embeddings.write"
        assert msg["operation"] == "store_embeddings"
        assert len(msg["payload"]["sequences"]) == 2

    def test_chunking_serializes_all_chunks(self) -> None:
        op = self._op()
        cfg = _mock_config(use_chunking=True)
        seqs = [self._make_sequence(1, "ACDEF")]
        session = self._make_session(cfg, seqs)

        fake_vec = np.array([0.1, 0.2, 0.3], dtype=np.float32)
        three_chunks = [
            ChunkEmbedding(0, 4, fake_vec),
            ChunkEmbedding(4, 8, fake_vec),
            ChunkEmbedding(8, 10, fake_vec),
        ]

        with patch.object(op, "_load_model", return_value=(MagicMock(), MagicMock())), \
             patch.object(op, "_embed_batch", return_value=[three_chunks]):
            result = op.execute(session, self._base_payload(cfg), emit=_noop_emit)

        _, msg = result.publish_operations[0]
        assert len(msg["payload"]["sequences"][0]["chunks"]) == 3


# ---------------------------------------------------------------------------
# Batch-size consistency
# ---------------------------------------------------------------------------

@pytest.mark.slow
class TestBatchSizeConsistency:
    """Embeddings must be numerically identical regardless of batch grouping.

    ESM test loads real facebook/esm2_t6_8M_UR50D weights (~30 MB).
    T5 test uses a synthetic model to avoid downloading multi-GB weights.

    Run with: pytest -m slow
    """

    SEQUENCES = ["ACDEF", "GHIKL", "MNPQR", "STVWY"]

    def _esm_cfg(self):
        return _mock_config(
            layer_indices=[0], layer_agg="mean",
            pooling="mean", normalize=False, normalize_residues=False,
        )

    def test_esm_batch_size_consistency(self):
        """ESM embeddings must be bit-exact for batch_size 1, 2, and 4."""
        from transformers import AutoTokenizer, EsmModel

        from protea.core.operations.compute_embeddings import _embed_esm

        cfg = self._esm_cfg()
        tokenizer = AutoTokenizer.from_pretrained(cfg.model_name)
        model = EsmModel.from_pretrained(cfg.model_name, output_hidden_states=True)
        model.eval()

        # Reference: one sequence at a time
        ref = [_embed_esm(model, tokenizer, [s], cfg, "cpu")[0] for s in self.SEQUENCES]

        for batch_size in (2, 4):
            batched = []
            for i in range(0, len(self.SEQUENCES), batch_size):
                batched.extend(
                    _embed_esm(model, tokenizer, self.SEQUENCES[i:i + batch_size], cfg, "cpu")
                )

            for i, (got, expected) in enumerate(zip(batched, ref, strict=False)):
                np.testing.assert_allclose(
                    got[0].vector, expected[0].vector, rtol=1e-5, atol=1e-6,
                    err_msg=f"ESM batch_size={batch_size}: mismatch at sequence {i}",
                )

    def test_t5_padding_does_not_affect_embeddings(self):
        """Padding in T5 batches must not influence valid-position embeddings.

        Uses a synthetic model so the test runs without downloading large weights.
        The fake model sets hidden_states[i, j, :] = input_ids[i, j], so any
        leakage from padding positions (which have input_ids=0) into valid
        positions would produce wrong values and fail the assertion.
        """
        import torch

        from protea.core.operations.compute_embeddings import _embed_t5

        cfg = _mock_config(
            backend="t5", layer_indices=[0], layer_agg="mean",
            pooling="mean", normalize=False, normalize_residues=False,
        )
        cfg.model_name = "Rostlab/prot_t5_xl_uniref50"  # non-prostt5 path

        class _FakeT5:
            def __call__(self, input_ids, attention_mask, output_hidden_states=True):
                B, L = input_ids.shape
                hs = input_ids.float().unsqueeze(-1).expand(B, L, 8).clone()
                out = MagicMock()
                out.hidden_states = (hs,)
                return out

        class _FakeTokenizer:
            def batch_encode_plus(self, seqs, **kwargs):
                # seqs are already space-separated by _embed_t5; strip spaces
                encoded = [
                    [ord(c) % 100 + 2 for c in s if c != " "] + [1]  # +EOS
                    for s in seqs
                ]
                max_len = max(len(e) for e in encoded)
                B = len(seqs)
                input_ids = torch.zeros(B, max_len, dtype=torch.long)
                attention_mask = torch.zeros(B, max_len, dtype=torch.long)
                for i, enc in enumerate(encoded):
                    input_ids[i, :len(enc)] = torch.tensor(enc)
                    attention_mask[i, :len(enc)] = 1
                return {"input_ids": input_ids, "attention_mask": attention_mask}

        model = _FakeT5()
        tokenizer = _FakeTokenizer()

        # Reference: one sequence at a time
        ref = [_embed_t5(model, tokenizer, [s], cfg, "cpu")[0][0].vector for s in self.SEQUENCES]

        for batch_size in (2, 4):
            results = []
            for i in range(0, len(self.SEQUENCES), batch_size):
                for r in _embed_t5(model, tokenizer, self.SEQUENCES[i:i + batch_size], cfg, "cpu"):
                    results.append(r[0].vector)

            for i in range(len(self.SEQUENCES)):
                np.testing.assert_allclose(
                    results[i], ref[i], rtol=1e-5, atol=1e-6,
                    err_msg=f"T5 batch_size={batch_size}: mismatch at sequence {i}",
                )


# ---------------------------------------------------------------------------
# StoreEmbeddingsOperation
# ---------------------------------------------------------------------------

class TestStoreEmbeddingsOperation:
    def _op(self) -> StoreEmbeddingsOperation:
        return StoreEmbeddingsOperation()

    def _make_payload(self, n_sequences=2, skip_existing=True, **kw):
        sequences = []
        for i in range(n_sequences):
            sequences.append({
                "sequence_id": i + 1,
                "chunks": [{
                    "chunk_index_s": 0,
                    "chunk_index_e": None,
                    "vector": [0.1, 0.2, 0.3],
                    "embedding_dim": 3,
                }],
            })
        defaults = {
            "parent_job_id": str(uuid.uuid4()),
            "embedding_config_id": str(uuid.uuid4()),
            "skip_existing": skip_existing,
            "sequences": sequences,
        }
        defaults.update(kw)
        return defaults

    def test_stores_embeddings(self) -> None:
        op = self._op()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.RUNNING
        session.get.return_value = parent
        # No existing embeddings
        session.query.return_value.filter_by.return_value.first.return_value = None
        # Progress update
        row = MagicMock()
        row.progress_current = 1
        row.progress_total = 5
        session.execute.return_value.fetchone.return_value = row

        result = op.execute(session, self._make_payload(), emit=_noop_emit)
        assert result.result["embeddings_stored"] == 2
        assert result.result["sequences_skipped"] == 0

    def test_skips_existing_when_enabled(self) -> None:
        op = self._op()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.RUNNING
        session.get.return_value = parent
        # All existing
        session.query.return_value.filter_by.return_value.first.return_value = MagicMock()
        row = MagicMock()
        row.progress_current = 1
        row.progress_total = 5
        session.execute.return_value.fetchone.return_value = row

        result = op.execute(session, self._make_payload(skip_existing=True), emit=_noop_emit)
        assert result.result["sequences_skipped"] == 2
        assert result.result["embeddings_stored"] == 0

    def test_deletes_existing_when_skip_disabled(self) -> None:
        op = self._op()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.RUNNING
        session.get.return_value = parent
        row = MagicMock()
        row.progress_current = 1
        row.progress_total = 5
        session.execute.return_value.fetchone.return_value = row

        result = op.execute(session, self._make_payload(skip_existing=False), emit=_noop_emit)
        assert result.result["embeddings_stored"] == 2
        # Should have called delete on existing rows
        assert session.query.return_value.filter_by.return_value.delete.called

    def test_skips_when_parent_cancelled(self) -> None:
        op = self._op()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.CANCELLED
        session.get.return_value = parent

        result = op.execute(session, self._make_payload(), emit=_noop_emit)
        assert result.result["skipped"] is True

    def test_skips_when_parent_failed(self) -> None:
        op = self._op()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.FAILED
        session.get.return_value = parent

        result = op.execute(session, self._make_payload(), emit=_noop_emit)
        assert result.result["skipped"] is True

    def test_last_batch_closes_parent(self) -> None:
        op = self._op()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.RUNNING
        session.get.return_value = parent
        session.query.return_value.filter_by.return_value.first.return_value = None

        progress_row = MagicMock()
        progress_row.progress_current = 3
        progress_row.progress_total = 3
        closed_row = MagicMock()
        closed_row.id = uuid.uuid4()
        session.execute.return_value.fetchone.side_effect = [progress_row, closed_row]

        events = []
        def capture_emit(event, msg, fields, level):
            events.append(event)

        op.execute(session, self._make_payload(n_sequences=1), emit=capture_emit)
        assert "store_embeddings.parent_succeeded" in events

    def test_multiple_chunks_per_sequence(self) -> None:
        op = self._op()
        session = MagicMock()
        parent = MagicMock()
        parent.status = JobStatus.RUNNING
        session.get.return_value = parent
        session.query.return_value.filter_by.return_value.first.return_value = None
        row = MagicMock()
        row.progress_current = 1
        row.progress_total = 5
        session.execute.return_value.fetchone.return_value = row

        payload = self._make_payload(n_sequences=0)
        payload["sequences"] = [{
            "sequence_id": 1,
            "chunks": [
                {"chunk_index_s": 0, "chunk_index_e": 4, "vector": [0.1], "embedding_dim": 1},
                {"chunk_index_s": 4, "chunk_index_e": 8, "vector": [0.2], "embedding_dim": 1},
                {"chunk_index_s": 8, "chunk_index_e": 10, "vector": [0.3], "embedding_dim": 1},
            ],
        }]

        result = op.execute(session, payload, emit=_noop_emit)
        assert result.result["embeddings_stored"] == 3

    def test_name(self) -> None:
        assert StoreEmbeddingsOperation().name == "store_embeddings"


# ---------------------------------------------------------------------------
# Coordinator — GPU retry (RetryLaterError)
# ---------------------------------------------------------------------------

class TestComputeEmbeddingsRetryLogic:
    def _op(self) -> ComputeEmbeddingsOperation:
        return ComputeEmbeddingsOperation()

    def test_gpu_busy_raises_retry_later(self) -> None:
        from protea.core.contracts.operation import RetryLaterError

        op = self._op()
        cfg = _mock_config()
        session = MagicMock()
        session.get.return_value = cfg

        # Simulate another running compute_embeddings job (GPU mutex)
        other_job = MagicMock()
        session.query.return_value.filter.return_value.first.return_value = other_job

        payload = {
            "embedding_config_id": str(cfg.id),
            "_job_id": str(uuid.uuid4()),
        }

        with patch.object(op, "_load_sequence_ids", return_value=[1, 2, 3]):
            with pytest.raises(RetryLaterError):
                op.execute(session, payload, emit=_noop_emit)
