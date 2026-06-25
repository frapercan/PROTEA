"""Materialise a learned-encoder code as SequenceEmbedding rows under a new EmbeddingConfig.

A learned encoder (trained offline in protea-reranker-lab) is applied here, in-platform, to every
protein that already has a source embedding. The resulting codes are stored as ordinary
``SequenceEmbedding`` rows under a fresh ``EmbeddingConfig`` (``model_backend="learned-code"``), so the
UNCHANGED ``predict_go_terms`` / KNN / reranker / ``run_cafa_evaluation`` pipeline can consume them by
pointing at the new ``embedding_config_id`` -- the cosine KNN simply runs on codes instead of dense
vectors.

Two pooling families are supported, discriminated by ``meta["pooling"]`` in the artifact:

* ``"learned"`` / ``"mean"`` (default): ``topk_real(Linear(l2_normalise(mean_pool(chunks))), top_k)``.
  The per-chunk vectors of a sequence are averaged, then a single ``Linear(d -> dict)`` projects into
  a GO-aligned top-k-real code. Artifact: ``{"state_dict": Linear, "meta": {in_dim, dict_dim, top_k}}``
  from ``protea_reranker_lab.encoder_ablation``.
* ``"attention"``: an additive multi-head attention pool *over the per-chunk vectors* (light-attention
  style) learns which chunks matter, then a ``Linear(d * heads -> dict)`` projects the pooled vector,
  followed by top-k-real. The per-chunk vectors are NOT averaged; they are fed as a padded
  ``(n, cap, d)`` tensor with a chunk-presence mask. Artifact:
  ``{"state_dict": {W, v, lin}, "meta": {in_dim, dict_dim, top_k, att_dim, heads, cap_chunks}}``
  from ``protea_reranker_lab.chunk_attn_encoder``.
"""
from __future__ import annotations

import uuid
from typing import Any

from pydantic import field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding


class ApplyLearnedEncoderPayload(ProteaPayload, frozen=True):
    source_embedding_config_id: str
    encoder_artifact_path: str
    target_model_name: str = "learned-code"
    batch_size: int = 4000
    skip_existing: bool = True
    sequence_id_limit: int | None = None  # smoke-test cap; None = all source sequences

    @field_validator("source_embedding_config_id", "encoder_artifact_path", mode="before")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


def _topk_real(z, top_k: int):
    """Keep the ``top_k`` largest-magnitude entries per row (zero the rest). ``z``: (n, dict)."""
    import numpy as np

    if top_k >= z.shape[1]:
        return z
    out = np.zeros_like(z)
    idx = np.argpartition(-np.abs(z), top_k, axis=1)[:, :top_k]
    np.put_along_axis(out, idx, np.take_along_axis(z, idx, axis=1), axis=1)
    return out


def _build_mean_apply(blob: dict, meta: dict, dev: str):
    """Mean-pool family: Linear(d -> dict) over the per-sequence mean of chunk vectors."""
    import numpy as np
    import torch
    import torch.nn as nn

    enc = nn.Linear(int(meta["in_dim"]), int(meta["dict_dim"]))
    enc.load_state_dict(blob["state_dict"])
    enc.to(dev).eval()
    top_k = int(meta["top_k"])

    def apply(chunk_groups: list) -> np.ndarray:
        """chunk_groups: list of (n_chunks_i, in_dim) arrays -> (n, dict_dim) top-k real codes."""
        mat = np.vstack([np.mean(g, axis=0) for g in chunk_groups]).astype(np.float32)
        nrm = np.linalg.norm(mat, axis=1, keepdims=True)
        nrm[nrm == 0] = 1.0
        xn = (mat / nrm).astype(np.float32)
        with torch.no_grad():
            z = enc(torch.tensor(xn, device=dev)).cpu().numpy().astype(np.float32)
        return _topk_real(z, top_k)

    return apply


def _build_attention_apply(blob: dict, meta: dict, dev: str):
    """Attention family: additive multi-head pool over per-chunk vectors, then Linear(d*h -> dict)."""
    import numpy as np
    import torch
    import torch.nn as nn

    in_dim = int(meta["in_dim"])
    dict_dim = int(meta["dict_dim"])
    att_dim = int(meta["att_dim"])
    heads = int(meta["heads"])
    cap = int(meta.get("cap_chunks", 16))
    top_k = int(meta["top_k"])

    class AttnPoolChunkEncoder(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.heads = heads
            self.W = nn.Linear(in_dim, att_dim)
            self.v = nn.Linear(att_dim, heads, bias=False)
            self.lin = nn.Linear(in_dim * heads, dict_dim)

        def forward(self, M: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
            s = self.v(torch.tanh(self.W(M)))
            s = s.masked_fill(~mask.unsqueeze(-1), -1e9)
            a = torch.softmax(s, dim=1)
            pooled = torch.einsum("bch,bcd->bhd", a, M)
            return self.lin(pooled.reshape(M.shape[0], -1))

    enc = AttnPoolChunkEncoder()
    enc.load_state_dict(blob["state_dict"])
    enc.to(dev).eval()

    def apply(chunk_groups: list) -> np.ndarray:
        """chunk_groups: list of (n_chunks_i, in_dim) arrays -> (n, dict_dim) top-k real codes.

        Each group is truncated/padded to ``cap_chunks`` and weighted by learned attention; chunk
        order is the source ``chunk_index_s`` order the caller provides.
        """
        n = len(chunk_groups)
        Xpad = np.zeros((n, cap, in_dim), np.float32)
        mask = np.zeros((n, cap), bool)
        for i, g in enumerate(chunk_groups):
            m = np.asarray(g, dtype=np.float32)[:cap]
            Xpad[i, : m.shape[0]] = m
            mask[i, : m.shape[0]] = True
        with torch.no_grad():
            z = enc(
                torch.tensor(Xpad, device=dev), torch.tensor(mask, device=dev)
            ).cpu().numpy().astype(np.float32)
        return _topk_real(z, top_k)

    return apply


def _load_encoder(artifact_path: str):
    """Load the torch artifact -> (apply_fn, meta). Lazy-imports torch/numpy.

    ``apply_fn`` takes a list of per-sequence chunk-vector matrices (each ``(n_chunks, in_dim)``) and
    returns ``(n, dict_dim)`` top-k real codes, so the caller groups source chunks identically for
    both pooling families. ``meta["pooling"]`` selects mean-pool (default) vs attention-pool.
    """
    import torch

    blob = torch.load(artifact_path, map_location="cpu", weights_only=False)
    meta = blob["meta"]
    dev = "cuda" if torch.cuda.is_available() else "cpu"
    pooling = str(meta.get("pooling", "mean")).lower()
    if pooling == "attention":
        return _build_attention_apply(blob, meta, dev), meta
    return _build_mean_apply(blob, meta, dev), meta


def _ensure_target_config(session: Session, p: ApplyLearnedEncoderPayload, meta: dict) -> uuid.UUID:
    """Create (idempotently, keyed on model_name) the learned-code EmbeddingConfig; return its id."""
    pooling = str(meta.get("pooling", "mean")).lower()
    pool_tag = "attn" if pooling == "attention" else "mean"
    name = (f"{p.target_model_name}:{pool_tag}:{meta.get('objective', 'learned')}:"
            f"{str(p.source_embedding_config_id)[:8]}")
    existing = session.execute(
        select(EmbeddingConfig).where(EmbeddingConfig.model_name == name)
    ).scalar_one_or_none()
    if existing is not None:
        return existing.id
    cfg = EmbeddingConfig(
        model_name=name, model_backend="learned-code", layer_indices=[0], layer_agg="none",
        pooling=pooling if pooling == "attention" else "learned",
        normalize_residues=False, normalize=False, use_chunking=False,
        description=(f"learned GO-aligned code ({pool_tag}-pool, dict={meta['dict_dim']}, "
                     f"top_k={meta['top_k']}, objective={meta.get('objective')}) over source config "
                     f"{p.source_embedding_config_id}"),
        display_name=name, family="learned-code",
    )
    session.add(cfg)
    session.flush()
    return cfg.id


class ApplyLearnedEncoderOperation:
    """Apply a trained learned encoder to every source embedding, storing codes under a new config."""

    name = "apply_learned_encoder"
    description = (
        "Project every protein's mean-pooled embedding (a source EmbeddingConfig) through a "
        "trained learned encoder into a GO-aligned top-k code, stored as SequenceEmbedding rows "
        "under a new learned-code EmbeddingConfig for KNN retrieval."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        src = str(p.get("source_embedding_config_id", ""))[:8]
        artifact = str(p.get("encoder_artifact_path", "")).rsplit("/", 1)[-1]
        bits = []
        if src:
            bits.append(f"src={src}")
        if artifact:
            bits.append(f"encoder={artifact}")
        limit = p.get("sequence_id_limit")
        if limit is not None:
            bits.append(f"limit={limit}")
        return " ".join(bits)

    def execute(self, session: Session, payload: dict[str, Any], *, emit: EmitFn) -> OperationResult:
        import numpy as np

        p = ApplyLearnedEncoderPayload.model_validate(payload)
        src_id = uuid.UUID(p.source_embedding_config_id)
        apply, meta = _load_encoder(p.encoder_artifact_path)
        if int(meta["in_dim"]) <= 0:
            raise ValueError("encoder meta missing in_dim")
        target_id = _ensure_target_config(session, p, meta)
        emit("apply_learned_encoder.start", None,
             {"source": str(src_id), "target": str(target_id), "meta": meta}, "info")

        # distinct source sequence_ids (per-chunk rows are grouped + pooled per sequence in apply)
        seq_q = select(SequenceEmbedding.sequence_id).where(
            SequenceEmbedding.embedding_config_id == src_id).distinct()
        if p.sequence_id_limit is not None:
            seq_q = seq_q.limit(p.sequence_id_limit)
        seq_ids = [r[0] for r in session.execute(seq_q).all()]
        emit("apply_learned_encoder.scope", None, {"source_sequences": len(seq_ids)}, "info")

        stored = skipped = 0
        for start in range(0, len(seq_ids), p.batch_size):
            batch = seq_ids[start:start + p.batch_size]
            if p.skip_existing:
                done = {r[0] for r in session.execute(
                    select(SequenceEmbedding.sequence_id).where(
                        SequenceEmbedding.embedding_config_id == target_id,
                        SequenceEmbedding.sequence_id.in_(batch))).all()}
                batch = [s for s in batch if s not in done]
                skipped += len(done)
            if not batch:
                continue
            rows = session.execute(
                select(SequenceEmbedding.sequence_id, SequenceEmbedding.embedding,
                       SequenceEmbedding.chunk_index_s)
                .where(SequenceEmbedding.embedding_config_id == src_id,
                       SequenceEmbedding.sequence_id.in_(batch))
                .order_by(SequenceEmbedding.sequence_id, SequenceEmbedding.chunk_index_s)).all()
            by_seq: dict[int, list] = {}
            for sid, emb, _cs in rows:
                by_seq.setdefault(sid, []).append(np.asarray(emb.to_list(), dtype=np.float32))
            order = [s for s in batch if s in by_seq]
            # per-sequence chunk-vector matrices in chunk_index_s order (apply pools internally)
            chunk_groups = [np.vstack(by_seq[s]).astype(np.float32) for s in order]
            codes = apply(chunk_groups)
            session.bulk_insert_mappings(SequenceEmbedding, [
                {"sequence_id": s, "embedding_config_id": target_id, "chunk_index_s": 0,
                 "chunk_index_e": None, "embedding": codes[i].tolist(),
                 "embedding_dim": int(codes.shape[1])}
                for i, s in enumerate(order)])
            session.commit()
            stored += len(order)
            emit("apply_learned_encoder.progress", None,
                 {"stored": stored, "skipped": skipped, "total": len(seq_ids)}, "info")

        result = {"target_embedding_config_id": str(target_id), "stored": stored,
                  "skipped": skipped, "source_sequences": len(seq_ids)}
        emit("apply_learned_encoder.done", None, result, "info")
        return OperationResult(result=result)
