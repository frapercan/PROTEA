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

from pydantic import field_validator, model_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.operations._compute_embeddings_helpers import (
    fetch_embedding_scale,
    scale_and_clip_embedding,
)
from protea.core.operations._encoder_artifact import (
    resolve_encoder_artifact,
    resolve_training_cut,
)
from protea.core.operations.encode_residue_sparse import ORDERS
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding


class ApplyLearnedEncoderPayload(ProteaPayload, frozen=True):
    source_embedding_config_id: str
    encoder_artifact_path: str | None = None
    encoder_artifact_uri: str | None = None
    target_model_name: str = "learned-code"
    batch_size: int = 4000
    skip_existing: bool = True
    sequence_id_limit: int | None = None  # smoke-test cap; None = all source sequences

    @field_validator("source_embedding_config_id", mode="before")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @model_validator(mode="after")
    def _exactly_one_address(self) -> ApplyLearnedEncoderPayload:
        """Refuse both addresses and refuse neither, as the residue operation does.

        A local path resolves against whichever host runs the work. This operation runs
        inline rather than fanning out, so it runs wherever the operations queue is
        consumed, which is not where an artifact fitted on the compute node was written.
        The store address is the one that means the same thing on both.
        """
        has_path = bool((self.encoder_artifact_path or "").strip())
        has_uri = bool((self.encoder_artifact_uri or "").strip())
        if has_path == has_uri:
            raise ValueError(
                "give exactly one of encoder_artifact_path and encoder_artifact_uri. "
                "The path resolves on whichever host consumes the operations queue; the "
                "uri resolves through the artifact store and is usable when the artifact "
                "was written somewhere else"
            )
        return self


def _topk_real(z, top_k: int):
    """Keep the ``top_k`` largest-magnitude entries per row (zero the rest). ``z``: (n, dict)."""
    import numpy as np

    if top_k >= z.shape[1]:
        return z
    out = np.zeros_like(z)
    idx = np.argpartition(-np.abs(z), top_k, axis=1)[:, :top_k]
    np.put_along_axis(out, idx, np.take_along_axis(z, idx, axis=1), axis=1)
    return out


def _sibling_scaler_path(artifact_path: str) -> str:
    """Default sibling z-score scaler path for a head artifact (``<stem>.scaler.npz``).

    A head stored at ``.../<id>.pt`` looks for ``.../<id>.scaler.npz`` alongside it,
    so the scaler travels with the head with no separate configuration.
    """
    base = artifact_path[:-3] if artifact_path.endswith(".pt") else artifact_path
    return f"{base}.scaler.npz"


def _load_scaler(scaler_path: str | None, in_dim: int):
    """Load an optional per-dim z-score scaler ``(mu, sigma)`` for the base embedding.

    A learned head may ship a sibling ``.scaler.npz`` (keys ``mu`` and ``sigma``,
    each ``(in_dim,)``, fit on the reference pool the head trained over). When present
    the base embedding is standardised ``(x - mu) / sigma`` BEFORE the in-head L2 +
    Linear, reproducing the offline z-score recipe. When ABSENT this returns ``None``
    and the apply math is byte-for-byte the legacy behaviour (existing heads untouched).

    A present-but-malformed scaler (wrong keys, wrong shape, non-positive sigma) raises
    a clear ``ValueError`` rather than silently corrupting the code.
    """
    import os  # noqa: PLC0415

    import numpy as np  # noqa: PLC0415

    if not scaler_path or not os.path.exists(scaler_path):
        return None
    data = np.load(scaler_path)
    if "mu" not in data or "sigma" not in data:
        raise ValueError(
            f"scaler {scaler_path!r} must contain arrays 'mu' and 'sigma'; got {list(data.keys())}"
        )
    mu = np.asarray(data["mu"], dtype=np.float32).reshape(-1)
    sigma = np.asarray(data["sigma"], dtype=np.float32).reshape(-1)
    if mu.shape != (in_dim,) or sigma.shape != (in_dim,):
        raise ValueError(
            f"scaler {scaler_path!r} shape mismatch: mu{mu.shape} sigma{sigma.shape} "
            f"!= expected ({in_dim},) from head meta in_dim"
        )
    if not bool(np.all(np.isfinite(sigma))) or not bool(np.all(sigma > 0)):
        raise ValueError(f"scaler {scaler_path!r} has non-finite or non-positive sigma entries")
    return mu, sigma


def _build_mean_apply(blob: dict, meta: dict, dev: str, scaler=None):
    """Mean-pool family: Linear(d -> dict) over the per-sequence mean of chunk vectors.

    When ``scaler`` is ``(mu, sigma)`` the mean-pooled base is z-scored
    ``(x - mu) / sigma`` before the in-head L2 + Linear (the offline recipe order:
    z-score -> L2 -> Linear -> top-k). ``scaler=None`` is the unchanged legacy path.
    """
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
        if scaler is not None:
            mu, sigma = scaler
            mat = ((mat - mu) / sigma).astype(np.float32)
        nrm = np.linalg.norm(mat, axis=1, keepdims=True)
        nrm[nrm == 0] = 1.0
        xn = (mat / nrm).astype(np.float32)
        with torch.no_grad():
            z = enc(torch.tensor(xn, device=dev)).cpu().numpy().astype(np.float32)
        return _topk_real(z, top_k)

    return apply


def _build_attention_apply(blob: dict, meta: dict, dev: str, scaler=None):
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
            if scaler is not None:
                mu, sigma = scaler
                m = ((m - mu) / sigma).astype(np.float32)
            Xpad[i, : m.shape[0]] = m
            mask[i, : m.shape[0]] = True
        with torch.no_grad():
            z = enc(
                torch.tensor(Xpad, device=dev), torch.tensor(mask, device=dev)
            ).cpu().numpy().astype(np.float32)
        return _topk_real(z, top_k)

    return apply


#: The order this operation implements. It pools the chunk vectors FIRST and applies the map
#: to the result, so the selection at the end sees a whole sequence rather than a residue.
#: ``encode_residue_sparse`` implements the other one and refuses this one by name; this
#: refuses that one by name. Neither is the special case, and an artifact runs in exactly one.
IMPLEMENTED_ORDER = "pool-then-select"


def refuse_wrong_order(meta: dict, artifact_path: str) -> None:
    """Refuse an artifact fitted for the other order, and refuse one that does not say.

    The distinction cannot be recovered from the weights. Both orders produce the same tensor
    shapes and declare the same ``in_dim`` and ``dict_dim``, so an artifact fitted for
    per-residue selection runs happily through this path and yields a complete, plausible code
    computed by the wrong mechanism. Measured on the arms of the encoder study, that code
    shares 12 of 128 atoms with the intended one, at cosine 0.08. Nothing downstream can tell
    them apart, which is why the artifact has to say which it is, and why saying nothing is not
    allowed to mean this one.

    HOW TO ANSWER THE QUESTION RATHER THAN GUESS AT IT. Every artifact predating this guard
    hits the first branch below, and its operator has no way to know which order was used:
    that is the whole premise. The answer is recoverable whenever the bundle ships codes it
    produced. Re-encode a handful of its own proteins through one order and compare against
    the stored codes. The right order reproduces the strong components exactly, and the wrong
    one does not come close: the 12-of-128 overlap at cosine 0.08 above is what wrong looks
    like, against near-perfect agreement on the top atoms and a rank correlation of 1.000 on
    the indices both select.

    Disagreement confined to the selection boundary is expected and is not evidence of the
    wrong order. Atoms near the cut are nearly tied, and half-precision storage of the input
    reshuffles which side of it they land on.

    Deriving the order from the architecture instead is tempting and is not the same thing.
    An ``in_dim`` matching the pooled width means the head never sees residues, which is a
    strong argument and not a measurement, and this codebase has spent enough on the
    difference.
    """
    order = meta.get("order")
    if order is None:
        raise ValueError(
            f"{artifact_path} declares no order. A learned encoder must state whether it was "
            f"fitted to pool before selecting or to select before pooling, because the two "
            f"produce different codes from identical weights and shapes. If the bundle "
            f"ships codes it produced, re-encode a few of its own proteins through this "
            f"order and compare: the right order reproduces the strong components and "
            f"agrees on their ranking, the wrong one overlaps by about 12 of 128 atoms at "
            f"cosine 0.08. Then add order={IMPLEMENTED_ORDER!r} to the artifact meta, with "
            f"the evidence beside it")
    order = str(order)
    if order not in ORDERS:
        raise ValueError(
            f"{artifact_path} declares order {order!r}, which is not one of {list(ORDERS)}")
    if order != IMPLEMENTED_ORDER:
        raise ValueError(
            f"{artifact_path} declares order {order!r}, which this operation does not "
            f"implement: it pools first and applies the map to the pooled vector. Run it "
            f"through encode_residue_sparse, which selects atoms per residue before pooling. "
            f"Running it here would produce a complete code computed the other way")
    if not str(meta.get("training_release") or "").strip():
        raise ValueError(
            f"{artifact_path} declares no training_release. A fitted encoder must say "
            "which annotation release it was fitted against, because the column that "
            "records it means NOT FITTED when it is NULL, and the temporal gate reads "
            "that column before scoring"
        )


def _load_encoder(artifact_path: str, scaler_path: str | None = None):
    """Load the torch artifact -> (apply_fn, meta). Lazy-imports torch/numpy.

    ``apply_fn`` takes a list of per-sequence chunk-vector matrices (each ``(n_chunks, in_dim)``) and
    returns ``(n, dict_dim)`` top-k real codes, so the caller groups source chunks identically for
    both pooling families. ``meta["pooling"]`` selects mean-pool (default) vs attention-pool.

    An optional per-dim z-score scaler travels with the head: ``scaler_path`` (explicit) or, when
    unset, the ``<stem>.scaler.npz`` sibling of ``artifact_path``. When a scaler is found the base
    embedding is standardised before the in-head L2 + Linear (offline recipe); when none is found the
    apply math is the unchanged legacy behaviour, so existing heads are byte-for-byte untouched.
    """
    import torch

    blob = torch.load(artifact_path, map_location="cpu", weights_only=False)
    meta = blob["meta"]
    refuse_wrong_order(meta, artifact_path)
    dev = "cuda" if torch.cuda.is_available() else "cpu"
    scaler = _load_scaler(scaler_path or _sibling_scaler_path(artifact_path), int(meta["in_dim"]))
    pooling = str(meta.get("pooling", "mean")).lower()
    if pooling == "attention":
        return _build_attention_apply(blob, meta, dev, scaler), meta
    return _build_mean_apply(blob, meta, dev, scaler), meta


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
        trained_on_annotation_set_id=resolve_training_cut(session, meta),
    )
    session.add(cfg)
    session.flush()
    return cfg.id


def _encode_and_store_batch(
    session: Session, apply: Any, batch: list[int],
    src_id: uuid.UUID, target_id: uuid.UUID, target_scale: float,
) -> tuple[int, int]:
    """Encode one batch of source sequences to codes and bulk-insert the rows.

    Groups each sequence's per-chunk source vectors, applies the learned encoder,
    then divides each code by the target config's uniform ``embedding_scale`` and
    clips to the fp16 halfvec range before insert. Returns
    ``(sequences_stored, components_clipped)``.
    """
    import numpy as np

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
    clipped = 0
    code_rows = []
    for i, s in enumerate(order):
        scaled_vector, n_clipped = scale_and_clip_embedding(codes[i].tolist(), target_scale)
        clipped += n_clipped
        code_rows.append(
            {"sequence_id": s, "embedding_config_id": target_id, "chunk_index_s": 0,
             "chunk_index_e": None, "embedding": scaled_vector,
             "embedding_dim": int(codes.shape[1])})
    session.bulk_insert_mappings(SequenceEmbedding, code_rows)
    session.commit()
    return len(order), clipped


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
        address = p.get("encoder_artifact_path") or p.get("encoder_artifact_uri") or ""
        artifact = str(address).rsplit("/", 1)[-1]
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
        p = ApplyLearnedEncoderPayload.model_validate(payload)
        src_id = uuid.UUID(p.source_embedding_config_id)
        artifact = resolve_encoder_artifact(p.encoder_artifact_path, p.encoder_artifact_uri)
        apply, meta = _load_encoder(artifact)
        if int(meta["in_dim"]) <= 0:
            raise ValueError("encoder meta missing in_dim")
        target_id = _ensure_target_config(session, p, meta)
        # Uniform per-config divisor + fp16 clip so a high-magnitude learned code
        # cannot overflow the halfvec column; default scale 1.0 => no-op.
        target_scale = fetch_embedding_scale(session, target_id)
        clipped_total = 0
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
            n_stored, n_clipped = _encode_and_store_batch(
                session, apply, batch, src_id, target_id, target_scale)
            stored += n_stored
            clipped_total += n_clipped
            emit("apply_learned_encoder.progress", None,
                 {"stored": stored, "skipped": skipped, "total": len(seq_ids)}, "info")

        if clipped_total:
            emit("apply_learned_encoder.halfvec_clipped", None,
                 {"components_clipped": clipped_total,
                  "reason": "embedding_scale too small; values exceeded fp16 range "
                            "and were clipped to [-65504, 65504]"}, "warning")
        result = {"target_embedding_config_id": str(target_id), "stored": stored,
                  "skipped": skipped, "source_sequences": len(seq_ids)}
        emit("apply_learned_encoder.done", None, result, "info")
        return OperationResult(result=result)
