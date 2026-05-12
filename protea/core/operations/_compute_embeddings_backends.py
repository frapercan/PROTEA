"""Per-PLM backend implementations extracted from ``compute_embeddings``.

This sibling module hosts the four embed-function families (ESM,
T5, Ankh, ESM3c) and the shared layer / pooling / chunking helpers
they all build on. The dispatch registry + plugin loader stay in
``compute_embeddings.py``; this file only contains pure forward-pass
+ pooling code.

The ``ChunkEmbedding`` data container is now sourced from
``protea_backends._chunk_helpers`` (T2A.5b consolidation) so the
plugin-returned value (``list[ChunkEmbedding]`` produced by
``plugin.embed_chunks``) and any PROTEA-local construction share a
single class identity. PROTEA re-exports it from here and from
``compute_embeddings`` for backwards-compatible imports.

The four ``_embed_*`` functions are retained as the bit-exact
parity reference for the plugin ports (see
``tests/test_compute_embeddings_backend_dispatch.py``) and are
re-exported from ``compute_embeddings`` so the documented
``unittest.mock.patch("protea.core.operations.compute_embeddings._embed_*")``
test pattern keeps working unchanged.
"""

from __future__ import annotations

import re
from typing import Any, NamedTuple

from protea_backends._chunk_helpers import ChunkEmbedding

from protea.core.operations._compute_embeddings_helpers import t5_forward_pass
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig

__all__ = [
    "ChunkEmbedding",
    "_aggregate_1d",
    "_aggregate_residue_layers",
    "_chunk_and_pool",
    "_compute_chunk_spans",
    "_embed_ankh",
    "_embed_esm",
    "_embed_esm3c",
    "_embed_t5",
    "_validate_layers",
]


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _validate_layers(
    layer_indices: list[int],
    hidden_states: Any,
    model_tag: str,
    seq_id: str,
) -> list[int]:
    """Validate reverse-indexed layer indices against the model's hidden states.

    ``layer_indices = [0]`` → last layer; ``[1]`` → penultimate; etc.
    Raises ``ValueError`` if any index is out of range.
    Returns a sorted, deduplicated list of valid indices.
    """
    import torch

    if isinstance(hidden_states, torch.Tensor):
        total = int(hidden_states.shape[0])
    else:
        total = len(hidden_states)

    req = sorted(set(int(li) for li in layer_indices))
    invalid = [li for li in req if not (0 <= li < total)]
    if invalid:
        raise ValueError(
            f"[{model_tag}] seq={seq_id!r}: invalid layer_indices {invalid}. "
            f"Valid range: 0..{total - 1}  (0 = last layer)."
        )
    return req


def _aggregate_residue_layers(layer_tensors: list[Any], layer_agg: str) -> Any:
    """Combine [L, D] tensors from multiple layers into one [L, D] tensor."""
    import torch

    if layer_agg == "last":
        return layer_tensors[-1]
    elif layer_agg == "mean":
        return torch.stack(layer_tensors, dim=0).mean(dim=0)
    elif layer_agg == "concat":
        return torch.cat(layer_tensors, dim=-1)
    else:
        raise ValueError(f"Unknown layer_agg: {layer_agg!r}. Choose: last, mean, concat")


def _aggregate_1d(layer_tensors: list[Any], layer_agg: str) -> Any:
    """Combine [D] tensors from multiple layers into one [D] tensor (CLS path)."""
    import torch

    if layer_agg == "last":
        return layer_tensors[-1]
    elif layer_agg == "mean":
        return torch.stack(layer_tensors, dim=0).mean(dim=0)
    elif layer_agg == "concat":
        return torch.cat(layer_tensors, dim=-1)
    else:
        raise ValueError(f"Unknown layer_agg: {layer_agg!r}. Choose: last, mean, concat")


def _chunk_and_pool(residues: Any, config: EmbeddingConfig) -> list[ChunkEmbedding]:
    """Apply chunking (optional) and pooling to a residue tensor [L, D].

    Returns one ``ChunkEmbedding`` per chunk.  Without chunking, returns a
    single element covering the full sequence.
    """
    import torch
    import torch.nn.functional as F

    if config.use_chunking:
        spans = _compute_chunk_spans(residues.shape[0], config.chunk_size, config.chunk_overlap)
    else:
        spans = [(0, residues.shape[0])]

    results: list[ChunkEmbedding] = []
    for start, end in spans:
        chunk = residues[start:end]  # [chunk_L, D]

        if config.pooling == "mean":
            pooled = chunk.mean(dim=0)
        elif config.pooling == "max":
            pooled = chunk.max(dim=0).values
        elif config.pooling == "mean_max":
            pooled = torch.cat([chunk.mean(dim=0), chunk.max(dim=0).values])
        else:
            raise ValueError(
                f"Pooling {config.pooling!r} is not supported in residue-level mode. "
                f"Use 'cls' for CLS token pooling."
            )

        if config.normalize:
            pooled = F.normalize(pooled.unsqueeze(0), p=2, dim=1).squeeze(0)

        chunk_index_e = end if config.use_chunking else None
        results.append(
            ChunkEmbedding(
                chunk_index_s=start,
                chunk_index_e=chunk_index_e,
                vector=pooled.float().cpu().numpy(),
            )
        )

    return results


def _compute_chunk_spans(length: int, chunk_size: int, overlap: int) -> list[tuple[int, int]]:
    """Compute (start, end) spans for overlapping chunks over a sequence of ``length`` residues.

    Raises ``ValueError`` if ``overlap >= chunk_size``; such a configuration
    would produce O(L) single-residue chunks or an infinite loop.
    """
    if overlap >= chunk_size:
        raise ValueError(
            f"chunk_overlap ({overlap}) must be strictly less than chunk_size ({chunk_size})"
        )
    step = chunk_size - overlap
    spans: list[tuple[int, int]] = []
    start = 0
    while start < length:
        end = min(start + chunk_size, length)
        spans.append((start, end))
        start += step
    return spans


# ---------------------------------------------------------------------------
# Backend: ESM (HuggingFace EsmModel)
# ---------------------------------------------------------------------------


def _embed_esm_one(
    model: Any,
    tokenizer: Any,
    seq_str: str,
    config: EmbeddingConfig,
    device: str,
) -> list[ChunkEmbedding]:
    """ESM-2 forward pass + pooling for one sequence.

    Used by :func:`_embed_esm` to keep the per-batch loop body
    readable. Excludes CLS (position 0) and EOS (last valid position)
    from residue-level operations. ``attention_mask.sum()`` covers
    CLS + content + EOS, so the residue slice is ``[1:actual_len-1]``.
    """
    import torch
    import torch.nn.functional as F

    tokens = tokenizer(
        seq_str,
        return_tensors="pt",
        truncation=True,
        max_length=config.max_length,
        add_special_tokens=True,
    )
    tokens = {k: v.to(device) for k, v in tokens.items()}
    outputs = model(**tokens, output_hidden_states=True)
    hidden_states = outputs.hidden_states
    valid_layers = _validate_layers(config.layer_indices, hidden_states, "ESM", seq_str[:20])
    if config.pooling == "cls":
        layer_tensors_1d = [hidden_states[-(li + 1)][0, 0, :].float() for li in valid_layers]
        pooled = _aggregate_1d(layer_tensors_1d, config.layer_agg)
        if config.normalize:
            pooled = F.normalize(pooled.unsqueeze(0), p=2, dim=1).squeeze(0)
        chunks = [ChunkEmbedding(0, None, pooled.cpu().numpy())]
    else:
        actual_len = int(tokens["attention_mask"].sum().item())
        layer_tensors_2d = [
            hidden_states[-(li + 1)][0, 1 : actual_len - 1, :].float() for li in valid_layers
        ]
        residues = _aggregate_residue_layers(layer_tensors_2d, config.layer_agg)
        if config.normalize_residues:
            residues = F.normalize(residues, p=2, dim=1)
        chunks = _chunk_and_pool(residues, config)
    del outputs, hidden_states
    torch.cuda.empty_cache()
    return chunks


def _embed_esm(
    model: Any,
    tokenizer: Any,
    sequences: list[str],
    config: EmbeddingConfig,
    device: str,
) -> list[list[ChunkEmbedding]]:
    """Embed sequences with ESM-2 / EsmModel.

    Processes one sequence at a time to handle variable lengths
    without OOM issues.
    """
    import torch

    results: list[list[ChunkEmbedding]] = []
    with torch.no_grad():
        for seq_str in sequences:
            results.append(_embed_esm_one(model, tokenizer, seq_str, config, device))
    return results


# ---------------------------------------------------------------------------
# Backend: T5 (HuggingFace T5EncoderModel)
# ---------------------------------------------------------------------------


class _T5Mode(NamedTuple):
    """Tokenisation/prefix mode for ``_embed_t5``.

    ``use_aa2fold=None`` triggers auto-detect from ``config.model_name``
    (ProstT5 substring); callers like Ankh override with explicit values.
    """

    use_aa2fold: bool | None = None
    split_into_words: bool = False


def _t5_tokenise(
    tokenizer: Any,
    cleaned: list[str],
    config: EmbeddingConfig,
    mode: _T5Mode,
    use_aa2fold: bool,
) -> Any:
    """Apply the tokeniser branch picked by ``mode.split_into_words``.

    ``use_aa2fold`` here is the resolved boolean (auto-detected or
    explicitly set), used only on the space-joined path.
    """
    if mode.split_into_words:
        # Ankh path: list-of-chars with is_split_into_words=True so the
        # tokeniser treats each residue as one word and never falls back to <unk>.
        return tokenizer.batch_encode_plus(
            [list(c) for c in cleaned],
            padding="longest",
            truncation=True,
            max_length=config.max_length,
            add_special_tokens=True,
            is_split_into_words=True,
            return_tensors="pt",
        )
    processed = [("<AA2fold> " if use_aa2fold else "") + " ".join(c) for c in cleaned]
    return tokenizer.batch_encode_plus(
        processed,
        padding="longest",
        truncation=True,
        max_length=config.max_length,
        add_special_tokens=True,
        return_tensors="pt",
    )


def _t5_pool_one(
    seq_idx: int,
    hidden_states: Any,
    attention_mask: Any,
    valid_layers: list[int],
    start_idx: int,
    config: EmbeddingConfig,
) -> list[ChunkEmbedding]:
    """Pool one batched sequence's hidden states into ChunkEmbedding rows.

    Two pooling paths:
    * ``cls``: position 0 (``<AA2fold>`` on ProstT5, otherwise first AA).
    * residue: strip prefix (``start_idx``) and trailing EOS so residues
      start at AA 0 and ``residues.shape[0]`` equals the amino-acid count.
    """
    import torch.nn.functional as F

    actual_len = int(attention_mask[seq_idx].sum().item())
    if config.pooling == "cls":
        layer_tensors_1d = [hidden_states[-(li + 1)][seq_idx, 0, :].float() for li in valid_layers]
        pooled = _aggregate_1d(layer_tensors_1d, config.layer_agg)
        if config.normalize:
            pooled = F.normalize(pooled.unsqueeze(0), p=2, dim=1).squeeze(0)
        return [ChunkEmbedding(0, None, pooled.cpu().numpy())]
    layer_tensors_2d = [
        hidden_states[-(li + 1)][seq_idx, start_idx : actual_len - 1, :].float()
        for li in valid_layers
    ]
    residues = _aggregate_residue_layers(layer_tensors_2d, config.layer_agg)
    if config.normalize_residues:
        residues = F.normalize(residues, p=2, dim=1)
    return _chunk_and_pool(residues, config)


def _embed_t5(
    model: Any,
    tokenizer: Any,
    sequences: list[str],
    config: EmbeddingConfig,
    device: str,
    mode: _T5Mode = _T5Mode(),
) -> list[list[ChunkEmbedding]]:
    """Embed sequences with T5EncoderModel (ProstT5, prot_t5_xl, Ankh, …).

    Sequences are processed as a padded batch.  ProSTT5 mode is auto-detected
    from ``config.model_name`` (looks for ``prostt5`` substring, case-insensitive)
    when ``mode.use_aa2fold`` is ``None``; callers (e.g. the Ankh backend) can
    pass ``use_aa2fold=False`` via ``mode`` to disable the prefix unconditionally.

    Tokenisation strategies live on ``mode.split_into_words``:
    ``False`` → space-joined string (ProstT5 / prot_t5_xl SentencePiece);
    ``True`` → per-residue list with ``is_split_into_words=True`` (Ankh,
    whose tokeniser maps a literal space to ``<unk>``).

    Residue slicing strips the optional ``<AA2fold>`` prefix and trailing
    EOS so ``residues[0]`` is always AA 0 and ``residues.shape[0]`` equals
    the amino-acid count, matching the convention used by ``_embed_esm`` /
    ``_embed_esm3c``.
    """
    import torch

    use_aa2fold = (
        mode.use_aa2fold if mode.use_aa2fold is not None else "prostt5" in config.model_name.lower()
    )
    cleaned = [re.sub(r"[UZOB]", "X", seq_str) for seq_str in sequences]
    inputs = _t5_tokenise(tokenizer, cleaned, config, mode, use_aa2fold)
    inputs = {k: v.to(device) for k, v in inputs.items()}

    hidden_states = t5_forward_pass(model, inputs["input_ids"], inputs["attention_mask"])

    valid_layers = _validate_layers(config.layer_indices, hidden_states, "T5", "batch")
    start_idx = 1 if use_aa2fold else 0  # skip <AA2fold> on ProstT5
    results: list[list[ChunkEmbedding]] = [
        _t5_pool_one(i, hidden_states, inputs["attention_mask"], valid_layers, start_idx, config)
        for i in range(len(sequences))
    ]

    del hidden_states
    torch.cuda.empty_cache()
    return results


# ---------------------------------------------------------------------------
# Backend: Ankh (HuggingFace T5EncoderModel, loaded via AutoTokenizer)
# ---------------------------------------------------------------------------


def _embed_ankh(
    model: Any,
    tokenizer: Any,
    sequences: list[str],
    config: EmbeddingConfig,
    device: str,
) -> list[list[ChunkEmbedding]]:
    """Embed sequences with Ankh (base / large).

    Ankh is a T5 encoder-decoder; we reuse the shared T5 batched pipeline but
    with two deviations:

    * never inject the ProstT5 ``<AA2fold>`` prefix (Ankh was pre-trained on
      plain amino-acid sequences);
    * tokenise via ``is_split_into_words=True`` with a list of per-residue
      characters.  Ankh's SentencePiece tokeniser maps a literal space to
      ``<unk>``, so the space-joined path used for ProstT5 produces ~50%
      ``<unk>`` tokens and collapses to NaN under FP16.  Verified on
      ``ElnaggarLab/ankh-base``.
    """
    return _embed_t5(
        model,
        tokenizer,
        sequences,
        config,
        device,
        mode=_T5Mode(use_aa2fold=False, split_into_words=True),
    )


# ---------------------------------------------------------------------------
# Backend: ESM3c (ESM SDK ESMC)
# ---------------------------------------------------------------------------


def _embed_esm3c_one(
    model: Any,
    seq_str: str,
    config: EmbeddingConfig,
    device_obj: Any,
) -> list[ChunkEmbedding]:
    """ESM3c forward pass + pooling for one sequence.

    Used by :func:`_embed_esm3c` to keep the per-batch loop body readable.
    Strips BOS (position 0) and EOS (position -1) tokens before residue-level
    pooling, matching PIS / FANTASIA behaviour.
    """
    import torch
    import torch.nn.functional as F
    from esm.sdk.api import ESMProtein, LogitsConfig

    protein = ESMProtein(sequence=seq_str[: config.max_length])
    with torch.autocast(
        device_type=device_obj.type,
        dtype=torch.float16,
        enabled=(device_obj.type == "cuda"),
    ):
        protein_tensor = model.encode(protein)
        logits_output = model.logits(
            protein_tensor,
            LogitsConfig(sequence=True, return_hidden_states=True),
        )

    hs = logits_output.hidden_states
    if hs is None:
        raise RuntimeError(f"ESM3c returned no hidden_states for sequence {seq_str[:20]!r}")
    if isinstance(hs, torch.Tensor):
        hs = [hs[i] for i in range(hs.shape[0])]

    valid_layers = _validate_layers(config.layer_indices, hs, "ESM3c", seq_str[:20])

    if config.pooling == "cls":
        # BOS token at position 0 (before stripping)
        layer_tensors_1d = [hs[-(li + 1)][0, 0, :].float() for li in valid_layers]
        pooled = _aggregate_1d(layer_tensors_1d, config.layer_agg)
        if config.normalize:
            pooled = F.normalize(pooled.unsqueeze(0), p=2, dim=1).squeeze(0)
        chunks = [ChunkEmbedding(0, None, pooled.cpu().numpy())]
    else:
        # Strip BOS (0) and EOS (-1): positions [1:-1]
        layer_tensors_2d = [hs[-(li + 1)][0, 1:-1, :].float() for li in valid_layers]
        residues = _aggregate_residue_layers(layer_tensors_2d, config.layer_agg)
        if config.normalize_residues:
            residues = F.normalize(residues, p=2, dim=1)
        chunks = _chunk_and_pool(residues, config)

    del logits_output, hs
    torch.cuda.empty_cache()
    return chunks


def _embed_esm3c(
    model: Any,
    sequences: list[str],
    config: EmbeddingConfig,
    device: str,
) -> list[list[ChunkEmbedding]]:
    """Embed sequences with ESMC (ESM3c family).

    Uses the ESM SDK directly: no external tokenizer.  The model must have
    been loaded with ``ESMC.from_pretrained`` and cast to FP16.  Hidden states
    are returned via ``LogitsConfig(return_hidden_states=True)``.
    """
    import torch

    device_obj = torch.device(device) if isinstance(device, str) else device
    results: list[list[ChunkEmbedding]] = []
    with torch.no_grad():
        for seq_str in sequences:
            results.append(_embed_esm3c_one(model, seq_str, config, device_obj))
    return results
