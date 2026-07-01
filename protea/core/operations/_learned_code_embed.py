"""On-the-fly learned-code embedding for the serve query path.

A ``learned-code`` :class:`EmbeddingConfig` (for example the validated
``d8979601`` k-WTA retrieval encoder) stores GO-aligned codes, not a raw PLM
vector. Offline those codes were materialised by
:mod:`protea.core.operations.apply_learned_encoder` over BASE embeddings that
had already been pre-computed for every pool protein. A NOVEL ``/annotate``
query has no pre-computed base embedding, so pinning retrieval to a learned
config previously left the query un-embeddable (the compute path tried to load
``"learned-code:..."`` as a HuggingFace model and failed).

This module closes that gap. When the serve embed path is asked to embed a
sequence with a learned-code config it, on the fly:

1. resolves the BASE :class:`EmbeddingConfig` the learned head was trained over
   (:func:`resolve_base_config`),
2. embeds the query with that base config via a caller-injected embed function
   (a standard PLM the backend already supports),
3. applies the learned head, reusing
   :func:`~protea.core.operations.apply_learned_encoder._load_encoder`'s
   apply-builder so the k-WTA / attention-pool math is never duplicated,
4. returns the codes as a single per-sequence
   :class:`~protea_backends._chunk_helpers.ChunkEmbedding` so the UNCHANGED
   ``store_embeddings`` path persists them under the learned config for KNN
   retrieval to reuse (cache: computed once per novel query).

This mirrors the offline pipeline exactly; the ONLY difference is the base
embeddings are computed on demand instead of read back from the DB.

Base-config resolution
----------------------
``apply_learned_encoder`` names the learned config
``"{target_model_name}:{pool_tag}:{objective}:{source_id[:8]}"`` (for example
``"learned-code:hard-neg:08234f06"``). The trailing colon-segment is therefore
the first 8 hex chars of the SOURCE (base) config id, so the base config is
recovered by matching that id prefix (:func:`resolve_base_config`). No path or
id is hard-coded in the serve code.

Head-artifact resolution
-------------------------
The head ``.pt`` blob is resolved from env, mirroring the two-tower classifier's
env-only artifact convention (``PROTEA_TWO_TOWER_*``): ``PROTEA_LEARNED_ENCODER``
``_ARTIFACT`` (an explicit ``.pt``) wins; otherwise
``PROTEA_LEARNED_ENCODER_DIR`` is searched for ``<config_id>.pt`` then
``<config_id[:8]>.pt``. A missing artifact or base config raises a clear
``ValueError`` (never hangs).
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import TYPE_CHECKING

from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig

if TYPE_CHECKING:
    import numpy as np
    from protea_backends._chunk_helpers import ChunkEmbedding
    from sqlalchemy.orm import Session

    from protea.core.contracts.operation import EmitFn

# Head-artifact env vars (mirrors the PROTEA_TWO_TOWER_* artifact convention).
_ARTIFACT_ENV = "PROTEA_LEARNED_ENCODER_ARTIFACT"  # explicit head .pt path
_DIR_ENV = "PROTEA_LEARNED_ENCODER_DIR"  # dir of <config_id>.pt head blobs

# Callable a caller injects to embed a batch with a STANDARD base config:
# (base_config, sequences) -> per-sequence per-chunk ChunkEmbedding lists. The
# caller's closure binds the device, so the learned path stays device-agnostic.
EmbedBaseFn = Callable[[EmbeddingConfig, "list[str]"], "list[list[ChunkEmbedding]]"]

LEARNED_CODE_BACKEND = "learned-code"


def is_learned_code_config(config: EmbeddingConfig) -> bool:
    """True when ``config`` is a learned-code (GO-aligned code) embedding config."""
    return config.model_backend == LEARNED_CODE_BACKEND


def _base_config_prefix(config: EmbeddingConfig) -> str:
    """Recover the base config id prefix encoded in the learned config's name.

    ``apply_learned_encoder`` names the learned config with the SOURCE config's
    ``id[:8]`` as the trailing colon-segment; that segment is returned here.
    """
    name = config.model_name or ""
    return name.rsplit(":", 1)[-1].strip()


def resolve_base_config(session: Session, config: EmbeddingConfig) -> EmbeddingConfig:
    """Resolve the standard-PLM base :class:`EmbeddingConfig` for a learned config.

    Matches the id-prefix encoded in ``config.model_name`` against existing
    configs, excluding learned-code configs (so a learned config never resolves
    to itself). Raises a clear ``ValueError`` on a missing or ambiguous match so
    the serve path fails fast instead of hanging.
    """
    from sqlalchemy import String, cast, select  # noqa: PLC0415

    prefix = _base_config_prefix(config)
    if not prefix:
        raise ValueError(
            f"learned-code config {config.model_name!r} encodes no base config id; "
            "expected a name ending in ':<base_config_id[:8]>'"
        )
    matches = [
        c
        for c in session.execute(
            select(EmbeddingConfig).where(cast(EmbeddingConfig.id, String).like(f"{prefix}%"))
        )
        .scalars()
        .all()
        if c.model_backend != LEARNED_CODE_BACKEND
    ]
    if not matches:
        raise ValueError(
            f"learned-code config {config.model_name!r}: no base EmbeddingConfig with id "
            f"prefix {prefix!r} exists; embed the base PLM before pinning this config"
        )
    if len(matches) > 1:
        raise ValueError(
            f"learned-code config {config.model_name!r}: id prefix {prefix!r} is ambiguous "
            f"({len(matches)} base configs match: {[str(c.id) for c in matches]})"
        )
    return matches[0]


def resolve_encoder_artifact(config: EmbeddingConfig) -> str:
    """Resolve the learned head ``.pt`` path from env (no hard-coded path).

    Priority: ``PROTEA_LEARNED_ENCODER_ARTIFACT`` (explicit path) then
    ``PROTEA_LEARNED_ENCODER_DIR``/<config_id>.pt or /<config_id[:8]>.pt.
    Raises ``ValueError`` when unset or the file is absent.
    """
    explicit = os.environ.get(_ARTIFACT_ENV)
    if explicit:
        if not os.path.exists(explicit):
            raise ValueError(f"{_ARTIFACT_ENV}={explicit!r} does not exist")
        return explicit
    enc_dir = os.environ.get(_DIR_ENV)
    if enc_dir:
        candidates = [f"{config.id}.pt", f"{str(config.id)[:8]}.pt"]
        for cand in candidates:
            path = os.path.join(enc_dir, cand)
            if os.path.exists(path):
                return path
        raise ValueError(
            f"no learned head for config {config.id} under {_DIR_ENV}={enc_dir!r} "
            f"(looked for {candidates})"
        )
    raise ValueError(
        f"learned-code config {config.model_name!r} needs a head artifact: set "
        f"{_ARTIFACT_ENV} (explicit .pt) or {_DIR_ENV} (dir of <config_id>.pt)"
    )


def _group_chunk_vectors(base_chunks: list[list[ChunkEmbedding]]) -> list[np.ndarray]:
    """Stack each sequence's per-chunk vectors into a ``(n_chunks, in_dim)`` array."""
    import numpy as np  # noqa: PLC0415

    return [np.vstack([c.vector for c in chunks]).astype(np.float32) for chunks in base_chunks]


def embed_learned_code(
    session: Session,
    config: EmbeddingConfig,
    sequences: list[str],
    emit: EmitFn,
    *,
    embed_base: EmbedBaseFn,
    batch_size: int = 1,
) -> list[list[ChunkEmbedding]]:
    """Embed ``sequences`` under a learned-code ``config`` (base -> head -> codes).

    Resolves the base config + head artifact once, then for each batch embeds
    with the base PLM (via ``embed_base``, which binds the device), pools the
    per-chunk vectors, and applies the learned head. Returns one single-chunk
    :class:`ChunkEmbedding` list per sequence (the GO-aligned code), aligned to
    ``sequences``, ready for the unchanged ``store_embeddings`` persistence path.
    """
    from protea_backends._chunk_helpers import ChunkEmbedding  # noqa: PLC0415

    from protea.core.operations.apply_learned_encoder import _load_encoder  # noqa: PLC0415

    base_config = resolve_base_config(session, config)
    artifact = resolve_encoder_artifact(config)
    apply, meta = _load_encoder(artifact)
    if int(meta.get("in_dim", 0)) <= 0:
        raise ValueError(f"learned head {artifact!r} meta missing a positive in_dim")
    emit(
        "compute_embeddings.learned_code_start",
        None,
        {
            "learned_config": str(config.id),
            "base_config": str(base_config.id),
            "artifact": os.path.basename(artifact),
            "dict_dim": int(meta.get("dict_dim", 0)),
        },
        "info",
    )
    results: list[list[ChunkEmbedding]] = []
    for start in range(0, len(sequences), max(1, batch_size)):
        batch = sequences[start : start + max(1, batch_size)]
        base_chunks = embed_base(base_config, batch)
        codes = apply(_group_chunk_vectors(base_chunks))
        results.extend([ChunkEmbedding(0, None, row.astype("float32"))] for row in codes)
    emit(
        "compute_embeddings.learned_code_done",
        None,
        {"sequences": len(results), "base_config": str(base_config.id)},
        "info",
    )
    return results
