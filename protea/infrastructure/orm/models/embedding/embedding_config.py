from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, Boolean, DateTime, Float, Index, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base

_VALID_LAYER_AGG = {"mean", "last", "concat"}
_VALID_POOLING = {"mean", "max", "cls", "mean_max"}
_VALID_BACKENDS = {"esm", "esm3c", "t5", "ankh", "protst", "auto"}


class EmbeddingConfig(Base):
    """Defines a reproducible recipe for computing protein embeddings.

    Every ``SequenceEmbedding`` row points to exactly one ``EmbeddingConfig``,
    providing complete provenance: which model, which transformer layers, how
    layers are aggregated, how the sequence is pooled, and whether the vector
    is L2-normalised.

    Layer indexing convention (reverse, consistent with PIS)
    --------------------------------------------------------
    ``layer_indices`` use **reverse indexing**: 0 = last (most semantic) layer,
    1 = penultimate, 2 = antepenultimate, etc.  This matches the convention
    used across all backends in PIS / FANTASIA.

    Layer aggregation strategies
    ----------------------------
    - ``mean``   : element-wise average across selected layers (dim unchanged).
    - ``concat`` : concatenation of all selected layers (dim × n_layers).

    Sequence pooling strategies
    ---------------------------
    - ``mean``     : mean over residue representations.
    - ``max``      : max over residue representations.
    - ``cls``      : CLS/BOS token only (position 0 of raw hidden states).
    - ``mean_max`` : concatenation of mean and max (dim × 2).

    Model backends
    --------------
    - ``esm``   : HuggingFace ``EsmModel`` (ESM-2 family).
    - ``esm3c`` : ESM SDK ``ESMC`` (ESM3c family).  No external tokenizer.
                  Runs FP16 on GPU.  CLS and EOS tokens stripped for pooling.
    - ``t5``    : HuggingFace ``T5EncoderModel`` (ProstT5, prot_t5_xl…).
                  ProSTT5 mode auto-detected from ``model_name``.
    - ``ankh``  : HuggingFace ``T5EncoderModel`` loaded via ``AutoTokenizer``
                  (Ankh base/large).  No ``<AA2fold>`` prefix; ambiguous
                  residues are substituted with ``X`` like the other T5 path.
    - ``protst``: text-aligned ProtST-ESM1b (``mila-intel/ProtST-esm1b``).
                  Whole-protein ``protein_feature`` projection (512-d,
                  orthogonal text-aligned signal); one vector per sequence,
                  honours only ``normalize``.
    - ``auto``  : falls back to ``esm``.

    Normalisation
    -------------
    - ``normalize_residues`` : L2-normalise each residue representation before
                               pooling (applied after layer aggregation).
    - ``normalize``          : L2-normalise the final pooled vector.

    Chunking
    --------
    Long sequences can be split into overlapping chunks before pooling.
    Each chunk produces one ``SequenceEmbedding`` row identified by
    ``chunk_index_s`` and ``chunk_index_e``.
    """

    __tablename__ = "embedding_config"
    __table_args__ = (
        # T3.5: list endpoints order by ``created_at DESC``.
        Index("ix_embedding_config_created_at", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    model_name: Mapped[str] = mapped_column(String, nullable=False, index=True)
    model_backend: Mapped[str] = mapped_column(String, nullable=False, default="esm")
    layer_indices: Mapped[list[Any]] = mapped_column(JSONB, nullable=False)
    layer_agg: Mapped[str] = mapped_column(String, nullable=False, default="mean")
    pooling: Mapped[str] = mapped_column(String, nullable=False, default="mean")
    normalize_residues: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    normalize: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # Uniform per-config divisor applied before the halfvec (fp16) store so
    # high-magnitude PLM layers (e.g. Ankh-base layer 10, |max| ~4.9e5) fit the
    # fp16 range without overflow. Default 1.0 => no-op: every existing config
    # (incl. the byte-for-byte champion) stores its raw embedding unchanged.
    # Safe for all consumers: cosine-KNN is scale-invariant and the downstream
    # per-dim z-score absorbs a uniform scale, so embedding/scale == embedding.
    embedding_scale: Mapped[float] = mapped_column(
        Float, nullable=False, default=1.0, server_default="1.0"
    )
    max_length: Mapped[int] = mapped_column(Integer, nullable=False, default=1022)
    use_chunking: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    chunk_size: Mapped[int] = mapped_column(Integer, nullable=False, default=512)
    chunk_overlap: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    description: Mapped[str | None] = mapped_column(String, nullable=True)
    display_name: Mapped[str | None] = mapped_column(String, nullable=True)
    family: Mapped[str | None] = mapped_column(String, nullable=True)
    param_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    def __repr__(self) -> str:
        return (
            f"<EmbeddingConfig({self.model_name!r} | backend={self.model_backend} "
            f"| layers={self.layer_indices} | agg={self.layer_agg} "
            f"| pool={self.pooling} | norm_res={self.normalize_residues} "
            f"| norm={self.normalize} | chunking={self.use_chunking})>"
        )
