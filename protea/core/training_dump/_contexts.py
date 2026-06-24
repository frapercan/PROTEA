"""Parameter objects for the dump pipeline.

Originally lived in ``protea/core/training_dump_helpers.py``. Extracted
to a leaf submodule (T2B.6) so the type-only consumers in
``protea/core/_knn_transfer_runner.py`` can import them without
dragging the larger orchestration code into the import graph.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

# ---------------------------------------------------------------------------
# Parameter objects for _knn_transfer_and_label
#
# The function used to take 20 keyword arguments. Two natural clusters
# (per-protein sequence/taxonomy lookups, and the streaming-parquet output
# config) are now passed as small immutable dataclasses to keep call sites
# readable.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SequenceContext:
    """Per-protein sequence and taxonomy lookups.

    All four attributes are optional; passing ``None`` disables the
    corresponding feature family (alignment / taxonomy).
    """

    query_sequences: dict[str, str] | None = None
    ref_sequences: dict[str, str] | None = None
    query_tax_ids: dict[str, int | None] | None = None
    ref_tax_ids: dict[str, int | None] | None = None


@dataclass(frozen=True)
class StreamOutput:
    """Streaming parquet output for memory-bounded dataset generation.

    When provided, ``_knn_transfer_and_label`` writes labeled rows
    directly to ``output_parquet`` in chunks of ``chunk_rows`` instead of
    accumulating the full result list in memory.
    """

    output_parquet: Path
    chunk_rows: int = 100_000


@dataclass(frozen=True)
class KnnTransferContext:
    """Bundle of KNN inputs + enrichment maps for ``_knn_transfer_and_label``.

    Groups the 12 per-call data arguments (queries, references, ontology
    maps, optional enrichment helpers) so the entry-point signature
    stays under flake8-bugbear's parameter ceiling. ``session``,
    payload ``p``, ``sequence_context``, and ``stream_output`` remain
    standalone arguments because they are configuration / IO concerns,
    not data.
    """

    valid_queries: list[str]
    query_emb: np.ndarray
    ref_by_aspect: dict[str, dict[str, Any]]
    go_id_map: dict[int, str]
    aspect_map: dict[int, str]
    gt_pairs: set[tuple[str, str]]
    query_known_gos: dict[str, set[str]] | None = None
    parent_map_str: dict[str, set[str]] | None = None
    ia_weights: dict[str, float] | None = None
    pca_state: tuple[np.ndarray, np.ndarray] | None = None
    pivot_go_ids: set[str] | frozenset[str] | None = None
    embedding_pool: np.ndarray | None = None
    # lafa-integrate INT-6: the pre-cutoff t0 annotation set id (the SAME set
    # the KNN reference pool was built from). Carries the leakage-clean source
    # the self_prior / association / classifier producers read when the export
    # ``compute_*`` flags are on. ``None`` when no parity feature is requested.
    t0_annotation_set_id: uuid.UUID | None = None
