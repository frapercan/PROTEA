"""Reference / sequence / taxonomy loaders for the dump pipeline.

Originally lived in ``protea/core/training_dump_helpers.py``. Extracted
to a leaf submodule (T2B.6) so the orchestration code can import the
loaders without pulling in the entire split runner. Behaviour is byte
identical.
"""

from __future__ import annotations

import uuid
from typing import Any

import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core._training_dump_loaders import (
    _count_embeddings_with_dim,
    _load_annotation_aggregations,
    _stream_embeddings,
)
from protea.core.contracts.operation import EmitFn
from protea.core.disk_cache import _load_reference_pool_cached
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence


def _load_parent_map(session: Session, snapshot_id: uuid.UUID) -> dict[str, set[str]]:
    """Return ``{child_go_id: {parent_go_id, ...}}`` for is_a + part_of edges.

    Used to drive True-Path-Rule max-propagation of predicted scores
    before computing Fmax / AUC-PR, so the internal training-time
    metric matches what cafaeval reports externally.
    """
    rows = session.execute(
        text(
            "SELECT c.go_id AS child, p.go_id AS parent "
            "FROM go_term_relationship r "
            "JOIN go_term c ON c.id = r.child_go_term_id "
            "JOIN go_term p ON p.id = r.parent_go_term_id "
            "WHERE r.ontology_snapshot_id = :snap_id "
            "AND r.relation_type IN ('is_a', 'part_of')"
        ),
        {"snap_id": snapshot_id},
    ).fetchall()
    parent_map: dict[str, set[str]] = {}
    for child, parent in rows:
        parent_map.setdefault(str(child), set()).add(str(parent))
    return parent_map


# bulk embedding preload (used by dump_helper)

# Nil UUID: the dump preload spans every annotation set for a config.
_DUMP_ANN_SET_SENTINEL = uuid.UUID(int=0)


def _preload_all_embeddings(
    session: Session,
    emb_config_id: uuid.UUID,
    emit: EmitFn,
) -> tuple[np.ndarray, list[str], dict[str, int]]:
    """Load ALL embeddings once (disk-cached) and return (embs, accs, idx)."""
    from protea.config.tuning import get_tuning

    conn = session.connection()
    total, dim = _count_embeddings_with_dim(conn, emb_config_id)
    emit("dump_helper.preloading_embeddings", None, {"total": total, "dim": dim}, "info")

    def _db_loader() -> tuple[list[str], np.ndarray]:
        stream_chunk = get_tuning().operation.stream_chunk_size
        embs, accs = _stream_embeddings(conn, emb_config_id, total, dim, stream_chunk)
        return accs, embs

    accessions, embeddings = _load_reference_pool_cached(
        emb_config_id,
        _DUMP_ANN_SET_SENTINEL,
        _db_loader,
        expected_count=total,
        emit=lambda ev, fields: emit(f"dump_helper.{ev}", None, fields, "info"),
    )
    acc_to_idx = {acc: i for i, acc in enumerate(accessions)}
    emit(
        "dump_helper.embeddings_preloaded",
        None,
        {
            "total": len(accessions),
            "dim": dim,
            "memory_mb": round(embeddings.nbytes / 1024 / 1024, 1),
        },
        "info",
    )
    return embeddings, accessions, acc_to_idx


def _build_reference_from_cache(
    session: Session,
    annotation_set_id: uuid.UUID,
    all_embeddings: np.ndarray,
    all_accessions: list[str],
    acc_to_idx: dict[str, int],
    emit: EmitFn,
) -> dict[str, dict[str, Any]]:
    """Build per-aspect reference data using preloaded embeddings.

    Only loads annotations from the DB (fast, small rows), then filters
    the preloaded embedding matrix in memory.
    """
    conn = session.connection()
    aspect_accs, aspect_go_map = _load_annotation_aggregations(
        conn, annotation_set_id, acc_to_idx
    )

    result: dict[str, dict[str, Any]] = {}
    for asp in _ASPECTS:
        indices = np.array(
            [acc_to_idx[a] for a in aspect_accs[asp]],
            dtype=np.int32,
        )
        asp_accessions = [all_accessions[i] for i in indices]
        # Store indices into the shared preload pool instead of a fancy-indexed
        # copy: with ~500k refs x 1024 x float16, the copy was ~1 GB per aspect
        # and stayed alive until the split ended. The consumer
        # (_knn_transfer_and_label) materialises a transient float32 view on
        # demand, one aspect at a time.
        result[asp] = {
            "accessions": asp_accessions,
            "indices": indices,
            "go_map": aspect_go_map[asp],
        }
        emit(
            "dump_helper.aspect_loaded",
            None,
            {"aspect": asp, "references": len(indices)},
            "info",
        )

    return result


def _load_sequences(
    session: Session,
    accessions: set[str],
) -> dict[str, str]:
    from protea.config.tuning import get_tuning

    chunk_size = get_tuning().operation.annotation_chunk_size
    result: dict[str, str] = {}
    acc_list = list(accessions)
    for i in range(0, len(acc_list), chunk_size):
        chunk = acc_list[i : i + chunk_size]
        rows = (
            session.query(Protein.accession, Sequence.sequence)
            .join(Protein.sequence)
            .filter(Protein.accession.in_(chunk))
            .all()
        )
        for acc, seq in rows:
            result[acc] = seq
    return result


def _load_taxonomy_ids(
    session: Session,
    accessions: set[str],
) -> dict[str, int | None]:
    from protea.config.tuning import get_tuning

    chunk_size = get_tuning().operation.annotation_chunk_size
    result: dict[str, int | None] = {}
    acc_list = list(accessions)
    for i in range(0, len(acc_list), chunk_size):
        chunk = acc_list[i : i + chunk_size]
        rows = (
            session.query(Protein.accession, Protein.taxonomy_id)
            .filter(Protein.accession.in_(chunk))
            .all()
        )
        for acc, tid in rows:
            result[acc] = int(tid) if tid else None
    return result
