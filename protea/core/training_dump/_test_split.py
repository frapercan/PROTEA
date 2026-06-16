"""Test-split helpers for the dump pipeline.

Originally part of ``protea/core/training_dump_helpers.py``. Extracted
(T2B.6) so the split phase modules stay under the file-LOC ceiling.
Behaviour is unchanged; the helpers still mutate the orchestrator's
``test_files`` map in place where they did before.
"""

from __future__ import annotations

import gc
from pathlib import Path
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy.orm import Session

from protea.core._training_dump_loaders import (
    _TestQueryInputs,
    _TestSequences,
    _TestSplitContext,
)
from protea.core.contracts.operation import EmitFn
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS
from protea.core.reranker import LABEL_COLUMN
from protea.core.training_dump._constants import _CATEGORIES
from protea.core.training_dump._contexts import (
    KnnTransferContext,
    SequenceContext,
    StreamOutput,
)
from protea.core.training_dump._data_loaders import (
    _build_reference_from_cache,
    _load_sequences,
    _load_taxonomy_ids,
)
from protea.core.training_dump._knn_transfer import _knn_transfer_and_label
from protea.core.training_dump._payload import TrainRerankerAutoPayload


def _prepare_test_query_inputs(
    session: Session,
    ctx: _TestSplitContext,
    emit: EmitFn,
) -> _TestQueryInputs:
    """Resolve the test-pair reference set + query embeddings."""
    test_ref = _build_reference_from_cache(
        session,
        ctx.test_old_set_id,
        ctx.embedding_pool,
        ctx.all_accessions,
        ctx.acc_to_idx,
        emit,
    )
    test_accs = [a for a in ctx.test_all_queries if a in ctx.acc_to_idx]
    test_indices = np.array([ctx.acc_to_idx[a] for a in test_accs], dtype=np.int32)
    test_emb = (
        ctx.embedding_pool[test_indices].astype(np.float32)
        if len(test_indices) > 0
        else np.empty((0, ctx.embedding_pool.shape[1]), dtype=np.float32)
    )
    return _TestQueryInputs(ref_by_aspect=test_ref, valid=test_accs, emb=test_emb)


def _load_test_sequences_and_taxonomy(
    session: Session,
    payload: TrainRerankerAutoPayload,
    valid_queries: list[str],
    test_ref: dict[str, dict[str, Any]],
) -> _TestSequences:
    """Optionally fetch sequence + taxonomy lookups for the test split."""
    if not (payload.compute_alignments or payload.compute_taxonomy):
        return _TestSequences(None, None, None, None)
    test_ref_accs: set[str] = set()
    for asp in _ASPECTS:
        test_ref_accs.update(test_ref[asp]["accessions"])
    test_query_set = set(valid_queries)
    qs = _load_sequences(session, test_query_set) if payload.compute_alignments else None
    rs = _load_sequences(session, test_ref_accs) if payload.compute_alignments else None
    qt = _load_taxonomy_ids(session, test_query_set) if payload.compute_taxonomy else None
    rt = _load_taxonomy_ids(session, test_ref_accs) if payload.compute_taxonomy else None
    return _TestSequences(qs, rs, qt, rt)


def _stream_test_predictions(
    session: Session,
    ctx: _TestSplitContext,
    q_inputs: _TestQueryInputs,
    sequences: _TestSequences,
    output_path: Path,
) -> dict[str, Any]:
    """Run KNN+transfer for the test split, streaming rows to ``output_path``."""
    p = ctx.payload
    info = _knn_transfer_and_label(
        session,
        p,
        KnnTransferContext(
            valid_queries=q_inputs.valid,
            query_emb=q_inputs.emb,
            ref_by_aspect=q_inputs.ref_by_aspect,
            go_id_map=ctx.go_id_map,
            aspect_map=ctx.aspect_map,
            gt_pairs=set(),
            query_known_gos=ctx.test_eval_data.known,
            parent_map_str=ctx.parent_map,  # unconditional: lineage producer needs it
            ia_weights=ctx.ia_weights,
            pca_state=ctx.pca_state,
            pivot_go_ids=ctx.pivot_go_ids,
            embedding_pool=ctx.embedding_pool,
            # INT-6: t0 set for the optional parity features (the pre-cutoff
            # test reference version's annotation set).
            t0_annotation_set_id=ctx.test_old_set_id,
        ),
        sequence_context=SequenceContext(
            query_sequences=sequences.query_sequences,
            ref_sequences=sequences.ref_sequences,
            query_tax_ids=sequences.query_tax_ids,
            ref_tax_ids=sequences.ref_tax_ids,
        ),
        stream_output=StreamOutput(output_parquet=output_path),
    )
    return cast("dict[str, Any]", info)


def _compute_test_cat_membership(
    eval_data: Any,
    go_id_map: dict[Any, str],
    aspect_map: dict[Any, str],
) -> dict[str, set[tuple[str, str]]]:
    """Derive per-cat ``(protein, aspect)`` membership from eval data."""
    aspect_by_go_id: dict[str, str] = {
        go_id: aspect_map[term_id]
        for term_id, go_id in go_id_map.items()
        if term_id in aspect_map
    }
    membership: dict[str, set[tuple[str, str]]] = {}
    for cat in _CATEGORIES:
        gt = getattr(eval_data, cat)
        members: set[tuple[str, str]] = set()
        for protein, go_ids in gt.items():
            for go_id in go_ids:
                asp = aspect_by_go_id.get(go_id, "")
                if asp:
                    members.add((protein, asp))
        membership[cat] = members
    return membership


def _write_labeled_test_batches(
    pf: pq.ParquetFile,
    project_cols: list[str],
    membership: dict[str, set[tuple[str, str]]],
    test_cat_gt: dict[str, set[tuple[str, str]]],
    cat_paths: dict[str, Path],
) -> set[str]:
    """Stream pyarrow batches into per-cat labeled shards. Returns cats written."""
    cat_writers: dict[str, pq.ParquetWriter] = {}
    try:
        for batch in pf.iter_batches(batch_size=200_000, columns=project_cols):
            if LABEL_COLUMN in batch.schema.names:
                batch = batch.drop_columns([LABEL_COLUMN])
            accs = batch.column("protein_accession").to_pylist()
            asps = batch.column("aspect").to_pylist()
            for cat in _CATEGORIES:
                members = membership[cat]
                mask_list = [
                    (a, asp) in members for a, asp in zip(accs, asps, strict=False)
                ]
                if not any(mask_list):
                    continue
                mask_arr = pa.array(mask_list, type=pa.bool_())
                cat_batch = batch.filter(mask_arr)
                cat_accs = cat_batch.column("protein_accession").to_pylist()
                cat_gids = cat_batch.column("go_id").to_pylist()
                gt_p = test_cat_gt[cat]
                labels = pa.array(
                    [
                        1 if (a, g) in gt_p else 0
                        for a, g in zip(cat_accs, cat_gids, strict=False)
                    ],
                    type=pa.int8(),
                )
                cat_batch = cat_batch.append_column(LABEL_COLUMN, labels)
                table = pa.Table.from_batches([cat_batch])
                if cat not in cat_writers:
                    cat_writers[cat] = pq.ParquetWriter(
                        str(cat_paths[cat]), table.schema
                    )
                cat_writers[cat].write_table(table)
    finally:
        for w in cat_writers.values():
            w.close()
    return set(cat_writers.keys())


def _label_test_split_per_category(
    unlabeled_path: Path,
    ctx: _TestSplitContext,
    test_files: dict[str, Path | None],
) -> None:
    """Fan a streamed unlabeled test parquet out into per-category shards.

    Mutates ``test_files[cat]`` in place: the orchestrator owns the
    dict so the no-data branch can leave it as the all-``None`` initial
    map. The intermediate parquet is unlinked once the writers close.
    """
    pf = pq.ParquetFile(str(unlabeled_path))
    project_cols = [c for c in ctx.keep_cols if c in pf.schema_arrow.names]
    membership = _compute_test_cat_membership(
        ctx.test_eval_data, ctx.go_id_map, ctx.aspect_map
    )
    cat_paths: dict[str, Path] = {
        cat: ctx.tmp_dir / f"test_{cat}.parquet" for cat in _CATEGORIES
    }
    written = _write_labeled_test_batches(
        pf, project_cols, membership, ctx.test_cat_gt, cat_paths
    )
    for cat in written:
        test_files[cat] = cat_paths[cat]
    unlabeled_path.unlink(missing_ok=True)


def _run_test_split(
    session: Session,
    ctx: _TestSplitContext,
    emit: EmitFn,
) -> dict[str, Path | None]:
    """Run KNN once for the test pair and label per category.

    Streams predictions to an intermediate parquet so the per-cat
    labeled shards can be written without materialising ~10M records
    in memory. Returns ``{cat: Path | None}``; ``None`` for cats with
    no rows in the test pair.
    """
    test_files: dict[str, Path | None] = {c: None for c in _CATEGORIES}
    if not ctx.test_all_queries:
        return test_files
    q_inputs = _prepare_test_query_inputs(session, ctx, emit)
    if not q_inputs.valid:
        gc.collect()
        return test_files
    sequences = _load_test_sequences_and_taxonomy(
        session, ctx.payload, q_inputs.valid, q_inputs.ref_by_aspect
    )
    session.expire_all()
    test_unlabeled_path = ctx.tmp_dir / "test_unlabeled.parquet"
    info = _stream_test_predictions(session, ctx, q_inputs, sequences, test_unlabeled_path)
    gc.collect()
    n_rows = int(info.get("n_rows", 0))
    if n_rows > 0 and test_unlabeled_path.exists():
        _label_test_split_per_category(test_unlabeled_path, ctx, test_files)
    gc.collect()
    return test_files
