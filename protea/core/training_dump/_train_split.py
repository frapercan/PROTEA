"""Train-split helpers for the dump pipeline.

Mirror of the test-side helpers in ``_test_split`` but for the per-pair
training loop. Each helper covers one phase of the per-iteration body
so the orchestrator stays under the §3 60-LOC method ceiling. Several
helpers reuse the test-side bundles (``_TestQueryInputs``,
``_TestSequences``) and pure functions (``_compute_test_cat_membership``,
``_load_test_sequences_and_taxonomy``) since the data shape is
identical between sides; a follow-up renames them generically.

Like the test side, predictions are streamed to an intermediate parquet
(``StreamOutput``) and read back in pyarrow batches to write the
per-category labeled shards. That keeps peak memory at roughly one batch
instead of the previous list-of-dicts + DataFrame materialisation (which
peaked near 95 GB on the largest snapshot pairs).
"""

from __future__ import annotations

import gc
import uuid
from pathlib import Path
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy.orm import Session

from protea.core._training_dump_loaders import (
    _build_skipped_outcome,
    _collect_cat_gt_pairs,
    _TestQueryInputs,
    _TestSequences,
    _TrainSplitContext,
    _TrainSplitOutcome,
)
from protea.core.contracts.operation import EmitFn
from protea.core.evaluation import load_evaluation_data_for_set
from protea.core.reranker import LABEL_COLUMN
from protea.core.training_dump._constants import _CATEGORIES
from protea.core.training_dump._contexts import (
    KnnTransferContext,
    SequenceContext,
    StreamOutput,
)
from protea.core.training_dump._data_loaders import _build_reference_from_cache
from protea.core.training_dump._knn_transfer import _knn_transfer_and_label
from protea.core.training_dump._test_split import (
    _compute_test_cat_membership,
    _load_test_sequences_and_taxonomy,
)
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet


def _resolve_train_split_eval(
    session: Session,
    ctx: _TrainSplitContext,
    v_old: int,
    v_new: int,
) -> Any:
    """Look up the EvaluationSet for a train pair and load its delta.

    Raises ``RuntimeError`` if the eval set is missing because the dump
    pipeline assumes the deltas were materialized beforehand.
    """
    old_set_id = ctx.version_to_set[v_old]
    new_set_id = ctx.version_to_set[v_new]
    eset = (
        session.query(EvaluationSet)
        .filter_by(
            old_annotation_set_id=old_set_id,
            new_annotation_set_id=new_set_id,
        )
        .one_or_none()
    )
    if eset is None:
        raise RuntimeError(
            f"EvaluationSet missing for train pair ({v_old}->{v_new}). "
            "Materialize it via scripts/materialize_lab_intervals.py "
            "or POST /annotations/evaluation-sets/generate before retrying."
        )
    eval_data, _ = load_evaluation_data_for_set(session, eset)
    return eval_data


def _prepare_split_query_inputs(
    session: Session,
    ctx: _TrainSplitContext,
    old_set_id: uuid.UUID,
    all_query_accessions: set[str],
    emit: EmitFn,
) -> _TestQueryInputs:
    """Build references and the query-embedding slice for one split."""
    ref_by_aspect = _build_reference_from_cache(
        session,
        old_set_id,
        ctx.embedding_pool,
        ctx.all_accessions,
        ctx.acc_to_idx,
        emit,
    )
    query_accs = [a for a in all_query_accessions if a in ctx.acc_to_idx]
    query_indices = np.array([ctx.acc_to_idx[a] for a in query_accs], dtype=np.int32)
    query_emb = (
        ctx.embedding_pool[query_indices].astype(np.float32)
        if len(query_indices) > 0
        else np.empty((0, ctx.embedding_pool.shape[1]), dtype=np.float32)
    )
    return _TestQueryInputs(ref_by_aspect=ref_by_aspect, valid=query_accs, emb=query_emb)


def _build_train_knn_context(
    ctx: _TrainSplitContext,
    q_inputs: _TestQueryInputs,
    eval_data: Any,
    t0_set_id: uuid.UUID | None,
) -> KnnTransferContext:
    """Assemble the per-split KNN context for the train side.

    Passes ``pivot_go_ids`` so the runner's ``_emit`` filters to the pivot
    universe inline (replacing the old explicit post-filter on the returned
    list, value-preserving for the row set).
    """
    return KnnTransferContext(
        valid_queries=q_inputs.valid,
        query_emb=q_inputs.emb,
        ref_by_aspect=q_inputs.ref_by_aspect,
        go_id_map=ctx.go_id_map,
        aspect_map=ctx.aspect_map,
        gt_pairs=set(),
        query_known_gos=eval_data.known,
        parent_map_str=ctx.parent_map,  # unconditional: lineage producer needs it
        ia_weights=ctx.ia_weights,
        pca_state=ctx.pca_state,
        pivot_go_ids=ctx.pivot_go_ids,
        embedding_pool=ctx.embedding_pool,
        # INT-6: t0 set for the optional self_prior/association/classifier
        # parity features (the pre-cutoff reference version's set).
        t0_annotation_set_id=t0_set_id,
    )


def _stream_train_predictions(
    session: Session,
    payload: Any,
    knn_ctx: KnnTransferContext,
    sequences: _TestSequences,
    output_path: Path,
) -> dict[str, Any]:
    """Run KNN+transfer for one train split, streaming rows to ``output_path``.

    Returns the runner's ``{"parquet_path", "n_rows"}`` info dict; ``n_rows``
    is the post-pivot row count (the old ``len(unlabeled_preds)``).
    """
    info = _knn_transfer_and_label(
        session,
        payload,
        knn_ctx,
        sequence_context=SequenceContext(
            query_sequences=sequences.query_sequences,
            ref_sequences=sequences.ref_sequences,
            query_tax_ids=sequences.query_tax_ids,
            ref_tax_ids=sequences.ref_tax_ids,
        ),
        stream_output=StreamOutput(output_parquet=output_path),
    )
    return cast("dict[str, Any]", info)


def _write_labeled_train_batches(
    pf: pq.ParquetFile,
    project_cols: list[str],
    membership: dict[str, set[tuple[str, str]]],
    cat_gt_pairs: dict[str, set[tuple[str, str]]],
    cat_paths: dict[str, Path],
    split_stats: dict[str, Any],
) -> dict[str, Path]:
    """Stream pyarrow batches into per-cat labeled shards. Mutates ``split_stats``.

    Mirrors ``_write_labeled_test_batches`` but also tallies per-category
    positives/negatives into ``split_stats`` (every category gets a
    ``{cat}_positives`` / ``{cat}_negatives`` entry, 0 when the cat has no
    rows) and returns ``{cat: path}`` for the cats that were written.
    """
    pos: dict[str, int] = {cat: 0 for cat in _CATEGORIES}
    neg: dict[str, int] = {cat: 0 for cat in _CATEGORIES}
    cat_writers: dict[str, pq.ParquetWriter] = {}
    written: dict[str, Path] = {}
    try:
        for batch in pf.iter_batches(batch_size=200_000, columns=project_cols):
            if LABEL_COLUMN in batch.schema.names:
                batch = batch.drop_columns([LABEL_COLUMN])
            accs = batch.column("protein_accession").to_pylist()
            asps = batch.column("aspect").to_pylist()
            for cat in _CATEGORIES:
                members = membership[cat]
                mask_list = [(a, asp) in members for a, asp in zip(accs, asps, strict=False)]
                if not any(mask_list):
                    continue
                mask_arr = pa.array(mask_list, type=pa.bool_())
                cat_batch = batch.filter(mask_arr)
                cat_accs = cat_batch.column("protein_accession").to_pylist()
                cat_gids = cat_batch.column("go_id").to_pylist()
                gt_p = cat_gt_pairs[cat]
                label_vals = [
                    1 if (a, g) in gt_p else 0 for a, g in zip(cat_accs, cat_gids, strict=False)
                ]
                labels = pa.array(label_vals, type=pa.int8())
                cat_batch = cat_batch.append_column(LABEL_COLUMN, labels)
                table = pa.Table.from_batches([cat_batch])
                if cat not in cat_writers:
                    cat_writers[cat] = pq.ParquetWriter(str(cat_paths[cat]), table.schema)
                    written[cat] = cat_paths[cat]
                cat_writers[cat].write_table(table)
                n_pos = sum(label_vals)
                pos[cat] += n_pos
                neg[cat] += len(label_vals) - n_pos
    finally:
        for w in cat_writers.values():
            w.close()
    for cat in _CATEGORIES:
        split_stats[f"{cat}_positives"] = pos[cat]
        split_stats[f"{cat}_negatives"] = neg[cat]
    return written


def _label_and_write_train_split_shards(
    unlabeled_path: Path,
    ctx: _TrainSplitContext,
    cat_gt_pairs: dict[str, set[tuple[str, str]]],
    eval_data: Any,
    split_index: int,
    split_stats: dict[str, Any],
) -> dict[str, Path]:
    """Fan a streamed unlabeled train parquet out into per-category shards.

    Reads the intermediate parquet back in pyarrow batches and routes each
    batch to per-category ``train_{cat}_split{i}.parquet`` writers, so the
    full record set never materialises in memory. Mutates ``split_stats``
    with per-category positive/negative counts; unlinks the intermediate
    parquet once the writers close.
    """
    pf = pq.ParquetFile(str(unlabeled_path))
    project_cols = [c for c in ctx.keep_cols if c in pf.schema_arrow.names]
    membership = _compute_test_cat_membership(eval_data, ctx.go_id_map, ctx.aspect_map)
    cat_paths: dict[str, Path] = {
        cat: ctx.tmp_dir / f"train_{cat}_split{split_index}.parquet" for cat in _CATEGORIES
    }
    written = _write_labeled_train_batches(
        pf, project_cols, membership, cat_gt_pairs, cat_paths, split_stats
    )
    unlabeled_path.unlink(missing_ok=True)
    return written


def _finalize_train_split(
    unlabeled_path: Path,
    ctx: _TrainSplitContext,
    cat_gt_pairs: dict[str, set[tuple[str, str]]],
    eval_data: Any,
    split_index: int,
    split_stats: dict[str, Any],
) -> dict[str, Path]:
    """Label the streamed parquet (or zero-fill stats when it is empty).

    Reads the row count from ``split_stats["total_unlabeled"]`` so the
    caller does not branch: a non-empty parquet is fanned out into the
    per-category shards; an empty one is unlinked and every category gets
    a zeroed positive/negative count.
    """
    n_rows = int(split_stats.get("total_unlabeled", 0))
    if n_rows > 0 and unlabeled_path.exists():
        return _label_and_write_train_split_shards(
            unlabeled_path, ctx, cat_gt_pairs, eval_data, split_index, split_stats
        )
    unlabeled_path.unlink(missing_ok=True)
    for cat in _CATEGORIES:
        split_stats[f"{cat}_positives"] = 0
        split_stats[f"{cat}_negatives"] = 0
    return {}


def _emit_split_skipped(emit: EmitFn, split_num: int, reason: str) -> None:
    """Emit the audit-trail event for a skipped training split."""
    emit(
        "dump_helper.split_skipped",
        None,
        {"split": split_num, "reason": reason},
        "warning",
    )


def _run_train_split(
    session: Session,
    ctx: _TrainSplitContext,
    split_index: int,
    emit: EmitFn,
) -> _TrainSplitOutcome:
    """Run one training-split iteration end-to-end."""
    p = ctx.payload
    v_old = p.train_versions[split_index]
    v_new = p.train_versions[split_index + 1]
    emit(
        "dump_helper.split_start",
        None,
        {"split": split_index + 1, "v_old": v_old, "v_new": v_new},
        "info",
    )
    eval_data = _resolve_train_split_eval(session, ctx, v_old, v_new)
    cat_gt_pairs, all_query_accessions = _collect_cat_gt_pairs(eval_data)
    if not all_query_accessions:
        _emit_split_skipped(emit, split_index + 1, "no ground truth in any category")
        return _build_skipped_outcome(v_old, v_new, "no ground truth")
    q_inputs = _prepare_split_query_inputs(
        session, ctx, ctx.version_to_set[v_old], all_query_accessions, emit
    )
    if not q_inputs.valid:
        _emit_split_skipped(emit, split_index + 1, "no query embeddings")
        gc.collect()
        return _build_skipped_outcome(v_old, v_new, "no query embeddings")
    sequences = _load_test_sequences_and_taxonomy(
        session, p, q_inputs.valid, q_inputs.ref_by_aspect
    )
    session.expire_all()
    unlabeled_path = ctx.tmp_dir / f"train_unlabeled_split{split_index}.parquet"
    knn_ctx = _build_train_knn_context(ctx, q_inputs, eval_data, ctx.version_to_set[v_old])
    info = _stream_train_predictions(session, p, knn_ctx, sequences, unlabeled_path)
    gc.collect()
    split_stats: dict[str, Any] = {
        "v_old": v_old,
        "v_new": v_new,
        "skipped": False,
        "total_unlabeled": int(info.get("n_rows", 0)),
    }
    cat_paths = _finalize_train_split(
        unlabeled_path, ctx, cat_gt_pairs, eval_data, split_index, split_stats
    )
    session.expunge_all()
    gc.collect()
    emit("dump_helper.split_done", None, split_stats, "info")
    return _TrainSplitOutcome(split_files=cat_paths, stats=split_stats, skipped=False)
