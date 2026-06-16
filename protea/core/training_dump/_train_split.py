"""Train-split helpers for the dump pipeline.

Mirror of the test-side helpers in ``_test_split`` but for the per-pair
training loop. Each helper covers one phase of the per-iteration body
so the orchestrator stays under the §3 60-LOC method ceiling. Several
helpers reuse the test-side bundles (``_TestQueryInputs``,
``_TestSequences``) and pure functions (``_compute_test_cat_membership``,
``_load_test_sequences_and_taxonomy``) since the data shape is
identical between sides; a follow-up renames them generically.
"""

from __future__ import annotations

import gc
import uuid
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd
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
from protea.core.training_dump._contexts import KnnTransferContext, SequenceContext
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
    query_indices = np.array(
        [ctx.acc_to_idx[a] for a in query_accs], dtype=np.int32
    )
    query_emb = (
        ctx.embedding_pool[query_indices].astype(np.float32)
        if len(query_indices) > 0
        else np.empty((0, ctx.embedding_pool.shape[1]), dtype=np.float32)
    )
    return _TestQueryInputs(
        ref_by_aspect=ref_by_aspect, valid=query_accs, emb=query_emb
    )


def _knn_and_filter_to_pivot(
    session: Session,
    ctx: _TrainSplitContext,
    q_inputs: _TestQueryInputs,
    eval_data: Any,
    sequences: _TestSequences,
    t0_set_id: uuid.UUID | None = None,
) -> list[dict[str, Any]]:
    """Run KNN+transfer for one train split and filter to the pivot universe."""
    p = ctx.payload
    raw_preds = _knn_transfer_and_label(
        session,
        p,
        KnnTransferContext(
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
            embedding_pool=ctx.embedding_pool,
            # INT-6: t0 set for the optional self_prior/association/classifier
            # parity features (the pre-cutoff reference version's set).
            t0_annotation_set_id=t0_set_id,
        ),
        sequence_context=SequenceContext(
            query_sequences=sequences.query_sequences,
            ref_sequences=sequences.ref_sequences,
            query_tax_ids=sequences.query_tax_ids,
            ref_tax_ids=sequences.ref_tax_ids,
        ),
    )
    return cast(
        "list[dict[str, Any]]",
        [r for r in raw_preds if r["go_id"] in ctx.pivot_go_ids],  # type: ignore[index]
    )


def _label_and_write_train_split_shards(
    unlabeled_preds: list[dict[str, Any]],
    ctx: _TrainSplitContext,
    cat_gt_pairs: dict[str, set[tuple[str, str]]],
    eval_data: Any,
    split_index: int,
    split_stats: dict[str, Any],
) -> dict[str, Path]:
    """Label rows per category, write parquet shards. Mutates ``split_stats``."""
    base_df = pd.DataFrame(unlabeled_preds, columns=ctx.keep_cols)
    membership = _compute_test_cat_membership(
        eval_data, ctx.go_id_map, ctx.aspect_map
    )
    cat_paths: dict[str, Path] = {}
    for cat in _CATEGORIES:
        members = membership[cat]
        cat_mask = np.fromiter(
            (
                (acc, asp) in members
                for acc, asp in zip(
                    base_df["protein_accession"],
                    base_df["aspect"],
                    strict=False,
                )
            ),
            count=len(base_df),
            dtype=bool,
        )
        cat_df = base_df.loc[cat_mask].copy()
        if cat_df.empty:
            split_stats[f"{cat}_positives"] = 0
            split_stats[f"{cat}_negatives"] = 0
            continue
        gt_p = cat_gt_pairs[cat]
        labels = np.fromiter(
            (
                1 if (acc, go_id) in gt_p else 0
                for acc, go_id in zip(
                    cat_df["protein_accession"],
                    cat_df["go_id"],
                    strict=False,
                )
            ),
            count=len(cat_df),
            dtype=np.int8,
        )
        cat_df[LABEL_COLUMN] = labels
        n_pos = int(labels.sum())
        split_stats[f"{cat}_positives"] = n_pos
        split_stats[f"{cat}_negatives"] = len(cat_df) - n_pos
        pq_path = ctx.tmp_dir / f"train_{cat}_split{split_index}.parquet"
        cat_df.to_parquet(pq_path, index=False)
        cat_paths[cat] = pq_path
    return cat_paths


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
    unlabeled_preds = _knn_and_filter_to_pivot(
        session, ctx, q_inputs, eval_data, sequences, ctx.version_to_set[v_old]
    )
    gc.collect()
    split_stats: dict[str, Any] = {
        "v_old": v_old,
        "v_new": v_new,
        "skipped": False,
        "total_unlabeled": len(unlabeled_preds),
    }
    cat_paths = _label_and_write_train_split_shards(
        unlabeled_preds, ctx, cat_gt_pairs, eval_data, split_index, split_stats
    )
    session.expunge_all()
    gc.collect()
    emit("dump_helper.split_done", None, split_stats, "info")
    return _TrainSplitOutcome(split_files=cat_paths, stats=split_stats, skipped=False)
