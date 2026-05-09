"""Bulk DB loaders extracted out of ``training_dump_helpers``.

Three private helpers used by the dump pipeline:

* :func:`_count_embeddings_with_dim`: single COUNT + ``vector_dims``
  probe for a given embedding-config UUID.
* :func:`_stream_embeddings`: pre-allocated ``np.empty`` matrix filled
  via ``yield_per`` over the ``sequence_embedding`` join.
* :func:`_load_annotation_aggregations`: per-aspect grouping of
  annotation rows under the GO ``P/F/C`` aspects, filtering against a
  provided ``acc_to_idx`` so only proteins with a preloaded embedding
  contribute to the reference set.

All three are intentionally I/O-only: they take a SQLAlchemy
``Connection`` (already opened by the caller) and return plain Python /
NumPy structures. Keeping them out of ``training_dump_helpers`` lets
``_preload_all_embeddings`` and ``_build_reference_from_cache`` stay
under the §3 60-LOC method ceiling without the dump-helper file
ballooning further.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, NamedTuple

import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from protea.core.annotation_intern import intern_string
from protea.core.contracts.operation import EmitFn, OperationResult
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS


def _count_embeddings_with_dim(
    conn: Connection, emb_config_id: uuid.UUID
) -> tuple[int, int]:
    """Return ``(total_rows, embedding_dim)`` for a config.

    ``embedding_dim`` falls back to 960 when the lookup probe finds no
    row for the config (e.g. on an empty fixture); callers can rely on
    a sane default rather than ``None`` propagating through the
    pre-allocation step.
    """
    count_row = conn.execute(
        text(
            "SELECT COUNT(*), "
            "       (SELECT vector_dims(se2.embedding) "
            "          FROM sequence_embedding se2 "
            "         WHERE se2.embedding_config_id = :ecid LIMIT 1) "
            "  FROM protein p "
            "  JOIN sequence_embedding se "
            "    ON se.sequence_id = p.sequence_id "
            "   AND se.embedding_config_id = :ecid"
        ),
        {"ecid": emb_config_id},
    ).one()
    total = int(count_row[0])
    dim = int(count_row[1]) if count_row[1] else 960
    return total, dim


def _stream_embeddings(
    conn: Connection,
    emb_config_id: uuid.UUID,
    total: int,
    dim: int,
    stream_chunk: int,
) -> tuple[np.ndarray, list[str]]:
    """Stream all rows into a pre-allocated float16 matrix.

    Two-pass strategy (count + stream) is intentional: a single-pass
    ``list.append`` of NumPy rows would balloon memory while the JIT
    grows the buffer; pre-allocation caps the working set at
    ``total * dim * 2`` bytes plus the row cursor.
    """
    embeddings = np.empty((total, dim), dtype=np.float16)
    accessions: list[str] = []
    result_proxy = conn.execute(
        text(
            "SELECT p.accession, se.embedding "
            "  FROM protein p "
            "  JOIN sequence_embedding se "
            "    ON se.sequence_id = p.sequence_id "
            "   AND se.embedding_config_id = :ecid"
        ),
        {"ecid": emb_config_id},
    ).yield_per(stream_chunk)

    for i, (acc, emb_str) in enumerate(result_proxy):
        if isinstance(emb_str, str):
            emb_arr = np.fromstring(emb_str.strip("[]"), sep=",", dtype=np.float16)
        else:
            emb_arr = np.array(emb_str, dtype=np.float16)
        embeddings[i] = emb_arr
        accessions.append(acc)

    return embeddings, accessions


def _load_annotation_aggregations(
    conn: Connection,
    annotation_set_id: uuid.UUID,
    acc_to_idx: dict[str, int],
) -> tuple[
    dict[str, set[str]],
    dict[str, dict[str, list[dict[str, Any]]]],
]:
    """Group annotation rows by GO aspect (``P``/``F``/``C``).

    Returns ``(aspect_accs, aspect_go_map)``:

    * ``aspect_accs[aspect]``: set of accessions with at least one
      annotation of that aspect AND a preloaded embedding (i.e. the
      accession is present in ``acc_to_idx``).
    * ``aspect_go_map[aspect][acc]``: list of GO term records keyed by
      accession. Qualifier and evidence-code strings are interned via
      ``annotation_intern.intern_string`` to avoid duplicating the same
      tokens across millions of rows.

    NOT-qualified rows are skipped at the SQL layer (any qualifier
    containing ``NOT`` is excluded), matching the upstream contract.
    """
    ann_rows = conn.execute(
        text(
            "SELECT pga.protein_accession, gt.aspect, pga.go_term_id, "
            "       pga.qualifier, pga.evidence_code "
            "  FROM protein_go_annotation pga "
            "  JOIN go_term gt ON gt.id = pga.go_term_id "
            " WHERE pga.annotation_set_id = :asid "
            "   AND gt.aspect IN ('P', 'F', 'C') "
            "   AND (pga.qualifier IS NULL OR pga.qualifier NOT LIKE '%%NOT%%')"
        ),
        {"asid": annotation_set_id},
    ).yield_per(50_000)

    aspect_accs: dict[str, set[str]] = {a: set() for a in _ASPECTS}
    aspect_go_map: dict[str, dict[str, list[dict[str, Any]]]] = {a: {} for a in _ASPECTS}
    for acc, asp, go_term_id, qualifier, evidence_code in ann_rows:
        if asp in aspect_accs and acc in acc_to_idx:
            aspect_accs[asp].add(acc)
            aspect_go_map[asp].setdefault(acc, []).append(
                {
                    "go_term_id": go_term_id,
                    "qualifier": intern_string(qualifier),
                    "evidence_code": intern_string(evidence_code),
                }
            )

    return aspect_accs, aspect_go_map


def _resolve_annotation_set_ids(
    session: Session,
    source: str,
    versions: Iterable[int],
) -> tuple[dict[int, uuid.UUID], dict[int, uuid.UUID]]:
    """Resolve ``(version → annotation_set_id, version → native_snapshot_id)``.

    Looks up one ``AnnotationSet`` per version under ``source`` and raises
    ``ValueError`` for any version that lacks a row, so the caller does
    not have to guard the lookup. Used at the start of the dump pipeline
    to translate human-friendly GOA release numbers into the UUIDs the
    downstream queries need.
    """
    from protea.infrastructure.orm.models.annotation.annotation_set import (
        AnnotationSet,
    )

    version_to_set: dict[int, uuid.UUID] = {}
    version_to_native: dict[int, uuid.UUID] = {}
    for v in versions:
        aset = (
            session.query(AnnotationSet)
            .filter(
                AnnotationSet.source == source,
                AnnotationSet.source_version == str(v),
            )
            .first()
        )
        if aset is None:
            raise ValueError(
                f"AnnotationSet not found for source={source!r}, "
                f"source_version={str(v)!r}"
            )
        version_to_set[v] = aset.id
        version_to_native[v] = aset.ontology_snapshot_id
    return version_to_set, version_to_native


def _check_reranker_name_collisions(session: Session, names: Iterable[str]) -> None:
    """Raise ``ValueError`` if any ``RerankerModel`` row already uses ``name``.

    Caller batches every candidate name (``{base}-{cat}`` plus the
    optional per-cell extras) so the dump fails fast before running KNN
    over multiple GOA pairs. ``dump_only`` mode skips this check entirely;
    the caller decides whether to invoke us.
    """
    from protea.infrastructure.orm.models.embedding.reranker_model import (
        RerankerModel,
    )

    for model_name in names:
        existing = (
            session.query(RerankerModel)
            .filter(RerankerModel.name == model_name)
            .first()
        )
        if existing is not None:
            raise ValueError(f"RerankerModel {model_name!r} already exists")


def _load_ia_weights(ia_file: str | None) -> dict[str, float] | None:
    """Parse a CAFA-style IA TSV (``go_id\\tweight`` per line).

    Returns ``None`` when ``ia_file`` is empty/None so the caller can use
    membership in ``ia_weights is not None`` to gate sample-weighting.
    Whitespace lines and rows with fewer than two tab-separated fields
    are silently skipped, matching the reference ``cafaeval`` parser.
    """
    if not ia_file:
        return None
    weights: dict[str, float] = {}
    with open(ia_file) as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) >= 2:
                weights[parts[0]] = float(parts[1])
    return weights


def _load_go_maps(
    session: Session,
    ontology_snapshot_id: uuid.UUID,
    native_snapshots: set[uuid.UUID],
) -> tuple[dict[Any, str], dict[Any, str], set[str]]:
    """Load the ``(id→go_id, id→aspect, pivot_go_ids)`` triple used by the dump.

    ``go_id_map`` and ``aspect_map`` are keyed by the integer
    ``go_term.id`` so reference annotations from ANY native snapshot can
    be resolved back to a ``go_id`` string. ``pivot_go_ids`` is the
    set of GO IDs that exist in the pivot ontology snapshot; predictions
    are later filtered against it so the dump's term universe matches the
    reconciled ground-truth space.
    """
    map_snapshots = {ontology_snapshot_id} | set(native_snapshots)
    union_rows = session.execute(
        text(
            "SELECT id, go_id, aspect FROM go_term WHERE ontology_snapshot_id = ANY(:snap_ids)"
        ),
        {"snap_ids": [str(s) for s in map_snapshots]},
    ).fetchall()
    go_id_map: dict[Any, str] = {row_id: go_id for row_id, go_id, _ in union_rows}
    aspect_map: dict[Any, str] = {
        row_id: aspect for row_id, _, aspect in union_rows if aspect
    }
    pivot_rows = session.execute(
        text(
            "SELECT go_id FROM go_term "
            "WHERE ontology_snapshot_id = :snap_id AND aspect IS NOT NULL"
        ),
        {"snap_id": ontology_snapshot_id},
    ).fetchall()
    pivot_go_ids: set[str] = {row[0] for row in pivot_rows}
    return go_id_map, aspect_map, pivot_go_ids


def _maybe_fit_pca_state(
    embedding_config_id: uuid.UUID,
    all_embeddings: np.ndarray,
    use_pca: bool,
    emit: EmitFn,
) -> tuple[np.ndarray, np.ndarray] | None:
    """Resolve the embedding-PCA state via the shared cache when requested.

    Returns ``None`` for the no-PCA path (either ``use_pca`` is false or
    the preloaded pool is empty) so callers can use ``pca_state is None``
    as the gate for the PCA-projected feature columns. When the cache
    hit produces components, an ``dump_helper.pca_fit`` audit event is
    emitted with the input/output dims so downstream listeners know the
    PCA shape used at training time matches what ``predict_go_terms``
    will use at inference.

    The shared cache is load-bearing: a fresh per-call fit (the legacy
    behaviour) silently produced different components from a randomly
    subsampled pool, breaking ``emb_pca_query_*`` parity for the booster
    even when every other feature was correct.
    """
    if not use_pca or not all_embeddings.size:
        return None
    from protea.core.pca_cache import _load_or_fit_pca_state
    from protea.core.reranker import EMBEDDING_PCA_DIM

    pca_state = _load_or_fit_pca_state(embedding_config_id, all_embeddings)
    emit(
        "dump_helper.pca_fit",
        None,
        {
            "n_refs": int(all_embeddings.shape[0]),
            "dim_in": int(all_embeddings.shape[1]),
            "dim_out": EMBEDDING_PCA_DIM,
            "source": "shared_cache",
        },
        "info",
    )
    return pca_state


class _DumpRequest(NamedTuple):
    """Bundle of state passed to :func:`_perform_dataset_dump`.

    Aggregates the per-split shard list, the test-split shard map and
    the trio of payload-derived UUIDs/strings the parquet exporter
    needs. Frozen NamedTuple so the helper signature stays at 4 params
    (request + dump_fn + t0 + emit) and well under the §3 ceiling.
    """

    payload: Any
    split_files: dict[str, list[Path]]
    valid_split_versions: list[tuple[int, int]]
    test_files: dict[str, Path | None]
    test_old_v: int
    test_new_v: int
    emb_config_id: uuid.UUID
    ontology_snapshot_id: uuid.UUID


def _perform_dataset_dump(
    request: _DumpRequest,
    dump_fn: Callable[[Any], dict[str, Any]],
    t0: float,
    emit: EmitFn,
) -> OperationResult:
    """Run the frozen-parquet dump and shape the success ``OperationResult``.

    Raises ``ValueError`` when ``payload.dump_to`` is unset because the
    legacy in-process LightGBM training path was moved to
    ``protea-reranker-lab``; every caller must now target a dump path
    (``ExportResearchDatasetOperation`` always does).

    ``dump_fn`` is the (caller-owned) shim around
    ``export_reranker_parquets`` -- typically
    ``TrainRerankerAutoOperation._dump_frozen_dataset``. Passing it in
    lets the helper stay free of the ``ParquetExportContext`` import
    at module-load time.
    """
    p = request.payload
    if not p.dump_to:
        raise ValueError(
            "dump_helper requires dump_to: LightGBM training has been "
            "moved to protea-reranker-lab. Use "
            "ExportResearchDatasetOperation / POST /datasets."
        )
    from protea import __version__ as _protea_version
    from protea.core.parquet_export import (
        ParquetExportContext,
        resolve_protea_git_sha,
    )

    dump_stats = dump_fn(
        ParquetExportContext(
            stage_dir=Path(p.dump_to),
            split_files=request.split_files,
            valid_split_versions=request.valid_split_versions,
            test_files=request.test_files,
            test_old_v=request.test_old_v,
            test_new_v=request.test_new_v,
            name=p.name,
            k=int(p.limit_per_entry),
            embedding_config_id=str(request.emb_config_id),
            ontology_snapshot_id=str(request.ontology_snapshot_id),
            annotation_source=p.annotation_source,
            store=None,
            producer_version=_protea_version,
            producer_git_sha=resolve_protea_git_sha(),
        )
    )
    emit("dump_helper.dump_done", None, dump_stats, "info")
    elapsed = round(time.perf_counter() - t0, 1)
    result: dict[str, Any] = {
        "dumped": True,
        "dump_path": str(p.dump_to),
        "dump_stats": dump_stats,
        "elapsed_seconds": elapsed,
    }
    emit("dump_helper.done", None, result, "info")
    return OperationResult(result=result)


def _collect_cat_gt_pairs(
    eval_data: Any,
) -> tuple[dict[str, set[tuple[str, str]]], set[str]]:
    """Flatten the per-category ground-truth into ``(protein, go_id)`` pairs.

    Returns ``(cat_gt_pairs, all_query_accessions)``:

    * ``cat_gt_pairs[cat]`` is the set of every ground-truth pair across
      proteins in that category, used to derive the row-level label
      column for the per-category training shards.
    * ``all_query_accessions`` is the union of proteins across NK/LK/PK,
      used as the KNN-query set for the split.

    Identical shape on the train side and the test side; pulling the
    flatten loop here removes a 10-line code-duplication smell. Caller
    looks up ``eval_data.nk`` / ``eval_data.lk`` / ``eval_data.pk`` via
    ``getattr`` against the lowercase category strings.
    """
    cat_gt_pairs: dict[str, set[tuple[str, str]]] = {}
    all_query_accessions: set[str] = set()
    for cat_attr in ("nk", "lk", "pk"):
        gt: dict[str, set[str]] = getattr(eval_data, cat_attr)
        pairs: set[tuple[str, str]] = set()
        for protein, go_ids in gt.items():
            for go_id in go_ids:
                pairs.add((protein, go_id))
        cat_gt_pairs[cat_attr] = pairs
        all_query_accessions.update(gt.keys())
    return cat_gt_pairs, all_query_accessions


class _TestQueryInputs(NamedTuple):
    """Computed inputs for the test-split KNN call.

    Built by ``_prepare_test_query_inputs`` from ``_TestSplitContext``;
    bundles the per-aspect reference set, the surviving query
    accessions and the matching float32 embedding matrix so the KNN
    helper does not need a 7-arg signature.
    """

    ref_by_aspect: dict[str, dict[str, Any]]
    valid: list[str]
    emb: np.ndarray


class _TestSequences(NamedTuple):
    """Optional sequence and taxonomy lookups for the test split.

    All four fields default to ``None``; populated by
    ``_load_test_sequences_and_taxonomy`` only when the payload sets
    ``compute_alignments`` or ``compute_taxonomy``. Mirrors the field
    layout of ``SequenceContext`` so the KNN helper can pass it
    through without rewrapping.
    """

    query_sequences: dict[str, str] | None
    ref_sequences: dict[str, str] | None
    query_tax_ids: dict[str, int | None] | None
    ref_tax_ids: dict[str, int | None] | None


class _TestSplitContext(NamedTuple):
    """Bundle of inputs threaded through :func:`_run_test_split`.

    The test-split body needs sixteen pieces of state pulled from
    ``TrainRerankerAutoOperation.execute``: the payload, eval data
    derived from :func:`_resolve_test_eval_inputs` /
    :func:`_collect_cat_gt_pairs`, the preloaded embedding pool, the
    GO maps, optional enrichment maps (parent / IA / PCA), the GO
    pivot universe and a couple of in-memory paths. NamedTuple keeps
    the helper's signature at three params (session + ctx + emit) and
    well under the §3 6-arg ceiling without prescribing field order at
    every call site.
    """

    payload: Any
    test_eval_data: Any
    test_cat_gt: dict[str, set[tuple[str, str]]]
    test_all_queries: set[str]
    test_old_set_id: uuid.UUID
    embedding_pool: np.ndarray
    all_accessions: list[str]
    acc_to_idx: dict[str, int]
    go_id_map: dict[Any, str]
    aspect_map: dict[Any, str]
    parent_map: dict[str, set[str]] | None
    ia_weights: dict[str, float] | None
    pca_state: tuple[np.ndarray, np.ndarray] | None
    pivot_go_ids: set[str]
    keep_cols: list[str]
    tmp_dir: Path


def _resolve_test_eval_inputs(
    session: Session,
    train_versions: list[int],
    test_versions: list[int],
    version_to_set: dict[int, uuid.UUID],
) -> tuple[Any, Any, int, int]:
    """Resolve and load the test-pair ``EvaluationSet`` + its ``EvaluationData``.

    Picks the last training version as the "old" side of the test pair
    (all train splits up to and including that version are seen during
    training) and the first test version as the "new" side. Looks up
    the persisted ``EvaluationSet`` row keyed by the
    ``(old_set_id, new_set_id)`` pair; raises ``RuntimeError`` if it is
    missing because the dump pipeline assumes the eval set was
    materialized beforehand (``scripts/materialize_lab_intervals.py`` or
    ``POST /annotations/evaluation-sets/generate``).

    Returns ``(test_eset, test_eval_data, test_old_v, test_new_v)`` so
    the caller has both the ORM row (for downstream FK lookups, e.g.
    ``test_old_set_id = version_to_set[test_old_v]``) and the eagerly-
    loaded delta data.
    """
    from protea.core.evaluation import load_evaluation_data_for_set
    from protea.infrastructure.orm.models.annotation.evaluation_set import (
        EvaluationSet,
    )

    test_old_v = train_versions[-1]
    test_new_v = test_versions[0]
    test_old_set_id = version_to_set[test_old_v]
    test_new_set_id = version_to_set[test_new_v]

    test_eset = (
        session.query(EvaluationSet)
        .filter_by(
            old_annotation_set_id=test_old_set_id,
            new_annotation_set_id=test_new_set_id,
        )
        .one_or_none()
    )
    if test_eset is None:
        raise RuntimeError(
            f"EvaluationSet missing for test pair ({test_old_v}->{test_new_v}). "
            "Materialize it via scripts/materialize_lab_intervals.py "
            "or POST /annotations/evaluation-sets/generate before retrying."
        )
    test_eval_data, _ = load_evaluation_data_for_set(session, test_eset)
    return test_eset, test_eval_data, test_old_v, test_new_v
