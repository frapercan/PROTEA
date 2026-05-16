"""Reference-pool loading mixin for ``PredictGOTermsBatchOperation``.

Holds every method that materialises and slices the per-aspect / unified
reference embedding caches. Pulled out of the monolithic
``predict_go_terms.py`` as part of T2B.6 so the file budget stays under
the §3 ceiling.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

import numpy as np
from sqlalchemy.orm import Session

from protea.core.annotation_intern import intern_string
from protea.core.contracts.operation import EmitFn
from protea.core.disk_cache import (
    _aspect_index_path,
    _build_anno_csr,
    _derive_reference_views,
    _load_anno_csr_from_disk,
    _load_reference_pool_cached,
    _save_anno_csr_to_disk,
)
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS
from protea.core.operations.predict_go_terms._common import (
    _REF_CACHE,
    _UNIFIED_REF_KEY,
)
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.protein.protein import Protein

if TYPE_CHECKING:
    from protea.core.operations.predict_go_terms._common import _BatchExecCtx  # noqa: F401


class _ReferenceMixin:
    """Reference-pool cache management methods.

    The mixin holds no state of its own; all methods are pure or call
    into the per-class helpers (``_load_annotations_for`` lives on the
    feature mixin). Concrete instantiation happens via
    :class:`PredictGOTermsBatchOperation` which combines this mixin with
    the feature + reranker mixins.
    """

    def _ensure_reference_cache(
        self, session: Session, ctx: Any, emit: EmitFn
    ) -> Any:
        """Load (or reuse) the per-process reference embedding cache for this
        ``(embedding_config, annotation_set, aspect_separated_knn)`` triple.

        The cache key includes ``aspect_separated_knn`` so switching modes on
        the same worker does not serve stale data from a previous run. When
        the cache is full, the oldest entry is evicted to free numpy memory.
        """
        p = ctx.p
        cache_key = (p.embedding_config_id, p.annotation_set_id, p.aspect_separated_knn)
        if cache_key not in _REF_CACHE:
            from protea.config.tuning import get_tuning

            cache_max = get_tuning().worker.ref_cache_max
            if len(_REF_CACHE) >= cache_max:
                evict_key = next(iter(_REF_CACHE))
                del _REF_CACHE[evict_key]
            emit(
                "predict_go_terms_batch.loading_reference",
                None,
                {
                    "embedding_config_id": p.embedding_config_id,
                    "annotation_set_id": p.annotation_set_id,
                    "aspect_separated_knn": p.aspect_separated_knn,
                },
                "info",
            )
            if p.aspect_separated_knn:
                _REF_CACHE[cache_key] = self._load_reference_data_per_aspect(
                    session, ctx.embedding_config_id, ctx.annotation_set_id, emit
                )
            else:
                _REF_CACHE[cache_key] = self._load_reference_data(
                    session, ctx.embedding_config_id, ctx.annotation_set_id, emit
                )
        return _REF_CACHE[cache_key]

    def _load_reference_data(
        self,
        session: Session,
        embedding_config_id: uuid.UUID,
        annotation_set_id: uuid.UUID,
        emit: EmitFn,
    ) -> dict[str, Any]:
        """Load reference accessions and embeddings (float16) into the process cache.

        Disk cache first (survives worker restarts); falls back to a
        streamed DB load on miss / count-mismatch. A cheap accession-only
        ``COUNT(*)`` runs every call to detect drift after a reference
        re-ingest. GO annotations are loaded lazily per batch elsewhere.
        """
        emit("predict_go_terms_batch.load_references_start", None, {}, "info")
        source = "disk_cache"
        accession_q = self._reference_pool_query(
            session, embedding_config_id, annotation_set_id
        )

        def _db_loader() -> tuple[list[str], np.ndarray]:
            nonlocal source
            source = "database"
            return self._stream_reference_pool(accession_q)

        accessions, embeddings = _load_reference_pool_cached(
            embedding_config_id,
            annotation_set_id,
            _db_loader,
            expected_count=accession_q.count(),
            emit=lambda ev, fields: emit(
                f"predict_go_terms_batch.{ev}", None, fields, "info"
            ),
        )
        emit(
            "predict_go_terms_batch.load_references_done",
            None,
            {
                "references": len(accessions),
                "embeddings_mb": round(embeddings.nbytes / 1024 / 1024),
                "source": source,
            },
            "info",
        )
        return _derive_reference_views(accessions, embeddings)

    def _reference_pool_query(
        self,
        session: Session,
        embedding_config_id: uuid.UUID,
        annotation_set_id: uuid.UUID,
    ) -> Any:
        """Return the accession-only query for the reference pool (see PR #354)."""
        annotated_sq = (
            session.query(ProteinGOAnnotation.protein_accession)
            .filter(ProteinGOAnnotation.annotation_set_id == annotation_set_id)
            .distinct()
            .subquery()
        )
        return (
            session.query(Protein.accession)
            .join(
                SequenceEmbedding,
                (SequenceEmbedding.sequence_id == Protein.sequence_id)
                & (SequenceEmbedding.embedding_config_id == embedding_config_id),
            )
            .join(annotated_sq, Protein.accession == annotated_sq.c.protein_accession)
        )

    def _stream_reference_pool(
        self,
        accession_q: Any,
    ) -> tuple[list[str], np.ndarray]:
        """Stream ``(accession, embedding)`` rows into a pre-allocated f16 matrix."""
        total = accession_q.count()
        if total == 0:
            return [], np.empty((0,), dtype=np.float16)
        base_q = accession_q.add_columns(SequenceEmbedding.embedding)
        # pgvector HalfVector exposes .dimensions() / .to_numpy() after the
        # 2026-04-11 halfvec migration; pull dimension from the first row.
        dim = base_q.limit(1).one()[1].dimensions()
        from protea.config.tuning import get_tuning

        stream_chunk = get_tuning().operation.stream_chunk_size
        embeddings = np.empty((total, dim), dtype=np.float16)
        accessions: list[str] = []
        for i, (acc, emb) in enumerate(base_q.yield_per(stream_chunk)):
            embeddings[i] = emb.to_numpy()
            accessions.append(acc)
        return accessions, embeddings

    def _load_reference_data_per_aspect(
        self,
        session: Session,
        embedding_config_id: uuid.UUID,
        annotation_set_id: uuid.UUID,
        emit: EmitFn,
    ) -> dict[str, dict[str, Any]]:
        """Build per-aspect reference views as numpy slices over a single unified cache.

        One ~1 GB float16 embedding array + three int32 index arrays (~2 MB each)
        live on disk. Every per-aspect view is a fancy-index slice into the unified
        array, no embedding duplication. See :meth:`_query_and_persist_aspect_caches`
        for the on-disk layout and the (one-shot) DB scan path.
        """
        emit(
            "predict_go_terms_batch.load_references_per_aspect_start",
            None,
            {
                "embedding_config_id": str(embedding_config_id),
                "annotation_set_id": str(annotation_set_id),
            },
            "info",
        )

        unified = self._load_reference_data(session, embedding_config_id, annotation_set_id, emit)
        if not unified["accessions"]:
            empty = {
                asp: _derive_reference_views([], np.empty((0,), dtype=np.float16))
                for asp in _ASPECTS
            }
            empty[_UNIFIED_REF_KEY] = unified
            return empty

        acc_to_idx: dict[str, int] = {acc: i for i, acc in enumerate(unified["accessions"])}
        missing_aspects = self._find_missing_aspects(embedding_config_id, annotation_set_id)
        if missing_aspects:
            self._query_and_persist_aspect_caches(
                session,
                missing_aspects,
                unified["accessions"],
                acc_to_idx,
                embedding_config_id,
                annotation_set_id,
            )

        result, total_refs = self._assemble_all_aspect_views(
            unified, missing_aspects, embedding_config_id, annotation_set_id, emit
        )
        result[_UNIFIED_REF_KEY] = unified  # F2C.5b: pipeline.predict() needs unified pool
        emit(
            "predict_go_terms_batch.load_references_per_aspect_all_done",
            None,
            {"total_references": total_refs},
            "info",
        )
        return result

    def _assemble_all_aspect_views(
        self,
        unified: dict[str, Any],
        missing_aspects: list[str],
        embedding_config_id: uuid.UUID,
        annotation_set_id: uuid.UUID,
        emit: EmitFn,
    ) -> tuple[dict[str, dict[str, Any]], int]:
        """Slice the unified cache into per-aspect views, emit one ``done`` event
        per aspect, and return ``(views, total_refs)``."""
        result: dict[str, dict[str, Any]] = {}
        total_refs = 0
        for aspect in _ASPECTS:
            view, n_refs = self._assemble_aspect_view(
                aspect, unified, embedding_config_id, annotation_set_id
            )
            result[aspect] = view
            total_refs += n_refs
            emit(
                "predict_go_terms_batch.load_references_per_aspect_done",
                None,
                {
                    "aspect": aspect,
                    "references": n_refs,
                    "source": "database" if aspect in missing_aspects else "disk_cache",
                },
                "info",
            )
        return result, total_refs

    @staticmethod
    def _find_missing_aspects(
        embedding_config_id: uuid.UUID, annotation_set_id: uuid.UUID
    ) -> list[str]:
        """Return aspects whose index file or annotation CSR is absent on disk.
        These still need the DB query path to repopulate the on-disk cache."""
        return [
            asp
            for asp in _ASPECTS
            if not _aspect_index_path(embedding_config_id, annotation_set_id, asp).exists()
            or _load_anno_csr_from_disk(embedding_config_id, annotation_set_id, asp) is None
        ]

    @staticmethod
    def _query_and_persist_aspect_caches(
        session: Session,
        missing_aspects: list[str],
        unified_accessions: list[str],
        acc_to_idx: dict[str, int],
        embedding_config_id: uuid.UUID,
        annotation_set_id: uuid.UUID,
    ) -> None:
        """Single-pass DB scan + per-aspect on-disk persistence for the missing aspects.

        Collects ``(aspect_to_accset, aspect_to_go_map)`` via
        :meth:`_collect_aspect_annotations`, then writes the index array +
        annotation CSR for every missing aspect. Downstream batches read the
        CSR straight from disk; zero per-batch IN queries.
        """
        # Resolve the (possibly test-patched) ``_collect_aspect_annotations``
        # via the concrete subclass so ``patch.object(PredictGOTermsBatchOperation,
        # "_collect_aspect_annotations", ...)`` keeps wiring through. Lazy
        # import avoids the cycle since ``_batch_op`` already imports this
        # mixin at top level.
        from protea.core.operations.predict_go_terms._batch_op import (
            PredictGOTermsBatchOperation,
        )

        aspect_to_accset, aspect_to_go_map = (
            PredictGOTermsBatchOperation._collect_aspect_annotations(
                session, missing_aspects, annotation_set_id
            )
        )
        for asp in missing_aspects:
            idx_path = _aspect_index_path(embedding_config_id, annotation_set_id, asp)
            indices = np.array(
                [acc_to_idx[acc] for acc in aspect_to_accset[asp] if acc in acc_to_idx],
                dtype=np.int32,
            )
            idx_path.parent.mkdir(parents=True, exist_ok=True)
            np.save(idx_path, indices)
            asp_accessions = [unified_accessions[i] for i in indices]
            anno_csr = _build_anno_csr(asp_accessions, aspect_to_go_map[asp])
            _save_anno_csr_to_disk(embedding_config_id, annotation_set_id, asp, anno_csr)

    @staticmethod
    def _collect_aspect_annotations(
        session: Session,
        missing_aspects: list[str],
        annotation_set_id: uuid.UUID,
    ) -> tuple[
        dict[str, set[str]],
        dict[str, dict[str, list[dict[str, Any]]]],
    ]:
        """Single-pass query that fetches every ``ProteinGOAnnotation`` row for
        the missing aspects in one table scan, returning per-aspect accession
        sets + per-protein GO maps. Skips ``NOT`` qualifiers."""
        aspect_to_accset: dict[str, set[str]] = {asp: set() for asp in missing_aspects}
        aspect_to_go_map: dict[str, dict[str, list[dict[str, Any]]]] = {
            asp: {} for asp in missing_aspects
        }
        rows = (
            session.query(
                ProteinGOAnnotation.protein_accession,
                GOTerm.aspect,
                ProteinGOAnnotation.go_term_id,
                ProteinGOAnnotation.qualifier,
                ProteinGOAnnotation.evidence_code,
            )
            .join(ProteinGOAnnotation.go_term)
            .filter(
                ProteinGOAnnotation.annotation_set_id == annotation_set_id,
                GOTerm.aspect.in_(missing_aspects),
                (
                    ProteinGOAnnotation.qualifier.is_(None)
                    | ~ProteinGOAnnotation.qualifier.like("%NOT%")
                ),
            )
            .yield_per(50_000)
        )
        for acc, asp, go_term_id, qualifier, evidence_code in rows:
            if asp not in aspect_to_accset:
                continue
            aspect_to_accset[asp].add(acc)
            aspect_to_go_map[asp].setdefault(acc, []).append(
                {
                    "go_term_id": go_term_id,
                    # Flyweight; see ``protea.core.annotation_intern``.
                    "qualifier": intern_string(qualifier),
                    "evidence_code": intern_string(evidence_code),
                }
            )
        return aspect_to_accset, aspect_to_go_map

    @staticmethod
    def _assemble_aspect_view(
        aspect: str,
        unified: dict[str, Any],
        embedding_config_id: uuid.UUID,
        annotation_set_id: uuid.UUID,
    ) -> tuple[dict[str, Any], int]:
        """Slice the unified embeddings by the aspect's on-disk index array and
        attach the CSR annotation cache. Returns ``(view, n_refs)``."""
        idx_path = _aspect_index_path(embedding_config_id, annotation_set_id, aspect)
        indices = np.load(idx_path)
        aspect_accessions = [unified["accessions"][i] for i in indices]
        view: dict[str, Any] = {
            "accessions": aspect_accessions,
            "embeddings": unified["embeddings"][indices],
            "embeddings_f32": unified["embeddings_f32"][indices],
            "embeddings_f32_cos": unified["embeddings_f32_cos"][indices],
        }
        anno_csr = _load_anno_csr_from_disk(embedding_config_id, annotation_set_id, aspect)
        if anno_csr is not None:
            gtids, quals, ecodes, offsets = anno_csr
            view.update(
                {
                    "anno_gtids": gtids,
                    "anno_quals": quals,
                    "anno_ecodes": ecodes,
                    "anno_offsets": offsets,
                    "acc_to_anno_idx": {acc: i for i, acc in enumerate(aspect_accessions)},
                }
            )
        return view, len(indices)
