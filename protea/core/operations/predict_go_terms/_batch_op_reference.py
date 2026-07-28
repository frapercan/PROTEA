"""Reference-pool loading mixin for ``PredictGOTermsBatchOperation``.

Holds every method that materialises and slices the per-aspect / unified
reference embedding caches. Pulled out of the monolithic
``predict_go_terms.py`` as part of T2B.6 so the file budget stays under
the §3 ceiling.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, NamedTuple

import numpy as np
from sqlalchemy.orm import Session

from protea.core.annotation_intern import intern_string
from protea.core.contracts.operation import EmitFn
from protea.core.disk_cache import (
    RefPoolKey,
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


class _PoolRequest(NamedTuple):
    """What pool a run wants: which embeddings, which annotations, whose donors.

    The three travel together through every step that materialises the pool,
    and the donor policy decides both which proteins the query admits and where
    the result is cached. Passing them as one value is what stops a restricted
    pool being filtered on read but stored under the unrestricted name.
    """

    embedding_config_id: uuid.UUID
    annotation_set_id: uuid.UUID
    donor_policy: Any = None

    @property
    def discriminator(self) -> str:
        """The cache discriminator this policy implies, empty when permissive."""
        return "" if self.donor_policy is None else self.donor_policy.cache_discriminator()

    @property
    def cache_key(self) -> RefPoolKey:
        """The on-disk identity, derived rather than assembled by each caller."""
        return RefPoolKey(self.embedding_config_id, self.annotation_set_id, self.discriminator)


def _restrict_annotations(annotations: Any, donor_policy: Any) -> Any:
    """Narrow the donating annotations to those the policy admits.

    The two restrictions that act on the ANNOTATION rather than on the protein
    sit here together, so they are read together.
    """
    if donor_policy is None:
        return annotations

    codes = getattr(donor_policy, "evidence_codes", None)
    if codes:
        annotations = annotations.filter(ProteinGOAnnotation.evidence_code.in_(list(codes)))

    for prefix in getattr(donor_policy, "exclude_reference_prefixes", ()) or ():
        # A row with no reference is not evidence of the excluded kind, so it
        # stays. Left to SQL alone, NOT LIKE against a null drops it silently.
        annotations = annotations.filter(
            (ProteinGOAnnotation.db_reference.is_(None))
            | (~ProteinGOAnnotation.db_reference.startswith(prefix))
        )
    return annotations


class _ReferenceMixin:
    """Reference-pool cache management methods.

    The mixin holds no state of its own; all methods are pure or call
    into the per-class helpers (``_load_annotations_for`` lives on the
    feature mixin). Concrete instantiation happens via
    :class:`PredictGOTermsBatchOperation` which combines this mixin with
    the feature + reranker mixins.
    """

    def _ensure_reference_cache(self, session: Session, ctx: Any, emit: EmitFn) -> Any:
        """Load (or reuse) the per-process reference embedding cache for this
        ``(embedding_config, annotation_set, aspect_separated_knn)`` triple.

        The cache key includes ``aspect_separated_knn`` so switching modes on
        the same worker does not serve stale data from a previous run. It also
        includes ``need_cos`` / ``need_plain`` so a cosine run and a non-cosine
        run never share a pool that materialised only one metric's f32 copy.
        When the cache is full, the oldest entry is evicted to free numpy memory.
        """
        p = ctx.p
        need_cos, need_plain = self._reference_dtype_needs(p)
        request = _PoolRequest(
            ctx.embedding_config_id, ctx.annotation_set_id, getattr(p, "donor_policy", None)
        )
        cache_key = self._process_cache_key(p, request, need_cos=need_cos, need_plain=need_plain)
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
                    session, request, emit, need_cos=need_cos, need_plain=need_plain
                )
            else:
                _REF_CACHE[cache_key] = self._load_reference_data(
                    session, request, emit, need_cos=need_cos, need_plain=need_plain
                )
        return _REF_CACHE[cache_key]

    @staticmethod
    def _process_cache_key(
        p: Any,
        request: _PoolRequest,
        *,
        need_cos: bool,
        need_plain: bool,
    ) -> tuple[Any, ...]:
        """The in-process cache key for one reference pool.

        Everything that changes WHAT the cached object contains belongs here.
        The donor policy does, exactly as it does on disk: without it, switching
        policies on a warm worker serves the previous policy's pool from memory
        and nothing says so.

        Built in one place rather than at each site that needs it, because a key
        spelled twice is a key that can disagree with itself, and the disagreement
        is silent by construction.
        """
        return (
            p.embedding_config_id,
            p.annotation_set_id,
            p.aspect_separated_knn,
            need_cos,
            need_plain,
            request.discriminator,
        )

    @staticmethod
    def _reference_dtype_needs(p: Any) -> tuple[bool, bool]:
        """Decide which f32 reference copies this run actually reads.

        ``need_cos`` (the L2-normalised copy) is read by the KNN search when
        the metric is cosine; ``need_plain`` (the raw f32 copy) is read by the
        KNN search otherwise, and is also the pool the embedding-PCA fit
        consumes. The PCA state is cached on disk per embedding config and the
        fit ignores the pool once cached, so the raw copy is only needed when
        the PCA artefact is still missing. Skipping the unread copy keeps
        high-dim PLMs (ESM2-3B, 2560 dims) inside RAM.
        """
        from protea.core.pca_cache import _pca_state_path

        need_cos = getattr(p, "metric", "cosine") == "cosine"
        pca_cached = _pca_state_path(p.embedding_config_id).exists()
        need_plain = (not need_cos) or (not pca_cached)
        return need_cos, need_plain

    def _load_reference_data(
        self,
        session: Session,
        request: _PoolRequest,
        emit: EmitFn,
        *,
        need_cos: bool = True,
        need_plain: bool = True,
    ) -> dict[str, Any]:
        """Load reference accessions and embeddings (float16) into the process cache.

        Disk cache first (survives worker restarts); falls back to a streamed DB
        load on miss / count-mismatch. When the disk cache files are fresh (mtime
        within ``ref_cache_freshness_seconds``), the expensive COUNT(*)-over-JOIN
        is skipped entirely. GO annotations are loaded lazily per batch elsewhere.
        ``need_cos`` / ``need_plain`` control which f32 copies are materialised.
        """
        emit("predict_go_terms_batch.load_references_start", None, {}, "info")
        accessions, embeddings, source = self._load_ref_pool(session, request, emit)
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
        return _derive_reference_views(
            accessions, embeddings, need_cos=need_cos, need_plain=need_plain
        )

    def _load_ref_pool(
        self,
        session: Session,
        request: _PoolRequest,
        emit: EmitFn,
    ) -> tuple[list[str], np.ndarray, str]:
        """Fetch ``(accessions, embeddings, source)`` via disk cache or DB.

        When the cache files are fresh (mtime within
        ``ref_cache_freshness_seconds``), the COUNT(*)-over-JOIN is skipped
        so the fast path requires zero DB round-trips. ``source`` is
        ``"disk_cache"`` on hit and ``"database"`` on miss.
        """
        from protea.config.tuning import get_tuning
        from protea.core.disk_cache import _is_cache_fresh

        freshness_sec = get_tuning().operation.ref_cache_freshness_seconds
        source = "disk_cache"
        # The policy decides both WHICH proteins may donate and WHERE the pool
        # is cached. Deriving the key from the same object that filters the
        # query is what stops a restricted pool being stored under, or read
        # from, the unrestricted name.
        key = request.cache_key
        accession_q = self._reference_pool_query(session, request)

        def _db_loader() -> tuple[list[str], np.ndarray]:
            nonlocal source
            source = "database"
            return self._stream_reference_pool(accession_q)

        if freshness_sec > 0 and _is_cache_fresh(
            request.embedding_config_id,
            request.annotation_set_id,
            freshness_sec,
            request.discriminator,
        ):
            expected: int | None = None
        else:
            expected = accession_q.count()

        accessions, embeddings = _load_reference_pool_cached(
            key,
            _db_loader,
            expected_count=expected,
            emit=lambda ev, fields: emit(f"predict_go_terms_batch.{ev}", None, fields, "info"),
            freshness_seconds=freshness_sec,
        )
        return accessions, embeddings, source

    def _reference_pool_query(self, session: Session, request: _PoolRequest) -> Any:
        """Return the accession-only query for the reference pool.

        ``donor_policy`` restricts which proteins may donate annotations. The
        permissive default keeps the historical pool, every protein carrying an
        embedding and an annotation in the set, so an unrestricted run resolves
        exactly the query it always did.

        The restriction is applied HERE, in the query, rather than by filtering
        the materialised pool afterwards. Filtering afterwards would mean
        streaming the whole pool out of the database in order to discard part of
        it, and the pool is the largest thing this operation touches.
        """
        policy = request.donor_policy
        annotations = session.query(ProteinGOAnnotation.protein_accession).filter(
            ProteinGOAnnotation.annotation_set_id == request.annotation_set_id
        )
        annotations = _restrict_annotations(annotations, policy)
        annotated_sq = annotations.distinct().subquery()

        query = (
            session.query(Protein.accession)
            .join(
                SequenceEmbedding,
                (SequenceEmbedding.sequence_id == Protein.sequence_id)
                & (SequenceEmbedding.embedding_config_id == request.embedding_config_id),
            )
            .join(annotated_sq, Protein.accession == annotated_sq.c.protein_accession)
        )
        if policy is not None and getattr(policy, "reviewed_only", False):
            # ``reviewed`` is nullable, and an unknown review status is not a
            # reviewed one. Comparing to true excludes the nulls, which is the
            # conservative reading and the one the caller asked for: a reviewed
            # spine that quietly included proteins of unknown provenance would
            # not be a reviewed spine.
            query = query.filter(Protein.reviewed.is_(True))
        return query

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
        request: _PoolRequest,
        emit: EmitFn,
        *,
        need_cos: bool = True,
        need_plain: bool = True,
    ) -> dict[str, dict[str, Any]]:
        """Build per-aspect reference views as numpy slices over a single unified cache.

        One ~1 GB float16 embedding array + three int32 index arrays (~2 MB each)
        live on disk. Every per-aspect view is a fancy-index slice into the unified
        array, no embedding duplication. See :meth:`_query_and_persist_aspect_caches`
        for the on-disk layout and the (one-shot) DB scan path. ``need_cos`` /
        ``need_plain`` propagate to each per-aspect slice so only the f32 copies
        this run reads get materialised.
        """
        emit(
            "predict_go_terms_batch.load_references_per_aspect_start",
            None,
            {
                "embedding_config_id": str(request.embedding_config_id),
                "annotation_set_id": str(request.annotation_set_id),
            },
            "info",
        )

        unified = self._load_reference_data(
            session, request, emit, need_cos=need_cos, need_plain=need_plain
        )
        if not unified["accessions"]:
            empty = {
                asp: _derive_reference_views([], np.empty((0,), dtype=np.float16))
                for asp in _ASPECTS
            }
            empty[_UNIFIED_REF_KEY] = unified
            return empty

        acc_to_idx: dict[str, int] = {acc: i for i, acc in enumerate(unified["accessions"])}
        missing_aspects = self._find_missing_aspects(request.embedding_config_id, request.annotation_set_id)
        if missing_aspects:
            self._query_and_persist_aspect_caches(
                session,
                missing_aspects,
                unified["accessions"],
                acc_to_idx,
                request.embedding_config_id,
                request.annotation_set_id,
            )

        result, total_refs = self._assemble_all_aspect_views(
            unified,
            missing_aspects,
            request.embedding_config_id,
            request.annotation_set_id,
            emit,
            need_cos=need_cos,
            need_plain=need_plain,
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
        *,
        need_cos: bool = True,
        need_plain: bool = True,
    ) -> tuple[dict[str, dict[str, Any]], int]:
        """Slice the unified cache into per-aspect views, emit one ``done`` event
        per aspect, and return ``(views, total_refs)``."""
        result: dict[str, dict[str, Any]] = {}
        total_refs = 0
        for aspect in _ASPECTS:
            view, n_refs = self._assemble_aspect_view(
                aspect,
                unified,
                embedding_config_id,
                annotation_set_id,
                need_cos=need_cos,
                need_plain=need_plain,
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
        *,
        need_cos: bool = True,
        need_plain: bool = True,
    ) -> tuple[dict[str, Any], int]:
        """Slice the unified embeddings by the aspect's on-disk index array and
        attach the CSR annotation cache. Returns ``(view, n_refs)``.

        Each fancy-index slice is a full copy, so for high-dim PLMs a per-aspect
        f32 view is tens of GB. Only the copies this run reads are materialised
        (``need_cos`` → the cosine view, ``need_plain`` → the raw view); the f16
        slice is never read downstream and is left as an empty placeholder.
        """
        idx_path = _aspect_index_path(embedding_config_id, annotation_set_id, aspect)
        indices = np.load(idx_path)
        aspect_accessions = [unified["accessions"][i] for i in indices]
        unified_f32 = unified.get("embeddings_f32")
        unified_cos = unified.get("embeddings_f32_cos")
        view: dict[str, Any] = {
            "accessions": aspect_accessions,
            "embeddings": np.empty((0,), dtype=np.float16),
            "embeddings_f32": (
                unified_f32[indices] if need_plain and unified_f32 is not None else None
            ),
            "embeddings_f32_cos": (
                unified_cos[indices] if need_cos and unified_cos is not None else None
            ),
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
