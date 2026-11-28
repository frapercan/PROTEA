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
    _aspect_index_pool_path,
    _build_anno_csr,
    _derive_reference_views,
    _load_reference_pool_cached,
    _save_anno_csr_to_disk,
    pool_fingerprint,
)
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS
from protea.core.operations.predict_go_terms._aspect_caches import (
    BuiltAspect,
    _assemble_aspect_view,
    _find_missing_aspects,
)
from protea.core.operations.predict_go_terms._bank_shards import fill_from_shards
from protea.core.operations.predict_go_terms._common import (
    _REF_CACHE,
    _UNIFIED_REF_KEY,
)
from protea.core.operations.predict_go_terms._photo_sequences import donor_pool_base
from protea.core.operations.predict_go_terms._pool_admission import (
    ChannelSource,
    admit_channels,
    admit_donor_set,
    channels_of_the_cut,
    positive_rows,
)
from protea.core.operations.predict_go_terms._reference_binary import fill_from_binary
from protea.core.operations.predict_go_terms._reference_needs import reference_dtype_needs
from protea.core.operations.predict_go_terms._search_space import SearchSpace, search_space
from protea.core.operations.predict_go_terms._sparse_bank import (
    bank_bytes,
    bank_is_sparse,
    empty_sparse_bank,
    stream_sparse_pool,
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

    ``space`` is the declared search space (``._search_space``), None for the
    configuration's own. It changes the float32 copies the search reads and is
    part of the in-process key, never of the disk key: the disk cache holds the
    raw pool, which every space of it reuses.
    """

    embedding_config_id: uuid.UUID
    annotation_set_id: uuid.UUID
    donor_policy: Any = None
    space: SearchSpace | None = None

    @property
    def discriminator(self) -> str:
        """The cache discriminator this policy implies, empty when permissive."""
        return "" if self.donor_policy is None else self.donor_policy.cache_discriminator()

    @property
    def cache_key(self) -> RefPoolKey:
        """The on-disk identity, derived rather than assembled by each caller."""
        return RefPoolKey(self.embedding_config_id, self.annotation_set_id, self.discriminator)


def _restrict_annotations(
    annotations: Any, donor_policy: Any, channels_at: ChannelSource | None = None
) -> Any:
    """Narrow the donating annotations to those the policy admits.

    The three restrictions that act on the ANNOTATION rather than on the protein
    sit here together, so they are read together: the evidence codes, the
    excluded reference prefixes and the evidence channels; combined, all apply.
    ``channels_at`` is where the channels are read
    (:func:`._pool_admission.channels_of_the_cut`), and a policy naming channels
    without it is refused, so a reader cannot drop the channels silently.
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

    channels = getattr(donor_policy, "evidence_channels", None)
    if channels:
        annotations = admit_channels(annotations, channels, channels_at)
    return annotations


def _bank_order(accession: Any = Protein.accession) -> tuple[Any, ...]:
    """The ORDER BY the donor-pool query carries, and why it is a result.

    91 per cent of queries have neighbours at distance ~0 -- themselves, or twins
    carrying the same sequence -- so candidate terms tie at the k-th slot and the
    search admits whichever donor comes FIRST in the bank. Without an ORDER BY
    that is whatever the plan emits. MEASURED on the 219 bank, 142.422 rows,
    identical rows every time:

        hash join                          89b79ad15383
        merge join (enable_hashjoin=off)   cf77c750bed5
        the same rows sorted by accession  cf77c750bed5

    So the plan alone moved ``f_micro_w`` by ~1e-3 per cell, concentrated in CCO
    and with mixed signs, and the plan turns on statistics and ``work_mem``. The
    disk cache froze one order and hid it: an identical re-run reproduced to the
    pair, while a run after the cache expired would not have.

    ``COLLATE "C"``, not the database default. This Postgres runs ``en_US.utf8``,
    whose glibc collation ignores punctuation on the first pass: it puts
    ``A0A18`` before ``A0A1-9`` and ``P0DTC21`` before ``P0DTC2-3``.
    :func:`_aspect_indices` pins its half with Python's ``sorted``, which is
    codepoint order, so the default collation would leave the two halves of this
    disagreeing with each other -- and would not be the order an external
    reproduction picks unprompted either. ``C`` is codepoint.

    ``chunk_index_s`` second, because accession alone is not a total order.
    ``sequence_embedding`` is one row per (sequence, config, chunk_start), so
    under ``use_chunking`` every accession appears once per chunk and those rows
    tie under a sole accession key -- and near-duplicate chunks of one protein
    are the MOST tie-prone entries in the bank, which is the case this ordering
    exists for. No config in this campaign chunks today, so it costs nothing now
    and removes a trap later.
    """
    return (accession.collate("C"), SequenceEmbedding.chunk_index_s)


def _pin_annotation_order(
    aspect_to_go_map: dict[str, dict[str, list[dict[str, Any]]]],
) -> None:
    """Sort every per-donor annotation list in place, deterministically.

    Same defect as the pool's, one level down. The lists are appended in whatever
    order the plan emits, :func:`_build_anno_csr` flattens them into the CSR
    positionally and ``_csr_lookup`` reads them back in that order, so the order
    freezes into the on-disk CSR exactly as the pool order did. It reaches the
    output because a candidate takes its ``evidence_code`` from the first
    matching row, and the code multiplies the score.

    Sorted here rather than with an ORDER BY because the query that fills these
    scans 11.160.704 rows for release 219, and a sort that size on this Postgres
    is the ``work_mem`` spill pathology. These lists hold tens of entries each,
    so this is linear in rows already in memory.
    """
    for asp_map in aspect_to_go_map.values():
        for anns in asp_map.values():
            anns.sort(
                key=lambda a: (
                    a["go_term_id"],
                    a["evidence_code"] or "",
                    a["qualifier"] or "",
                )
            )


def _aspect_indices(accessions: Any, acc_to_idx: dict[str, int]) -> np.ndarray:
    """Map an aspect's accessions onto unified-pool indices, IN ACCESSION ORDER.

    The sort is load-bearing, not tidiness. This array IS the per-aspect bank
    order, and candidate terms tie at the k-th neighbour often enough for the
    order to decide which donors are admitted: 91 per cent of queries have
    neighbours at distance ~0, themselves or twins carrying the same sequence.

    Its caller hands over a ``set`` of accession STRINGS, and CPython
    randomises string hashing per process unless PYTHONHASHSEED is pinned,
    which it is not here. So iterating that set gave a different bank order on
    EVERY WORKER RESTART that rebuilt this cache -- worse than the unified
    pool's plan-dependence, which at least only moves when the plan does.
    Verified by running the same eight accessions through three interpreters
    and getting three orders.

    Sorting the accessions rather than the resulting indices keeps this correct
    whatever order the unified pool happens to be in.
    """
    return np.array(
        [acc_to_idx[acc] for acc in sorted(accessions) if acc in acc_to_idx],
        dtype=np.int32,
    )


def _request_for(session: Session, ctx: Any) -> _PoolRequest:
    """The pool a batch wants: its configuration, its annotation set, its donors, its space."""
    p = ctx.p
    return _PoolRequest(
        ctx.embedding_config_id,
        ctx.annotation_set_id,
        getattr(p, "donor_policy", None),
        search_space(session, p),
    )


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
        need_cos, need_plain = reference_dtype_needs(p)
        request = _request_for(session, ctx)
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
        is silent by construction. The search space is part of it: a raw and a
        whitened pool of the same rows are two entries (``SearchSpace.identity``).
        """
        return (
            p.embedding_config_id,
            p.annotation_set_id,
            p.aspect_separated_knn,
            need_cos,
            need_plain,
            request.discriminator,
            None if request.space is None else request.space.identity,
        )

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
        accessions, embeddings, source, transfer = self._load_ref_pool(session, request, emit)
        done: dict[str, Any] = {
            "references": len(accessions),
            "embeddings_mb": round(bank_bytes(embeddings) / 1024 / 1024),
            "source": source,
        }
        if transfer is not None:
            done["transfer"] = transfer
        emit("predict_go_terms_batch.load_references_done", None, done, "info")
        transform = None if request.space is None else request.space.apply
        return _derive_reference_views(
            accessions, embeddings, need_cos=need_cos, need_plain=need_plain, transform=transform
        )

    def _load_ref_pool(
        self,
        session: Session,
        request: _PoolRequest,
        emit: EmitFn,
    ) -> tuple[list[str], np.ndarray, str, str | None]:
        """Fetch ``(accessions, embeddings, source, transfer)`` via disk cache or DB.

        When the cache files are fresh (mtime within
        ``ref_cache_freshness_seconds``), the COUNT(*)-over-JOIN is skipped
        so the fast path requires zero DB round-trips. ``source`` is
        ``"disk_cache"`` on hit, else where the vectors came from (``_stream_pool``).

        ``transfer`` names the route the bank took across the driver, or is
        ``None`` when a cache answered and nothing crossed. See
        :mod:`._reference_binary`.
        """
        from protea.config.tuning import get_tuning
        from protea.core.disk_cache import _is_cache_fresh

        # A sparse bank never reaches the disk cache; ``NO_DISK_CACHE_FOR_SPARSE``
        # says why and what it costs.
        if bank_is_sparse(session, request.embedding_config_id):
            accessions, codes, _ = self._stream_reference_pool(
                self._reference_pool_query(session, request), sparse=True
            )
            return accessions, codes, "database", "text"

        freshness_sec = get_tuning().operation.ref_cache_freshness_seconds
        source = "disk_cache"
        transfer: str | None = None
        # The policy decides both WHICH proteins may donate and WHERE the pool
        # is cached. Deriving the key from the same object that filters the
        # query is what stops a restricted pool being stored under, or read
        # from, the unrestricted name.
        key = request.cache_key
        accession_q = self._reference_pool_query(session, request)

        def _db_loader() -> tuple[list[str], np.ndarray]:
            nonlocal source, transfer
            accs, embs, source, transfer = self._stream_pool(session, request, accession_q)
            return accs, embs

        fresh = freshness_sec > 0 and _is_cache_fresh(
            request.embedding_config_id,
            request.annotation_set_id,
            freshness_sec,
            request.discriminator,
        )
        expected: int | None = None if fresh else accession_q.count()

        accessions, embeddings = _load_reference_pool_cached(
            key,
            _db_loader,
            expected_count=expected,
            emit=lambda ev, fields: emit(f"predict_go_terms_batch.{ev}", None, fields, "info"),
            freshness_seconds=freshness_sec,
            write=get_tuning().operation.reference_pool_disk_write,
        )
        return accessions, embeddings, source, transfer

    def _stream_pool(
        self, session: Session, request: _PoolRequest, accession_q: Any
    ) -> tuple[list[str], Any, str, str | None]:
        """A pool the disk cache lacks: ``(accessions, vectors, source, transfer)``.

        From the node's bank shards when they are the ones the store recorded
        (:mod:`._bank_shards`, source ``"bank_shards"``), else streamed from the
        store (source ``"database"``), as every run did before.
        """
        from_shards = fill_from_shards(session, accession_q, request.embedding_config_id)
        if from_shards is not None:
            return from_shards[0], from_shards[1], "bank_shards", "shards"
        accessions, vectors, transfer = self._stream_reference_pool(accession_q)
        return accessions, vectors, "database", transfer

    def _reference_pool_query(self, session: Session, request: _PoolRequest) -> Any:
        """Return the accession-only query for the reference pool.

        ``donor_policy`` restricts which proteins may donate annotations. The
        permissive default is every protein carrying an embedding and a
        positive annotation in the set: a protein whose rows of the set are all
        ``NOT`` rows donates nothing, and since the pool cache's epoch 5 it
        takes no neighbour slot either. A declared set of photo members
        (``donor_set_id``) keeps the donors it lists.

        The restriction is applied HERE, in the query, rather than by filtering
        the materialised pool afterwards. Filtering afterwards would mean
        streaming the whole pool out of the database in order to discard part of
        it, and the pool is the largest thing this operation touches.
        """
        policy = request.donor_policy
        # A donor is admitted through a row it may donate: positive, and passing
        # the policy's filters (``_pool_admission`` says why the NOT rows left).
        annotations = session.query(ProteinGOAnnotation.protein_accession).filter(
            ProteinGOAnnotation.annotation_set_id == request.annotation_set_id, positive_rows()
        )
        annotations = _restrict_annotations(
            annotations, policy, channels_of_the_cut(session, request.annotation_set_id, policy)
        )
        annotated_sq = annotations.distinct().subquery()

        query, accession = donor_pool_base(
            session, request.annotation_set_id, request.embedding_config_id, annotated_sq
        )
        query = admit_donor_set(query, accession, getattr(policy, "donor_set_id", None))
        if policy is not None and getattr(policy, "reviewed_only", False):
            # ``reviewed`` is nullable, and an unknown review status is not a
            # reviewed one. Comparing to true excludes the nulls, which is the
            # conservative reading and the one the caller asked for: a reviewed
            # spine that quietly included proteins of unknown provenance would
            # not be a reviewed spine.
            query = query.filter(Protein.reviewed.is_(True))
        admitted_by = getattr(policy, "admitted_by_release", None) if policy is not None else None
        if admitted_by is not None:
            # Membership of the corpus at the window's start. A release's
            # annotation set keeps the annotations of every protein the corpus
            # holds when it is loaded, so release 211's set carries 30,128
            # proteins admitted only later (measured 2026-11-04), 29,922 of them
            # donors in the full length ankh bank. On the indexed column, and a
            # NULL is not <= anything: a protein no pass admitted is no member.
            query = query.filter(Protein.first_admitted_release <= int(admitted_by))
        return query.order_by(*_bank_order(accession))

    def _stream_reference_pool(
        self,
        accession_q: Any,
        *,
        sparse: bool = False,
    ) -> tuple[list[str], Any]:
        """Stream ``(accession, embedding)`` rows into a bank.

        Dense f16 by default, which is what every PLM bank wants. ``sparse``
        delegates to :func:`stream_sparse_pool`, which argues why a k-WTA bank
        gets a CSR and why its shape comes from the rows received rather than
        from ``total``.
        """
        total = accession_q.count()
        if total == 0:
            empty = empty_sparse_bank() if sparse else np.empty((0,), dtype=np.float16)
            return [], empty, None
        base_q = accession_q.add_columns(SequenceEmbedding.embedding)
        # pgvector HalfVector exposes .dimensions() / .to_numpy() after the
        # 2026-04-11 halfvec migration; pull dimension from the first row.
        dim = base_q.limit(1).one()[1].dimensions()
        from protea.config.tuning import get_tuning

        stream_chunk = get_tuning().operation.stream_chunk_size
        if sparse:
            accs, codes = stream_sparse_pool(
                base_q.yield_per(stream_chunk), dim=dim, n_accessions_hint=total
            )
            return accs, codes, "text"
        embeddings = np.empty((total, dim), dtype=np.float16)
        accessions = fill_from_binary(base_q, embeddings, stream_chunk, total)
        if accessions is not None:
            return accessions, embeddings, "binary"
        accessions = []
        for i, (acc, emb) in enumerate(base_q.yield_per(stream_chunk)):
            embeddings[i] = emb.to_numpy()
            accessions.append(acc)
        if len(accessions) != total:
            # The same guard the binary route carries, and for the same reason:
            # the bank is allocated at ``count()`` and filled by index, so fewer
            # rows leave the tail as whatever ``np.empty`` found and those rows
            # become donors with uninitialised coordinates. It RAISES here
            # because there is no third route behind this one, and a short bank
            # is worse than no bank.
            raise RuntimeError(
                f"the reference pool streamed {len(accessions)} rows where the query "
                f"counted {total}; the bank would be part uninitialised memory"
            )
        return accessions, embeddings, "text"

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
        missing_aspects = _find_missing_aspects(request, pool_fingerprint(unified["accessions"]))
        built = self._query_and_persist_aspect_caches(
            session, missing_aspects, unified["accessions"], acc_to_idx, request
        )
        result, total_refs = self._assemble_all_aspect_views(
            unified, built, request, emit, need_cos=need_cos, need_plain=need_plain
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
        built: dict[str, BuiltAspect],
        request: _PoolRequest,
        emit: EmitFn,
        *,
        need_cos: bool = True,
        need_plain: bool = True,
    ) -> tuple[dict[str, dict[str, Any]], int]:
        """Slice the unified cache into per-aspect views, emit one ``done`` event
        per aspect, and return ``(views, total_refs)``.

        ``built`` holds the aspects this process just built from the database,
        served as built; every other aspect is read from the disk cache."""
        result: dict[str, dict[str, Any]] = {}
        total_refs = 0
        for aspect in _ASPECTS:
            view, n_refs = _assemble_aspect_view(
                aspect,
                unified,
                request,
                need_cos=need_cos,
                need_plain=need_plain,
                built=built.get(aspect),
            )
            result[aspect] = view
            total_refs += n_refs
            emit(
                "predict_go_terms_batch.load_references_per_aspect_done",
                None,
                {
                    "aspect": aspect,
                    "references": n_refs,
                    "source": "database" if aspect in built else "disk_cache",
                },
                "info",
            )
        return result, total_refs

    @staticmethod
    def _query_and_persist_aspect_caches(
        session: Session,
        missing_aspects: list[str],
        unified_accessions: list[str],
        acc_to_idx: dict[str, int],
        request: _PoolRequest,
    ) -> dict[str, BuiltAspect]:
        """Single-pass DB scan + per-aspect on-disk persistence for the missing aspects.

        Collects ``(aspect_to_accset, aspect_to_go_map)`` via
        :meth:`_collect_aspect_annotations`, then builds the index array +
        annotation CSR for every missing aspect and returns them, written to
        disk unless the tuning switch ``reference_pool_disk_write`` is off.
        Downstream batches read the CSR from memory; zero per-batch IN queries.
        Nothing missing, nothing scanned.
        """
        if not missing_aspects:
            return {}
        # Resolve the (possibly test-patched) ``_collect_aspect_annotations``
        # via the concrete subclass so ``patch.object(PredictGOTermsBatchOperation,
        # "_collect_aspect_annotations", ...)`` keeps wiring through. Lazy
        # import avoids the cycle since ``_batch_op`` already imports this
        # mixin at top level.
        from protea.core.operations.predict_go_terms._batch_op import (
            PredictGOTermsBatchOperation,
        )

        ecid, asid, d = (
            request.embedding_config_id,
            request.annotation_set_id,
            request.discriminator,
        )
        # The policy travels into the CSR because the CSR is what the hot path
        # reads. Filtering only the fallback query would leave the cache, and
        # therefore almost every real run, unfiltered. The disk key already
        # carries the policy's discriminator, so a filtered and an unfiltered
        # pool cannot be confused for one another.
        aspect_to_accset, aspect_to_go_map = (
            PredictGOTermsBatchOperation._collect_aspect_annotations(
                session, missing_aspects, asid, request.donor_policy
            )
        )
        from protea.config.tuning import get_tuning

        write = get_tuning().operation.reference_pool_disk_write
        pool = pool_fingerprint(unified_accessions)
        built: dict[str, BuiltAspect] = {}
        for asp in missing_aspects:
            indices = _aspect_indices(aspect_to_accset[asp], acc_to_idx)
            asp_accessions = [unified_accessions[i] for i in indices]
            anno_csr = _build_anno_csr(asp_accessions, aspect_to_go_map[asp])
            built[asp] = BuiltAspect(indices, anno_csr, pool)
            if write:
                idx_path = _aspect_index_path(ecid, asid, asp, d)
                idx_path.parent.mkdir(parents=True, exist_ok=True)
                np.save(idx_path, indices)
                _aspect_index_pool_path(ecid, asid, asp, d).write_text(pool, encoding="utf-8")
                _save_anno_csr_to_disk(ecid, asid, asp, anno_csr, d)
        return built

    @staticmethod
    def _collect_aspect_annotations(
        session: Session,
        missing_aspects: list[str],
        annotation_set_id: uuid.UUID,
        donor_policy: Any = None,
    ) -> tuple[
        dict[str, set[str]],
        dict[str, dict[str, list[dict[str, Any]]]],
    ]:
        """Single-pass query that fetches every ``ProteinGOAnnotation`` row for
        the missing aspects in one table scan, returning per-aspect accession
        sets + per-protein GO maps. Reads the positive rows the policy admits,
        the ones the pool admits its donors through (``_pool_admission``)."""
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
                positive_rows(),
            )
        )
        channels_at = channels_of_the_cut(session, annotation_set_id, donor_policy)
        rows = _restrict_annotations(rows, donor_policy, channels_at).yield_per(50_000)
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
        _pin_annotation_order(aspect_to_go_map)
        return aspect_to_accset, aspect_to_go_map
