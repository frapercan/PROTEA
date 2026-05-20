"""Parallel + persistent computation of per-pair alignment features.

The export pipeline's hotspot is the per-(query, reference) alignment in
``_KnnTransferRunner._compute_pair_features``: a single-threaded triple
loop calling ``compute_alignment`` (parasail NW+SW with traceback) at a
few hundred pairs/s. Two structural wins live here:

1. **Process-level parallelism** over the unique alignment pairs. parasail's
   traceback variants do not release the GIL well enough for threads to
   scale, so a :class:`ProcessPoolExecutor` is used (benchmarked ~3x on a
   12-core box). The taxonomy lookups stay in the parent process because
   they are cheap (an ``lru_cache`` over ete3 lineages) and process
   re-init of the ete3 sqlite handle would dwarf the work.

2. **A persistent on-disk cache** keyed by the ordered sequence pair plus
   the alignment parameters. Intra-PLM the K=10 neighbour set is a
   superset of K=5 / K=3, so alignments computed once for the largest-K
   dataset make the smaller-K datasets (and any re-run) near-free.
   ``protea.training`` is serialised (one job at a time) so there is no
   concurrent-write contention; a plain sqlite file is enough.

This module is value-preserving by construction: the alignment feature
dict it returns is exactly what ``compute_alignment`` returns, only
computed concurrently and memoised. Disabling parallelism (workers <= 1)
and the cache (``PROTEA_ALIGN_CACHE_DIR`` unset / cache-miss) reduces to
the original serial code path.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import time
from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

from protea.core.feature_engineering import compute_alignment, compute_taxonomy

_LOG = logging.getLogger(__name__)

# (query_tax_ids, ref_tax_ids) — both optional, may be ``None`` when the
# taxonomy feature family is disabled.
TaxIdMaps = tuple[dict[str, Any] | None, dict[str, Any] | None]

# Alignment parameters are fixed in ``compute_alignment`` (BLOSUM62, gap
# open 10, gap extend 1). They are folded into the cache key so a future
# parameter change cannot return stale rows.
_GAP_OPEN = 10
_GAP_EXTEND = 1
_MATRIX = "blosum62"


def _align_worker(pair: tuple[str, str]) -> dict[str, Any]:
    """Picklable process-pool worker: alignment features for one pair.

    Imported lazily-by-value (the strings are the only payload), so no
    runner / ORM state crosses the process boundary.
    """
    q_seq, r_seq = pair
    return compute_alignment(q_seq, r_seq)


def _seq_digest(seq: str) -> str:
    return hashlib.md5(seq.encode(), usedforsecurity=False).hexdigest()


def _cache_key(q_seq: str, r_seq: str) -> str:
    """Ordered-pair cache key.

    parasail alignment is not guaranteed symmetric for identity/gaps, so
    the key is on the ordered pair ``(q, r)`` to stay byte-safe.
    """
    return f"{_seq_digest(q_seq)}:{_seq_digest(r_seq)}:{_GAP_OPEN}:{_GAP_EXTEND}:{_MATRIX}"


def resolve_workers() -> int:
    """Number of process-pool workers for pair-feature computation.

    ``PROTEA_PAIR_FEATURE_WORKERS`` overrides; default is the CPU count.
    A value <= 1 forces the serial path (no pool spin-up).
    """
    raw = os.environ.get("PROTEA_PAIR_FEATURE_WORKERS")
    if raw is not None:
        try:
            return max(1, int(raw))
        except ValueError:
            _LOG.warning("invalid PROTEA_PAIR_FEATURE_WORKERS=%r; using cpu count", raw)
    return os.cpu_count() or 1


def _resolve_cache_dir() -> Path | None:
    """On-disk alignment cache directory, or ``None`` to disable.

    ``PROTEA_ALIGN_CACHE_DIR`` overrides. The default lives under the
    process CWD (the worker runs from the project root) at
    ``artifacts/align_cache``. Set ``PROTEA_ALIGN_CACHE_DIR=`` (empty) to
    disable persistence entirely.
    """
    raw = os.environ.get("PROTEA_ALIGN_CACHE_DIR")
    if raw is not None:
        return Path(raw) if raw.strip() else None
    return Path.cwd() / "artifacts" / "align_cache"


class _AlignCache:
    """Thin sqlite-backed key→json-dict store for alignment features."""

    def __init__(self, cache_dir: Path) -> None:
        cache_dir.mkdir(parents=True, exist_ok=True)
        self._path = cache_dir / "alignments.sqlite"
        self._conn = sqlite3.connect(str(self._path))
        # WAL keeps the single-writer cheap and durable across jobs.
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("CREATE TABLE IF NOT EXISTS align (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
        self._conn.commit()

    def get_many(self, keys: list[str]) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        # Chunk the IN-clause to stay under sqlite's variable limit.
        for i in range(0, len(keys), 800):
            chunk = keys[i : i + 800]
            qs = ",".join("?" * len(chunk))
            rows = self._conn.execute(
                f"SELECT k, v FROM align WHERE k IN ({qs})",
                chunk,  # noqa: S608
            ).fetchall()
            for k, v in rows:
                out[k] = json.loads(v)
        return out

    def put_many(self, items: list[tuple[str, dict[str, Any]]]) -> None:
        if not items:
            return
        self._conn.executemany(
            "INSERT OR IGNORE INTO align (k, v) VALUES (?, ?)",
            [(k, json.dumps(v)) for k, v in items],
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


class PairAlignmentComputer:
    """Compute alignment-feature dicts for many pairs, cached + parallel.

    Use as a context manager so the sqlite handle and process pool are
    released deterministically.
    """

    def __init__(self) -> None:
        self._workers = resolve_workers()
        cache_dir = _resolve_cache_dir()
        self._cache = _AlignCache(cache_dir) if cache_dir is not None else None
        self.n_cache_hits = 0
        self.n_computed = 0

    def __enter__(self) -> PairAlignmentComputer:
        return self

    def __exit__(self, *_exc: object) -> None:
        if self._cache is not None:
            self._cache.close()

    def compute(self, pairs: list[tuple[str, str]]) -> dict[tuple[str, str], dict[str, Any]]:
        """Return ``{(q_seq, r_seq): alignment_dict}`` for every input pair.

        Empty-sequence pairs are skipped by the caller; here every pair
        is assumed to have two non-empty sequences. Deduplicates on the
        ordered sequence pair so identical pairs are aligned once.
        """
        unique: dict[str, tuple[str, str]] = {}
        for pair in pairs:
            unique.setdefault(_cache_key(*pair), pair)

        by_key: dict[str, dict[str, Any]] = {}
        if self._cache is not None:
            by_key = self._cache.get_many(list(unique))
            self.n_cache_hits += len(by_key)

        missing = [(k, p) for k, p in unique.items() if k not in by_key]
        self.n_computed += len(missing)
        if missing:
            computed = self._align_missing([p for _, p in missing])
            new_items: list[tuple[str, dict[str, Any]]] = []
            for (k, _p), feats in zip(missing, computed, strict=True):
                by_key[k] = feats
                new_items.append((k, feats))
            if self._cache is not None:
                self._cache.put_many(new_items)

        return {pair: by_key[_cache_key(*pair)] for pair in pairs}

    def _align_missing(self, pairs: list[tuple[str, str]]) -> list[dict[str, Any]]:
        """Align the cache-miss pairs, serial or via a process pool."""
        if self._workers <= 1 or len(pairs) < 2:
            return [_align_worker(p) for p in pairs]
        chunk = max(1, len(pairs) // (self._workers * 4))
        with ProcessPoolExecutor(max_workers=self._workers) as ex:
            return list(ex.map(_align_worker, pairs, chunksize=chunk))


def build_pair_feature_dict(
    q_acc: str,
    ref_acc: str,
    align_by_pair: dict[tuple[str, str], dict[str, Any]],
    *,
    do_alignments: bool,
    do_taxonomy: bool,
    tax_ids: TaxIdMaps,
) -> dict[str, Any]:
    """Merge the precomputed alignment + per-pair taxonomy for one pair.

    Value-identical to the original inline ``compute_alignment`` +
    ``compute_taxonomy`` merge: the alignment half is looked up from the
    batch-computed map, the taxonomy half is computed here (cheap,
    ``lru_cache``-backed). Either family is skipped when its flag is off.
    """
    feats: dict[str, Any] = {}
    if do_alignments:
        aln = align_by_pair.get((q_acc, ref_acc))
        if aln is not None:
            feats.update(aln)
    if do_taxonomy:
        query_tax_ids, ref_tax_ids = tax_ids
        assert query_tax_ids is not None and ref_tax_ids is not None
        q_tid = query_tax_ids.get(q_acc)
        r_tid = ref_tax_ids.get(ref_acc)
        feats.update(compute_taxonomy(q_tid, r_tid))
        feats["query_taxonomy_id"] = q_tid
        feats["ref_taxonomy_id"] = r_tid
    return feats


def _collect_unique_pairs(
    *,
    aspects: Sequence[str],
    neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]],
    valid_queries: Sequence[str],
    query_sequences: dict[str, str],
    ref_sequences: dict[str, str],
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Gather the unique (q_acc, ref_acc) pairs with non-empty sequences.

    Returns parallel lists ``(acc_pairs, seq_pairs)``: the accession keys
    and the corresponding ``(q_seq, r_seq)`` tuples to align.
    """
    seen: set[tuple[str, str]] = set()
    acc_pairs: list[tuple[str, str]] = []
    seq_pairs: list[tuple[str, str]] = []
    for aspect in aspects:
        nbs = neighbors_by_aspect[aspect]
        for q_idx, q_acc in enumerate(valid_queries):
            if q_idx >= len(nbs):
                continue
            q_seq = query_sequences.get(q_acc, "")
            if not q_seq:
                continue
            for ref_acc, _ in nbs[q_idx]:
                key = (q_acc, ref_acc)
                if key in seen:
                    continue
                seen.add(key)
                r_seq = ref_sequences.get(ref_acc, "")
                if not r_seq:
                    continue
                acc_pairs.append(key)
                seq_pairs.append((q_seq, r_seq))
    return acc_pairs, seq_pairs


def precompute_alignment_features(
    *,
    aspects: Sequence[str],
    neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]],
    valid_queries: Sequence[str],
    query_sequences: dict[str, str],
    ref_sequences: dict[str, str],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Batch-align every unique (q_acc, ref_acc) neighbour pair.

    Drives :class:`PairAlignmentComputer` (parallel + on-disk cache) over
    the deduplicated pair set and remaps the result back onto the
    ``(q_acc, ref_acc)`` accession keys the runner indexes by. Returns an
    empty map when there are no alignable pairs.
    """
    acc_pairs, seq_pairs = _collect_unique_pairs(
        aspects=aspects,
        neighbors_by_aspect=neighbors_by_aspect,
        valid_queries=valid_queries,
        query_sequences=query_sequences,
        ref_sequences=ref_sequences,
    )
    if not seq_pairs:
        return {}
    t0 = time.perf_counter()
    with PairAlignmentComputer() as computer:
        seq_result = computer.compute(seq_pairs)
        hits, computed = computer.n_cache_hits, computer.n_computed
    elapsed = max(1e-9, time.perf_counter() - t0)
    _LOG.info(
        "alignment precompute: %d pairs (%d cache-hit, %d computed) in %.1fs (%.0f pairs/s)",
        len(seq_pairs),
        hits,
        computed,
        elapsed,
        len(seq_pairs) / elapsed,
    )
    return {acc: seq_result[seq] for acc, seq in zip(acc_pairs, seq_pairs, strict=True)}
