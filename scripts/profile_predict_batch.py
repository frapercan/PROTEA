#!/usr/bin/env python
"""Profile one predict_go_terms_batch execution in-process.

Bypasses the queue: instantiates PredictGOTermsBatchOperation, builds a
synthetic payload with N delta-protein accessions, wraps the hot paths with
timing decorators, then calls execute() and prints a breakdown.

The profile runs against whichever reference cache is already warm on disk.
Defaults to ESM2-650M (the model currently being consumed by Phase A) — the
disk cache is guaranteed warm and the numbers reflect production config.

Note on CPU contention: if Phase A is still running, the batch worker will
compete for cores and inflate absolute numbers by ~2×, but the *proportions*
remain meaningful. Run with OMP_NUM_THREADS=2 to cap numpy interference.

Usage
-----
    OMP_NUM_THREADS=2 poetry run python scripts/profile_predict_batch.py \\
        --eval-set a73cb77c-9adf-4d55-b61f-c0b1bd05be01 \\
        --annotation-set 6892cb7c-514c-496f-9837-46ba9c1744c4 \\
        --snapshot 6d399baf-339b-4496-9aae-ed75d03229ea \\
        --emb-config c2e9dda3-e505-4170-b50d-435a451761ac \\
        [--n 128] [--k 5]
"""
from __future__ import annotations

import argparse
import cProfile
import pstats
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path

from protea.core.operations import predict_go_terms as pgt
from protea.core import feature_engineering as fe
from protea.core import knn_search as ks
from protea.core.evaluation import load_evaluation_data_for_set
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.session import build_session_factory
from protea.infrastructure.settings import load_settings


TOTALS: dict[str, list[float]] = defaultdict(lambda: [0.0, 0])


def wrap(name: str, fn):
    def wrapper(*args, **kwargs):
        t = time.perf_counter()
        try:
            return fn(*args, **kwargs)
        finally:
            elapsed = time.perf_counter() - t
            TOTALS[name][0] += elapsed
            TOTALS[name][1] += 1
    wrapper.__wrapped__ = fn
    return wrapper


def install_timers():
    """Monkey-patch hot paths so we can measure cumulative time & call counts."""
    # Module-level imports used by predict_go_terms_batch
    pgt.search_knn = wrap("search_knn", ks.search_knn)
    pgt.compute_alignment = wrap("compute_alignment", fe.compute_alignment)
    pgt.compute_taxonomy = wrap("compute_taxonomy", fe.compute_taxonomy)

    # Bound methods on the operation class
    cls = pgt.PredictGOTermsBatchOperation
    cls._load_annotations_for = wrap("_load_annotations_for", cls._load_annotations_for)
    cls._load_sequences_for_proteins = wrap(
        "_load_sequences_ref", cls._load_sequences_for_proteins
    )
    cls._load_sequences_for_queries = wrap(
        "_load_sequences_query", cls._load_sequences_for_queries
    )
    cls._load_taxonomy_ids_for_proteins = wrap(
        "_load_taxonomy_ref", cls._load_taxonomy_ids_for_proteins
    )
    cls._load_taxonomy_ids_for_queries = wrap(
        "_load_taxonomy_query", cls._load_taxonomy_ids_for_queries
    )
    cls._load_query_embeddings = wrap("_load_query_embeddings", cls._load_query_embeddings)
    cls._load_reference_data = wrap("_load_reference_data", cls._load_reference_data)
    cls._load_reference_data_per_aspect = wrap(
        "_load_reference_per_aspect", cls._load_reference_data_per_aspect
    )
    cls._run_aspect_separated_knn = wrap(
        "_run_aspect_separated_knn_TOTAL", cls._run_aspect_separated_knn
    )


def _delta_accessions(session, eval_set_id: uuid.UUID, n: int) -> list[str]:
    e = session.get(EvaluationSet, eval_set_id)
    data, _ = load_evaluation_data_for_set(session, e)
    accs = sorted(set(data.nk.keys()) | set(data.lk.keys()) | set(data.pk.keys()))
    return accs[:n]


def _silent_emit(*args, **kwargs):
    pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--eval-set", required=True)
    ap.add_argument("--annotation-set", required=True)
    ap.add_argument("--snapshot", required=True)
    ap.add_argument("--emb-config", required=True)
    ap.add_argument("--n", type=int, default=128, help="Queries per batch")
    ap.add_argument("--k", type=int, default=5)
    ap.add_argument("--aspect-separated", action="store_true", default=True)
    ap.add_argument("--alignments", action="store_true", default=True)
    ap.add_argument("--taxonomy", action="store_true", default=True)
    ap.add_argument("--reranker", action="store_true", default=True)
    ap.add_argument("--cprofile-top", type=int, default=30,
                    help="How many cProfile lines to print")
    args = ap.parse_args()

    install_timers()

    settings = load_settings(Path("."))
    factory = build_session_factory(settings.db_url)

    with factory() as session:
        print(f"[setup] Loading delta accessions (N={args.n}) …", flush=True)
        accs = _delta_accessions(session, uuid.UUID(args.eval_set), args.n)
        print(f"[setup] {len(accs)} accessions", flush=True)

        payload = {
            "embedding_config_id": args.emb_config,
            "annotation_set_id": args.annotation_set,
            "prediction_set_id": str(uuid.uuid4()),
            "parent_job_id": str(uuid.uuid4()),
            "query_accessions": accs,
            "limit_per_entry": args.k,
            "compute_alignments": args.alignments,
            "compute_taxonomy": args.taxonomy,
            "compute_reranker_features": args.reranker,
            "aspect_separated_knn": args.aspect_separated,
            "search_backend": "numpy",
        }

        op = pgt.PredictGOTermsBatchOperation()

        print(f"[run] execute() N={args.n} k={args.k} "
              f"aspect_separated={args.aspect_separated} "
              f"alignments={args.alignments} taxonomy={args.taxonomy} "
              f"reranker={args.reranker}", flush=True)

        # Warm: first call loads ref cache (~disk I/O, not representative of
        # per-batch cost). We still measure it as "cold" and then run a second
        # call for the "warm" number.
        t_cold = time.perf_counter()
        op.execute(session, payload, emit=_silent_emit)
        cold_elapsed = time.perf_counter() - t_cold
        print(f"\n[cold] first call (includes ref cache load): {cold_elapsed:.2f} s")

        # Reset timers for the warm run
        TOTALS.clear()

        # Warm run with cProfile for fine-grained detail
        prof = cProfile.Profile()
        t_warm = time.perf_counter()
        prof.enable()
        op.execute(session, payload, emit=_silent_emit)
        prof.disable()
        warm_elapsed = time.perf_counter() - t_warm
        print(f"[warm] second call: {warm_elapsed:.2f} s\n")

        # High-level breakdown from our wrappers
        print("="*70)
        print(f"{'function':<35s}{'total (s)':>12s}{'calls':>10s}{'avg (ms)':>12s}")
        print("="*70)
        for name, (tot, calls) in sorted(TOTALS.items(), key=lambda kv: -kv[1][0]):
            avg_ms = (tot / calls * 1000) if calls else 0.0
            print(f"{name:<35s}{tot:>12.3f}{calls:>10d}{avg_ms:>12.3f}")
        print("="*70)

        # Show what fraction is accounted for by top-level buckets
        top_level = (
            TOTALS.get("_run_aspect_separated_knn_TOTAL", [0, 0])[0]
            + TOTALS.get("_load_query_embeddings", [0, 0])[0]
            + TOTALS.get("_load_reference_data", [0, 0])[0]
            + TOTALS.get("_load_reference_per_aspect", [0, 0])[0]
        )
        print(f"\naccounted (top-level): {top_level:.2f} s  "
              f"(warm total = {warm_elapsed:.2f} s, "
              f"unaccounted = {warm_elapsed - top_level:+.2f} s)")

        print("\n" + "="*70)
        print("cProfile top-%d by cumulative time" % args.cprofile_top)
        print("="*70)
        ps = pstats.Stats(prof).sort_stats("cumulative")
        ps.print_stats(args.cprofile_top)

        print("="*70)
        print("cProfile top-%d by internal time (tottime)" % args.cprofile_top)
        print("="*70)
        ps = pstats.Stats(prof).sort_stats("tottime")
        ps.print_stats(args.cprofile_top)

    return 0


if __name__ == "__main__":
    sys.exit(main())
