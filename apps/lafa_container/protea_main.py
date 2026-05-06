"""LAFA-compatible PROTEA wrapper.

Entry point that honours the LAFA container CLI contract:

    --query_file        FASTA of query sequences
    --train_sequences   FASTA of training sequences
    --annot_file        TSV (EntryID, term, aspect) of training annotations
    --graph             go-basic.obo (currently unused; kept for contract parity)
    --output_baseline   3-column TSV output (Query_ID, GO_Term, Score)

Pipeline:
    1. Mean-pool ProtT5 embeddings for queries and refs (``prott5_encoder``).
    2. Cosine KNN via ``protea.core.knn_search.search_knn`` (numpy backend).
    3. First-hit GO transfer per query (matches PROTEA's ``_predict_batch``).
    4. Score = ``1 - distance`` (cosine, in [0, 1]).
    5. Emit ``<query>\\t<term>\\t<score:.4f>``; gzipped if ``--output_baseline``
       ends in ``.gz``.

Smoke-test focus: integration over fidelity. The ontology graph is accepted
but not consulted — LAFA distributes propagated TSVs in the official splits.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Iterator

import numpy as np

# Make `protea.core.knn_search` importable when running from a checkout.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from protea.core.knn_search import search_knn  # noqa: E402

from prott5_encoder import embed_sequences, fasta_accessions, parse_fasta  # noqa: E402


def _open_text(path: str):
    return gzip.open(path, "rt") if path.endswith(".gz") else open(path)


def _load_annotations(path: str, ref_accessions: set[str]) -> dict[str, list[str]]:
    """Return ``{ref_accession: [go_term, ...]}`` filtered to refs we use.

    Dispatches by extension: ``.gaf[.gz]`` → GAF parser (skipping ``NOT``
    qualifiers and ``!`` headers); anything else → TSV with ``EntryID`` /
    ``term`` columns in the header.
    """
    base = path[:-3] if path.endswith(".gz") else path
    if base.endswith(".gaf"):
        return _load_annotations_gaf(path, ref_accessions)
    return _load_annotations_tsv(path, ref_accessions)


def _load_annotations_tsv(path: str, ref_accessions: set[str]) -> dict[str, list[str]]:
    go_map: dict[str, list[str]] = defaultdict(list)
    with _open_text(path) as handle:
        header = handle.readline().rstrip("\n").split("\t")
        try:
            entry_idx = header.index("EntryID")
            term_idx = header.index("term")
        except ValueError:
            print(
                f"[protea_main] Annotation TSV must have header with 'EntryID' and 'term'. "
                f"Got: {header}",
                file=sys.stderr,
            )
            sys.exit(1)
        for line in handle:
            cols = line.rstrip("\n").split("\t")
            if len(cols) <= max(entry_idx, term_idx):
                continue
            acc = cols[entry_idx]
            term = cols[term_idx]
            if acc in ref_accessions:
                go_map[acc].append(term)
    return go_map


def _load_annotations_gaf(path: str, ref_accessions: set[str]) -> dict[str, list[str]]:
    """Parse a GAF 2.x file. Cols: 2=DB_Object_ID, 5=GO_ID, 4=Qualifier."""
    go_map: dict[str, list[str]] = defaultdict(list)
    with _open_text(path) as handle:
        for raw in handle:
            if raw.startswith("!"):
                continue
            cols = raw.rstrip("\n").split("\t")
            if len(cols) < 9:
                continue
            if "NOT" in cols[3]:
                continue
            acc = cols[1]
            term = cols[4]
            if acc in ref_accessions:
                go_map[acc].append(term)
    return go_map


def _open_output(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "wt", newline="")
    return open(path, "w", newline="")


def _stack(embeddings: dict[str, np.ndarray], order: list[str]) -> tuple[np.ndarray, list[str]]:
    """Stack embeddings in ``order``, dropping accessions that failed to embed."""
    kept_accs: list[str] = []
    rows: list[np.ndarray] = []
    for acc in order:
        vec = embeddings.get(acc)
        if vec is None:
            continue
        kept_accs.append(acc)
        rows.append(vec)
    if not rows:
        return np.empty((0, 0), dtype=np.float32), kept_accs
    return np.stack(rows).astype(np.float32, copy=False), kept_accs


def _transfer(
    query_accs: list[str],
    neighbors: list[list[tuple[str, float]]],
    go_map: dict[str, list[str]],
    *,
    keep_self_hits: bool,
) -> Iterator[tuple[str, str, float]]:
    """First-hit GO transfer; one ``(query, term, score)`` row per (q, term)."""
    for q_acc, top_refs in zip(query_accs, neighbors, strict=False):
        seen: set[str] = set()
        for ref_acc, distance in top_refs:
            if not keep_self_hits and ref_acc == q_acc:
                continue
            score = max(0.0, 1.0 - float(distance))
            for term in go_map.get(ref_acc, ()):
                if term in seen:
                    continue
                seen.add(term)
                yield q_acc, term, score


def main() -> None:
    parser = argparse.ArgumentParser(
        description="LAFA-compatible PROTEA KNN wrapper (ProtT5 + cosine KNN + first-hit transfer)."
    )
    parser.add_argument("--query_file", "-q", required=True)
    parser.add_argument("--train_sequences", required=True)
    parser.add_argument("--annot_file", "-a", required=True)
    parser.add_argument("--graph", required=True, help="OBO file (currently not consulted).")
    parser.add_argument("--output_baseline", "-o", required=True)
    parser.add_argument("--k", type=int, default=5, help="KNN neighbours per query (default: 5).")
    parser.add_argument("--metric", default="cosine", choices=["cosine", "l2"])
    parser.add_argument("--backend", default="numpy", choices=["numpy", "faiss"])
    parser.add_argument(
        "--keep_self_hits",
        action="store_true",
        help="Keep query==ref hits (default: drop, matching LAFA's prott5_container).",
    )
    parser.add_argument(
        "--model_dir",
        default=os.environ.get("HF_CACHE"),
        help="HuggingFace cache dir (default: $HF_CACHE).",
    )
    args = parser.parse_args()

    for label, path in (
        ("query", args.query_file),
        ("train", args.train_sequences),
        ("annot", args.annot_file),
        ("graph", args.graph),
    ):
        if not os.path.exists(path):
            print(f"[protea_main] {label} file not found: {path}", file=sys.stderr)
            sys.exit(1)

    print(f"[protea_main] reading FASTAs: {args.query_file} / {args.train_sequences}")
    query_seqs = parse_fasta(args.query_file)
    train_seqs = parse_fasta(args.train_sequences)
    print(f"[protea_main] queries={len(query_seqs)} refs={len(train_seqs)}")

    print(f"[protea_main] loading annotations from {args.annot_file}")
    go_map = _load_annotations(args.annot_file, set(train_seqs))
    refs_with_anns = [acc for acc in train_seqs if acc in go_map]
    print(f"[protea_main] refs with annotations: {len(refs_with_anns)}/{len(train_seqs)}")
    if not refs_with_anns:
        print("[protea_main] no annotated refs after filter — nothing to transfer.", file=sys.stderr)
        sys.exit(2)

    to_embed = {**{a: query_seqs[a] for a in query_seqs},
                **{a: train_seqs[a] for a in refs_with_anns}}
    print(f"[protea_main] embedding {len(to_embed)} sequences with ProtT5 mean-pool")
    embeddings = embed_sequences(to_embed, cache_dir=args.model_dir)

    query_order = fasta_accessions(args.query_file)
    Q, kept_q = _stack(embeddings, query_order)
    R, kept_r = _stack(embeddings, refs_with_anns)
    print(f"[protea_main] embedding matrix Q={Q.shape} R={R.shape}")
    if Q.size == 0 or R.size == 0:
        print("[protea_main] empty embedding matrix — aborting.", file=sys.stderr)
        sys.exit(3)

    print(f"[protea_main] KNN k={args.k} metric={args.metric} backend={args.backend}")
    neighbors = search_knn(
        Q,
        R,
        kept_r,
        k=args.k,
        metric=args.metric,
        backend=args.backend,
    )

    out_path = args.output_baseline
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    n_rows = 0
    with _open_output(out_path) as fh:
        writer = csv.writer(fh, delimiter="\t")
        for q_acc, term, score in _transfer(
            kept_q, neighbors, go_map, keep_self_hits=args.keep_self_hits
        ):
            writer.writerow([q_acc, term, f"{score:.4f}"])
            n_rows += 1

    print(f"[protea_main] wrote {n_rows} predictions to {out_path}")


if __name__ == "__main__":
    main()
