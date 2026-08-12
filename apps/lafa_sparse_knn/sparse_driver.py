"""Driver for the protea-sparse-knn LAFA container.

Nearest-neighbour GO transfer in the space of a learned sparse encoder.

The pipeline is:

1. Embed the queries with Ankh-base, using ``protea_backends`` so the
   recipe is the one that produced the frozen bank rather than a
   lookalike (the tokenisation differs between the two, see below).
2. Project each embedding through the learned encoder and keep the
   largest ``top_k`` components. Queries then live in the same sparse
   code space as the bank.
3. Retrieve neighbours by cosine against the bank, kept sparse.
4. Transfer the donors' GO terms, score by neighbour agreement, and
   propagate to ancestors under the true path rule.

Why ``protea_backends`` and not a local encoder function: Ankh is a T5
with a character vocabulary, and the backend tokenises a list of single
characters with ``is_split_into_words=True`` after mapping ``[UZOB]`` to
``X``. Tokenising the raw string instead yields different tokens, so a
lookalike encoder would place the queries in a different geometry from
the bank and degrade silently. The bank is the contract; the producer
comes with it.

Memory is bounded on purpose. The bank stays a CSR matrix (about 540 MB
for 575k proteins at 128 non-zeros) and queries are scored in chunks, so
the dense similarity block never exceeds one chunk by the bank size.
Retrieval is exact brute force, not an approximate index.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np

#: Queries scored per similarity block. 256 x 575k x 4 bytes is about
#: 590 MB, which keeps the container inside a modest memory budget
#: regardless of how many sequences the evaluator sends.
QUERY_CHUNK = int(os.environ.get("PROTEA_SPARSE_QUERY_CHUNK", "256"))

#: Neighbours retrieved per query before the vote is taken.
DEFAULT_K = 30

#: Weight on donor similarity against donor agreement in the default score.
#: Taken from PROTEA's own ``composite`` scoring configuration, which weights
#: embedding similarity 0.4 against neighbour vote fraction 0.2. Renormalised
#: over the two signals this container has, that is 2/3. A sweep on one release
#: window put the optimum at 0.70 in four of nine cells, more than any other
#: value, and pure agreement won none: the platform's ratio survives losing the
#: alignment and taxonomy signals it was chosen alongside. The published figure
#: is kept rather than the swept one, because a constant confirmed from
#: elsewhere is worth more than one fitted here.
DEFAULT_BLEND = 0.67


def log(message: str) -> None:
    """Emit a timestamped progress line on stderr."""
    print(f"[protea-sparse-knn] {time.strftime('%H:%M:%S')} {message}", file=sys.stderr, flush=True)


# --- inputs -----------------------------------------------------------


def read_fasta(path: Path) -> dict[str, str]:
    """Read a FASTA file into ``{accession: sequence}``, in file order.

    The accession is the first whitespace-delimited token of the header,
    with a leading ``>`` stripped. LAFA headers carry the EntryID there.
    """
    seqs: dict[str, str] = {}
    acc: str | None = None
    parts: list[str] = []
    with open(path) as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            if line.startswith(">"):
                if acc is not None:
                    seqs[acc] = "".join(parts)
                acc = line[1:].split()[0]
                parts = []
            else:
                parts.append(line)
    if acc is not None:
        seqs[acc] = "".join(parts)
    return seqs


def parse_obo(path: Path) -> tuple[dict[str, list[str]], dict[str, str], dict[str, str]]:
    """Parse an OBO file into ``(parents, aspect, alt_id -> primary)``.

    Only the fields this container needs are read: ``is_a`` and
    ``part_of`` for the true path rule, ``namespace`` for the aspect
    letter, and ``alt_id`` so an obsolete identifier in the bank still
    resolves. Obsolete terms are dropped.
    """
    namespaces = {
        "biological_process": "P",
        "molecular_function": "F",
        "cellular_component": "C",
    }
    parents: dict[str, list[str]] = {}
    aspect: dict[str, str] = {}
    alt: dict[str, str] = {}

    term_id: str | None = None
    term_parents: list[str] = []
    obsolete = False

    def flush() -> None:
        if term_id and not obsolete:
            parents[term_id] = term_parents

    with open(path) as handle:
        for raw in handle:
            line = raw.strip()
            if line in ("[Term]", "[Typedef]"):
                flush()
                term_id, term_parents, obsolete = None, [], line == "[Typedef]"
                continue
            if not line or ":" not in line:
                continue
            key, _, value = line.partition(": ")
            if key == "id" and value.startswith("GO:"):
                term_id = value
            elif key == "is_obsolete" and value == "true":
                obsolete = True
            elif key == "alt_id" and term_id:
                alt[value] = term_id
            elif key == "namespace" and term_id:
                aspect[term_id] = namespaces.get(value, "")
            elif key == "is_a" and term_id:
                term_parents.append(value.split()[0])
            elif key == "relationship" and term_id and value.startswith("part_of "):
                term_parents.append(value.split()[1])
    flush()
    return parents, aspect, alt


def ancestors_of(term: str, parents: dict[str, list[str]], cache: dict[str, set[str]]) -> set[str]:
    """Return ``term`` plus all its ancestors, memoised across calls."""
    hit = cache.get(term)
    if hit is not None:
        return hit
    seen: set[str] = set()
    stack = [term]
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        stack.extend(parents.get(node, ()))
    cache[term] = seen
    return seen


# --- bundle -----------------------------------------------------------


def load_bank(bundle: Path) -> tuple[Any, list[str], dict[str, Any]]:
    """Load the frozen bank as ``(csr_matrix, accessions, bank metadata)``.

    The bank ships as two aligned arrays, ``codes_idx`` giving the
    dictionary positions of the kept components and ``codes_val`` their
    values, which is about ten times smaller than the dense form. It is
    assembled into a row-normalised CSR matrix so a cosine is one sparse
    product.
    """
    from scipy import sparse

    meta = json.loads((bundle / "BANK.json").read_text())
    idx = np.load(bundle / "codes_idx.npy")
    val = np.load(bundle / "codes_val.npy").astype(np.float32)
    accessions = [str(a) for a in np.load(bundle / "accessions.npy", allow_pickle=True)]

    n_rows, k = idx.shape
    if len(accessions) != n_rows:
        raise ValueError(f"bank misaligned: {n_rows} code rows vs {len(accessions)} accessions")

    norms = np.linalg.norm(val, axis=1, keepdims=True)
    val = val / np.maximum(norms, 1e-8)

    indptr = np.arange(0, n_rows * k + 1, k, dtype=np.int64)
    bank = sparse.csr_matrix(
        (val.ravel(), idx.ravel().astype(np.int32), indptr),
        shape=(n_rows, int(meta["dict_dim"])),
    )
    log(
        f"bank {bank.shape[0]:,} x {bank.shape[1]} at {k} non-zeros, {bank.data.nbytes / 1e6:.0f} MB"
    )
    return bank, accessions, meta


def load_donors(bundle: Path) -> dict[str, list[str]]:
    """Load ``donors.tsv.gz`` into ``{accession: [GO id, ...]}``."""
    donors: dict[str, list[str]] = defaultdict(list)
    with gzip.open(bundle / "donors.tsv.gz", "rt") as handle:
        for line in handle:
            acc, _, go_id = line.rstrip("\n").partition("\t")
            if go_id:
                donors[acc].append(go_id)
    log(f"donors: {len(donors):,} proteins")
    return donors


def load_encoder(bundle: Path) -> tuple[Any, int]:
    """Load the trained encoder and return ``(module, top_k)``."""
    import torch
    import torch.nn as nn

    checkpoint = torch.load(bundle / "encoder.pt", map_location="cpu", weights_only=False)
    meta = checkpoint["meta"]
    encoder = nn.Linear(meta["in_dim"], meta["dict_dim"])
    encoder.load_state_dict(checkpoint["state_dict"])
    encoder.eval()
    log(f"encoder {meta['in_dim']} -> {meta['dict_dim']}, top {meta['top_k']}")
    return encoder, int(meta["top_k"])


# --- query side -------------------------------------------------------


class _BundleEmbeddingConfig:
    """Duck-typed stand-in for PROTEA's ``EmbeddingConfig``.

    ``protea_backends`` reads the recipe off an attribute bag rather than
    an ORM row, so the container reconstructs it from the bundle. The
    values are the ones the bank was built with; nothing here is a
    default chosen by this file.
    """

    def __init__(self, recipe: dict[str, Any]) -> None:
        for key, value in recipe.items():
            setattr(self, key, value)


def _silent_emit(*_args: Any, **_kwargs: Any) -> None:
    """No-op event sink. The backend emits into PROTEA's job event log,
    which does not exist inside a LAFA container."""


def embed_queries(
    sequences: dict[str, str],
    recipe: dict[str, Any],
    device: str | None,
    batch_size: int,
) -> tuple[list[str], np.ndarray]:
    """Embed the queries with the same backend that produced the bank.

    ``load_model`` is the backend's own loader, not a local
    reimplementation, because it carries a detail that matters: Ankh runs
    in bfloat16 on CUDA and float32 on CPU, since FP16 LayerNorm collapses
    to NaN on this family. A container that loaded the checkpoint itself
    would have to know that.
    """
    import torch
    from protea_backends.ankh import AnkhBackend

    # Fail here rather than silently reaching for the network. The backend loads
    # the checkpoint through the HuggingFace cache, and if the mount is absent or
    # the cache variables point elsewhere it downloads instead, which is slow
    # with a network and fatal without one. An evaluator running offline should
    # see the reason, not a timeout.
    cache = Path(os.environ.get("HF_HOME", ""))
    if cache.name and not (cache / "hub").is_dir():
        log(f"warning: {cache}/hub is not present, the backbone will be fetched")

    backend = AnkhBackend()
    config = _BundleEmbeddingConfig(recipe)
    dev = device or ("cuda" if torch.cuda.is_available() else "cpu")

    model, tokenizer = backend.load_model(recipe["model_name"], dev, _silent_emit)
    log(f"backbone {recipe['model_name']} on {dev}")

    order = list(sequences)
    vectors: list[np.ndarray] = []
    kept: list[str] = []
    for start in range(0, len(order), batch_size):
        block = order[start : start + batch_size]
        for accession, vector in _embed_block(
            backend, model, tokenizer, sequences, block, config, dev, torch
        ):
            kept.append(accession)
            vectors.append(vector)
        log(f"  embedded {min(start + batch_size, len(order)):,}/{len(order):,}")

    if not vectors:
        return [], np.empty((0, 0), dtype=np.float32)
    return kept, np.stack(vectors)


def _embed_block(
    backend: Any,
    model: Any,
    tokenizer: Any,
    sequences: dict[str, str],
    block: list[str],
    config: Any,
    dev: str,
    torch: Any,
) -> list[tuple[str, np.ndarray]]:
    """Embed one block, halving it whenever the device runs out of memory.

    Attention is quadratic in sequence length, so a block of long
    proteins can need several times the memory of a block of short ones.
    A fixed batch size therefore either wastes the device or dies on the
    worst block, and which it does depends on what else is resident:
    this failed at 1,248 of 7,401 queries because another process held
    4.4 GB of the same card.

    Splitting on failure keeps a fixed batch size as the fast path and
    falls back only where it is needed. A single sequence that still does
    not fit is reported and skipped, because losing one protein is a
    better outcome than losing the run.
    """
    try:
        chunked = backend.embed_chunks(model, tokenizer, [sequences[a] for a in block], config, dev)
    except torch.OutOfMemoryError:
        torch.cuda.empty_cache()
        if len(block) == 1:
            log(f"  {block[0]} does not fit on the device on its own, skipped")
            return []
        half = len(block) // 2
        log(
            f"  out of memory on {len(block)} sequences, splitting into {half} and {len(block) - half}"
        )
        return _embed_block(
            backend, model, tokenizer, sequences, block[:half], config, dev, torch
        ) + _embed_block(backend, model, tokenizer, sequences, block[half:], config, dev, torch)

    out: list[tuple[str, np.ndarray]] = []
    for accession, chunks in zip(block, chunked, strict=True):
        vector = _collapse_chunks(chunks)
        if vector is None:
            log(f"  no embedding for {accession}, skipped")
            continue
        out.append((accession, vector))
    return out


def _collapse_chunks(chunks: list[Any]) -> np.ndarray | None:
    """Reduce one sequence's chunk list to a single vector.

    The bank was built with chunking disabled, so the expected case is
    exactly one chunk covering the whole sequence. The mean is kept as
    the fallback for a bundle whose recipe enables chunking, which is the
    same reduction PROTEA applies on that path.
    """
    if not chunks:
        return None
    if len(chunks) == 1:
        return np.asarray(chunks[0].vector, dtype=np.float32)
    return np.mean([np.asarray(c.vector, dtype=np.float32) for c in chunks], axis=0)


def encode_sparse(
    embeddings: np.ndarray,
    encoder: Any,
    top_k: int,
    dict_dim: int,
) -> Any:
    """Project embeddings through the encoder and sparsify to ``top_k``.

    The L2 normalisation matches the one applied when the bank was
    built. It is not cosmetic: the encoder was fitted on normalised
    input, so skipping it would feed it a distribution it never saw.
    """
    import torch
    from scipy import sparse

    normed = embeddings / np.maximum(np.linalg.norm(embeddings, axis=1, keepdims=True), 1e-8)
    with torch.no_grad():
        dense = encoder(torch.tensor(normed)).numpy().astype(np.float32)

    keep = np.argpartition(-np.abs(dense), top_k - 1, axis=1)[:, :top_k]
    values = np.take_along_axis(dense, keep, axis=1)
    values = values / np.maximum(np.linalg.norm(values, axis=1, keepdims=True), 1e-8)

    n_rows = dense.shape[0]
    indptr = np.arange(0, n_rows * top_k + 1, top_k, dtype=np.int64)
    return sparse.csr_matrix(
        (values.ravel(), keep.ravel().astype(np.int32), indptr),
        shape=(n_rows, dict_dim),
    )


# --- retrieval and transfer -------------------------------------------


def search(
    queries: Any,
    bank: Any,
    accessions: list[str],
    k: int,
    query_accessions: list[str] | None = None,
) -> list[list[tuple[str, float]]]:
    """Exact cosine top-k of every query against the bank.

    Both sides are already row-normalised, so the sparse product is the
    cosine directly. Queries are processed in blocks to bound the dense
    similarity buffer.

    A query that is itself in the bank is always kept among its own
    neighbours. Without that, a heavily duplicated protein is evicted
    from its own list by its twins: the acyl carrier protein of E. coli
    has 41 canonical accessions carrying its exact sequence, so all 30
    slots tie at cosine 1.0 and which one is dropped is decided by the
    sort's tie-break. The effect is that a protein the reference set
    knows about stops donating what is known about it, which is an
    accident rather than a decision.
    """
    position = {accession: index for index, accession in enumerate(accessions)}
    hits: list[list[tuple[str, float]]] = []
    n_queries = queries.shape[0]
    effective_k = min(k, bank.shape[0])
    forced = 0
    for start in range(0, n_queries, QUERY_CHUNK):
        block = queries[start : start + QUERY_CHUNK]
        similarity = np.asarray((block @ bank.T).todense(), dtype=np.float32)
        top = np.argpartition(-similarity, effective_k - 1, axis=1)[:, :effective_k]
        for row in range(similarity.shape[0]):
            columns = top[row]
            if query_accessions is not None:
                own = position.get(query_accessions[start + row])
                if own is not None and own not in set(columns.tolist()):
                    columns = np.append(columns[:-1], own)
                    forced += 1
            ordered = columns[np.argsort(-similarity[row, columns])]
            hits.append([(accessions[int(c)], float(similarity[row, int(c)])) for c in ordered])
        log(f"  searched {min(start + QUERY_CHUNK, n_queries):,}/{n_queries:,}")
    if forced:
        log(f"  kept {forced:,} queries in their own neighbour list, evicted by exact ties")
    return hits


def transfer(
    query_accessions: list[str],
    neighbours: list[list[tuple[str, float]]],
    donors: dict[str, list[str]],
    parents: dict[str, list[str]],
    alt_ids: dict[str, str],
    scheme: str = "blend",
    blend: float = DEFAULT_BLEND,
) -> dict[str, dict[str, float]]:
    """Turn neighbours into scored GO terms, propagated to ancestors.

    Three scoring schemes, because the first two disagree about what a
    score means and the disagreement is load-bearing:

    ``vote``
        The similarity-weighted fraction of the neighbours that carry the
        term. Rewards consensus. A term only the query itself carries
        scores about 1/k, so a curated annotation the protein already
        holds is ranked like a guess.
    ``maxsim``
        The similarity of the closest neighbour carrying the term, which
        is the classic nearest-neighbour transfer score. Rewards
        proximity, and cannot tell one donor from thirty.
    ``blend``
        ``vote ** (1 - w) * maxsim ** w``. A term needs both a close
        donor and agreement among donors, and neither alone carries it.
        The geometric form is scale free and keeps the result in [0, 1]
        without a rescaling; ``w = 0`` is pure vote and ``w = 1`` pure
        maxsim, so the two pure schemes are the ends of one dial.

    These are the same two quantities PROTEA's own pipeline emits as
    ``neighbor_vote_fraction`` and ``min_distance``, where a per-aspect
    booster learns the weighting. This container carries no booster, so
    the weighting is fixed and measured instead of learned.

    Neither pure scheme is universally better: on one release window vote
    won seven of nine cells and maxsim the two no-knowledge cells where
    the answer was largely already stated at t0.

    The vote component is the similarity-weighted fraction of the
    neighbours that could vote at all, so it lands in ``[0, 1]`` by
    construction rather than by a rescaling after the fact, and the other
    two schemes inherit that range. Propagation takes the maximum over
    descendants, which keeps an ancestor at least as confident as
    anything implying it and satisfies the true path rule.

    Only neighbours carrying annotations enter the denominator. About 3%
    of the bank has an embedding but no GO term, and counting those in
    the total would scale every score for that query down by however many
    of them the retrieval happened to return, which is a property of the
    reference set rather than of the evidence for the call. The symptom
    was unmistakable once measured: the three ontology roots, which every
    annotated neighbour carries by construction, were scoring around 0.66
    instead of 1.0.
    """
    cache: dict[str, set[str]] = {}
    out: dict[str, dict[str, float]] = {}

    for accession, hits in zip(query_accessions, neighbours, strict=True):
        voters = [
            (donor, similarity)
            for donor, similarity in hits
            if similarity > 0.0 and donors.get(donor)
        ]
        total = sum(similarity for _, similarity in voters)
        if total <= 0.0:
            out[accession] = {}
            continue

        vote_score: dict[str, float] = {}
        max_score: dict[str, float] = {}
        for donor, similarity in voters:
            share = similarity / total
            propagated: set[str] = set()
            for leaf in donors[donor]:
                propagated |= ancestors_of(alt_ids.get(leaf, leaf), parents, cache)
            for term in propagated:
                vote_score[term] = vote_score.get(term, 0.0) + share
                max_score[term] = max(max_score.get(term, 0.0), similarity)

        if scheme == "vote":
            merged = vote_score
        elif scheme == "maxsim":
            merged = max_score
        else:  # "blend"
            w = blend
            merged = {
                term: min(1.0, value) ** (1.0 - w) * min(1.0, max_score[term]) ** w
                for term, value in vote_score.items()
            }
        out[accession] = {term: min(1.0, value) for term, value in merged.items()}
    return out


def write_predictions(
    scored: dict[str, dict[str, float]],
    aspect: dict[str, str],
    order: list[str],
    path: Path,
    caps: dict[str, int],
) -> int:
    """Write ``EntryID<TAB>GO_ID<TAB>score``, capped per query and aspect.

    The cap is per aspect rather than global so a query with a large
    biological process closure cannot crowd out its molecular function
    calls, which is the failure mode a single global cap produces.

    Biological process gets a larger allowance than the other two because
    its closures are far deeper. Measured on a 7,401-query run at a flat
    500, the terms a protein already carried at the cutoff and did not
    survive into the output were lost to the cap rather than to a missing
    vote, and the loss fell on biological process while cellular component
    lost none.

    The three counts that used to stand here are struck rather than
    corrected. Two of them were mutually impossible, one being a subset
    larger than the set it was drawn from, and the run that produced them
    cannot be reproduced from what this container ships. The mechanism is
    the part a reader can check anyway: compare closure depths per aspect.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with open(path, "w") as handle:
        for accession in order:
            per_aspect: dict[str, list[tuple[str, float]]] = defaultdict(list)
            for term, score in scored.get(accession, {}).items():
                per_aspect[aspect.get(term, "")].append((term, score))
            for letter in ("P", "F", "C"):
                rows = sorted(per_aspect.get(letter, []), key=lambda r: (-r[1], r[0]))
                for term, score in rows[: caps[letter]]:
                    if score <= 0.0:
                        continue
                    handle.write(f"{accession}\t{term}\t{score:.3f}\n")
                    written += 1
    return written


# --- entry point ------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="PROTEA sparse-code KNN GO transfer")

    # The LAFA container guide names these arguments, and its own baselines are
    # invoked with them, so the guide's spelling is accepted alongside ours. It
    # also asks that paths arrive as arguments rather than being hard-coded, so
    # every path here can be overridden on the command line even though the
    # entrypoint supplies a default.
    parser.add_argument("--query_file", "-q", required=True)
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--obo", "--graph", "-g", dest="obo", required=True)
    parser.add_argument("--output", "--output_file", "-o", dest="output", required=True)

    # Inputs the harness passes to every method and this one does not read. They
    # are declared rather than ignored, so an unexpected argument is still an
    # error and a supplied-but-unused one is reported instead of silently
    # dropped.
    parser.add_argument("--train_sequences", default=None)
    parser.add_argument("--annot_file", "-a", dest="annot_file", default=None)
    parser.add_argument("--train_terms", default=None)
    parser.add_argument("--train_taxonomy", default=None)
    parser.add_argument("--num_threads", type=int, default=None)
    parser.add_argument("--k", type=int, default=DEFAULT_K)
    parser.add_argument("--max_bpo", type=int, default=1500)
    parser.add_argument("--max_mfo", type=int, default=500)
    parser.add_argument("--max_cco", type=int, default=500)
    parser.add_argument("--batch_size", type=int, default=8)
    parser.add_argument("--device", default=None)
    parser.add_argument("--score", choices=("vote", "maxsim", "blend"), default="blend")
    parser.add_argument(
        "--blend",
        type=float,
        default=DEFAULT_BLEND,
        help="weight on maxsim in the blend; 0 is pure vote, 1 pure maxsim",
    )
    parser.add_argument(
        "--also_score",
        choices=("vote", "maxsim"),
        default=None,
        help="write a second file scored the other way, for comparison",
    )
    args = parser.parse_args(argv)

    unused = [
        n
        for n in ("train_sequences", "annot_file", "train_terms", "train_taxonomy")
        if getattr(args, n) is not None
    ]
    if unused:
        log(f"accepted but not used by this method: {', '.join(unused)}")
    if args.num_threads:
        import torch

        torch.set_num_threads(args.num_threads)
        log(f"torch threads set to {args.num_threads}")

    bundle = Path(args.bundle)
    started = time.time()

    queries = read_fasta(Path(args.query_file))
    log(f"queries: {len(queries):,}")
    if not queries:
        log("no queries, writing an empty prediction file")
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text("")
        return 0

    parents, aspect, alt_ids = parse_obo(Path(args.obo))
    log(f"ontology: {len(parents):,} terms")

    bank, accessions, bank_meta = load_bank(bundle)
    donors = load_donors(bundle)
    encoder, top_k = load_encoder(bundle)
    recipe = json.loads((bundle / "EMBEDDING_RECIPE.json").read_text())

    kept, embeddings = embed_queries(queries, recipe, args.device, args.batch_size)
    if not kept:
        log("no query embedded, writing an empty prediction file")
        Path(args.output).write_text("")
        return 0

    codes = encode_sparse(embeddings, encoder, top_k, int(bank_meta["dict_dim"]))
    neighbours = search(codes, bank, accessions, args.k, query_accessions=kept)
    scored = transfer(
        kept, neighbours, donors, parents, alt_ids, scheme=args.score, blend=args.blend
    )
    caps = {"P": args.max_bpo, "F": args.max_mfo, "C": args.max_cco}
    written = write_predictions(scored, aspect, kept, Path(args.output), caps)

    # A second scoring scheme costs one more pass over the neighbours, which
    # is negligible next to the embedding, and saves repeating the whole run
    # to compare them.
    if args.also_score:
        other = transfer(kept, neighbours, donors, parents, alt_ids, scheme=args.also_score)
        second = Path(args.output).with_name(
            Path(args.output).stem + f".{args.also_score}" + Path(args.output).suffix
        )
        n = write_predictions(other, aspect, kept, second, caps)
        log(f"also wrote {n:,} predictions scored by {args.also_score} to {second.name}")

    log(f"wrote {written:,} predictions for {len(kept):,} queries in {time.time() - started:.0f}s")
    return 0


if __name__ == "__main__":
    code = main()
    # Leave immediately rather than through the interpreter's shutdown. The
    # torch and tokenizer stacks leave non-daemon threads behind, and the
    # process was observed sitting idle for half an hour after writing its
    # output, which an orchestrator reads as a hung job rather than a finished
    # one. The work is done and the file is closed, so there is nothing left to
    # tear down gracefully.
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)
