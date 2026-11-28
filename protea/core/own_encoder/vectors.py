"""Every bank sequence's vector under a trained segment encoder, as an embedding bank (P4).

The population is the one an embedding bank prepared (ADR-D62: ``sequences.parquet``,
``shards.json`` and ``prepare.json`` in the bank folder), and the input is the lexicon
cut of that population (ADR-D63), one cut shard per bank shard and in the same order:
a cut shard whose md5s are not its bank shard's is refused. The encoder is the target
encoder of a complete training record (``own-encoder.json``), whose ``encoder.npz``
must hash to the sha256 the job names before anything is read.

A sequence's vector is the target encoder's mean output over its segments (at most
its first 64, as the laboratory read them), 256 values stored in half precision, the
dtype of the platform's ``halfvec`` column. Every sequence has at least one segment in
the lexicons decoded so far; one without any gets no vector and refuses the shard.

``vectors_pass`` writes ``vectors/<config>/shard-NNNNN.parquet`` (``md5`` and
``vector``, the embedding bank's format) with a record per shard, skips shards with a
record and stops before a shard it could not finish in the time it has, so a job
resumes where the last one ended. Once no shard of the bank is missing,
``manifest_pass`` writes ``bank-<config>.json``, the manifest ``load_embedding_bank``
verifies: the recipe of :func:`protea.core.own_encoder.configuration_recipe` (whose
derived id is the configuration's), the photos and counts of the prepare record, every
shard's sha256, the weights' sha256 as the model's revision, the software and the
card, and the encoder's own block (its family, its training record, its lexicon).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq
import torch

from protea.core import file_stage
from protea.core.embedding_bank.build import write_shard
from protea.core.file_stage import file_sha256, read_record, write_record
from protea.core.own_encoder import FAMILY, NAME, configuration_id, configuration_recipe
from protea.core.own_encoder.corpus import (
    Corpus,
    CorpusRefused,
    LexiconCut,
    checked_lexicon_cut,
    read_corpus,
)
from protea.core.own_encoder.model import (
    MAX_SEGMENTS,
    WIDTH,
    SegmentEncoder,
    collate,
    encoder_from_bytes,
    segment_item,
)

__all__ = ["VectorsRun", "encode", "main", "manifest_pass", "vectors_pass"]

#: Rows times the widest row of a batch: 512 proteins of 64 segments.
DEFAULT_TOKEN_BUDGET = 32_768
_SOFTWARE = ("torch", "numpy", "pyarrow")


@dataclass(frozen=True)
class VectorsRun:
    """One vectors job: the bank, the lexicon, the encoder and the shards it may do."""

    bank: Path
    lexicon: str
    encoder: Path
    encoder_sha256: str
    first: int
    last: int
    device: str = "cuda"
    token_budget: int = DEFAULT_TOKEN_BUDGET
    stop_after: float | None = None
    run_id: str = "local"
    revision: str = "unknown"


def checked_encoder(folder: Path, expected_sha256: str) -> tuple[SegmentEncoder, dict[str, Any]]:
    """The trained target encoder and its record, refused unless complete and the named weights."""
    record = read_record(folder / "own-encoder.json")
    if record.get("status") != "complete":
        raise CorpusRefused(
            f"the encoder in {folder} is {record.get('status')!r}: only a complete training "
            "gives a bank (a measurement's weights never do)"
        )
    got = file_sha256(folder / "encoder.npz")
    if got != expected_sha256 or record["encoder"]["sha256"] != got:
        raise CorpusRefused(f"encoder.npz hashes to {got}, not to {expected_sha256}")
    encoder = encoder_from_bytes((folder / "encoder.npz").read_bytes())
    return encoder, record


def _batches(counts: np.ndarray, budget: int) -> list[range]:
    """Consecutive rows whose count times the widest row stays within ``budget`` tokens."""
    batches, start, widest = [], 0, 0
    for row, count in enumerate(np.minimum(counts, MAX_SEGMENTS)):
        wider = max(widest, int(count))
        if row > start and (row + 1 - start) * wider > budget:
            batches.append(range(start, row))
            start, wider = row, int(count)
        widest = wider
    if start < len(counts):
        batches.append(range(start, len(counts)))
    return batches


def encode(
    encoder: SegmentEncoder, corpus: Corpus, device: str, budget: int = DEFAULT_TOKEN_BUDGET
) -> np.ndarray:
    """Every protein's mean output over its first segments, float32, in the corpus's order.

    A protein without a segment has nothing to average: its row is not a number,
    which the shard's record counts and the manifest refuses.
    """
    counts = corpus.counts()
    vectors = np.full((len(corpus), WIDTH), np.nan, dtype=np.float32)
    present = np.flatnonzero(counts > 0)
    rng = np.random.default_rng(0)  # never read: nothing is hidden
    encoder = encoder.to(device).eval()
    with torch.no_grad():
        for rows in _batches(counts[present], budget):
            proteins = present[rows.start : rows.stop]
            items = [segment_item(corpus.segments(int(p)), rng, hide=False) for p in proteins]
            batch = collate(items, device)
            with torch.autocast(device, dtype=torch.bfloat16, enabled=device == "cuda"):
                out = encoder(batch).float()
            keep = (~batch["padding"]).unsqueeze(-1).float()
            vectors[proteins] = ((out * keep).sum(1) / keep.sum(1)).cpu().numpy()
    return vectors


def _bank_md5s(bank: Path, shard: int) -> list[str]:
    table = pq.ParquetFile(bank / "sequences.parquet").read_row_group(shard, columns=["md5"])
    return [str(m) for m in table.column("md5").to_pylist()]


def _vectors_shard(
    run: VectorsRun, cut: LexiconCut, encoder: SegmentEncoder, out: Path, shard: int
) -> dict[str, Any]:
    """One shard's vectors and record; refused when its cut is not the bank shard's sequences."""
    started = time.monotonic()
    corpus = read_corpus(cut, [shard])
    if corpus.md5 != _bank_md5s(run.bank, shard):
        raise CorpusRefused(
            f"cut shard {shard} does not hold bank shard {shard}'s sequences in order"
        )
    counts = corpus.counts()
    half = encode(encoder, corpus, run.device, run.token_budget).astype(np.float16)
    finite = np.isfinite(half.astype(np.float32)).all(axis=1) & (counts > 0)
    path = write_shard(out, shard, corpus.md5, half)
    record = {
        "shard": shard,
        "rows": len(corpus),
        "dim": int(half.shape[1]),
        "segments": int(counts.sum()),
        "without_segments": int((counts == 0).sum()),
        "over_max_segments": int((counts > MAX_SEGMENTS).sum()),
        "not_finite": int((~finite).sum()),
        "not_finite_examples": [m for m, ok in zip(corpus.md5, finite, strict=True) if not ok][:5],
        "sha256": file_sha256(path),
        "seconds": round(time.monotonic() - started, 1),
        "run": run.run_id,
    }
    write_record(path.with_suffix(".json"), record)
    return record


def _todo(run: VectorsRun, out: Path) -> list[int]:
    shards = read_record(run.bank / "shards.json")["shards"]
    wanted = [int(s["shard"]) for s in shards if run.first <= int(s["shard"]) <= run.last]
    return [k for k in wanted if not (out / f"shard-{k:05d}.json").exists()]


def vectors_pass(run: VectorsRun) -> dict[str, Any]:
    """The shards ``first`` to ``last`` not done yet; stop before one there is no time for."""
    started = time.monotonic()
    encoder, _ = checked_encoder(run.encoder, run.encoder_sha256)
    cut = checked_lexicon_cut(run.bank, run.lexicon)
    config = str(configuration_id(run.encoder_sha256))
    out = run.bank / "vectors" / config
    out.mkdir(parents=True, exist_ok=True)
    todo = _todo(run, out)
    done: list[dict[str, Any]] = []
    slowest, stopped = 0.0, False
    for shard in todo:
        if run.stop_after and done and time.monotonic() - started + 1.25 * slowest > run.stop_after:
            stopped = True
            break
        done.append(_vectors_shard(run, cut, encoder, out, shard))
        slowest = max(slowest, done[-1]["seconds"])
        print(
            f"{time.strftime('%H:%M:%S')} shard {shard} rows {done[-1]['rows']} {done[-1]['seconds']} s",
            flush=True,
        )
    record = {
        "pass": "vectors",
        "config_id": config,
        "run_id": run.run_id,
        "range": [run.first, run.last],
        "token_budget": run.token_budget,
        "model_revision": run.encoder_sha256,
        "environment": file_stage.environment(run.device, _SOFTWARE),
        "shards_done": [r["shard"] for r in done],
        "shards_left": [k for k in todo if k not in {r["shard"] for r in done}],
        "stopped_for_time": stopped,
        "seconds": round(time.monotonic() - started),
    }
    if done:
        (out / "runs").mkdir(exist_ok=True)
        write_record(out / "runs" / f"{run.run_id}.json", record)
    return record


# Manifest ------------------------------------------------------------------------


def _shard_problems(out: Path, shard: dict[str, Any]) -> list[str]:
    name = f"shard-{int(shard['shard']):05d}"
    if not (out / f"{name}.json").exists() or not (out / f"{name}.parquet").exists():
        return [f"{name} missing"]
    record = read_record(out / f"{name}.json")
    problems = []
    if file_sha256(out / f"{name}.parquet") != record["sha256"]:
        problems.append(f"{name} does not hash to its record")
    if record["rows"] != shard["rows"]:
        problems.append(f"{name} holds {record['rows']} rows, the shard {shard['rows']}")
    if record["not_finite"]:
        problems.append(
            f"{name} holds {record['not_finite']} vectors that are not finite "
            f"({record['without_segments']} sequences without a segment)"
        )
    return problems


def _regime(out: Path, records: list[dict[str, Any]]) -> dict[str, list[str]]:
    runs = [read_record(out / "runs" / f"{r}.json") for r in sorted({x["run"] for x in records})]
    return {
        "software": sorted(
            {json.dumps(r["environment"]["software"], sort_keys=True) for r in runs}
        ),
        "card": sorted({str(r["environment"]["card"]) for r in runs}),
        "model_revision": sorted({str(r["model_revision"]) for r in runs}),
        "token_budget": sorted({str(r["token_budget"]) for r in runs}),
        "runs": [str(r["run_id"]) for r in runs],
    }


def _own_block(
    encoder: dict[str, Any], folder: Path, records: list[dict[str, Any]]
) -> dict[str, Any]:
    return {
        "name": NAME,
        "family": FAMILY,
        "encoder_sha256": encoder["encoder"]["sha256"],
        "training_record_sha256": file_sha256(folder / "own-encoder.json"),
        "members": encoder["members"],
        "lexicon": encoder["lexicon"],
        "segments_read": MAX_SEGMENTS,
        "read_on_their_first_segments": sum(int(r["over_max_segments"]) for r in records),
    }


def manifest_pass(bank: Path, lexicon: str, encoder_folder: Path, revision: str) -> dict[str, Any]:
    """Write ``bank-<config>.json``: ``complete``, or ``refused`` with every reason."""
    encoder = read_record(encoder_folder / "own-encoder.json")
    sha = str(encoder["encoder"]["sha256"])
    config = str(configuration_id(sha))
    prepare = read_record(bank / "prepare.json")
    shards = read_record(bank / "shards.json")["shards"]
    out = bank / "vectors" / config
    problems = [p for shard in shards for p in _shard_problems(out, shard)]
    if encoder.get("status") != "complete":
        problems.append(f"the encoder's training is {encoder.get('status')!r}, not complete")
    if (
        encoder["lexicon"]["bank_manifest_sha256"]
        != checked_lexicon_cut(bank, lexicon).manifest_sha256
    ):
        problems.append(f"the encoder was trained on another lexicon bank than {lexicon}'s")
    records = (
        [] if problems else [read_record(out / f"shard-{int(s['shard']):05d}.json") for s in shards]
    )
    regime = _regime(out, records) if records else {}
    for key in ("software", "card", "model_revision", "token_budget"):
        if len(regime.get(key, [])) > 1:
            problems.append(f"the shards were computed under {len(regime[key])} values of {key}")
    if records and regime["model_revision"] != [sha]:
        problems.append("the shards were computed with other weights than the encoder's")
    manifest = {
        "table": "embedding_bank",
        "config_id": config,
        "recipe": configuration_recipe(sha),
        "photos": prepare["photos"],
        "sequences": prepare["sequences"],
        "residues": prepare["residues"],
        "sequences_parquet_sha256": prepare["sequences_parquet_sha256"],
        "dim": records[0]["dim"] if records else None,
        "shards": [
            {"shard": r["shard"], "rows": r["rows"], "sha256": r["sha256"]} for r in records
        ],
        "model_revision": sha,
        "software": json.loads(regime["software"][0])
        if len(regime.get("software", [])) == 1
        else None,
        "card": regime["card"][0] if len(regime.get("card", [])) == 1 else None,
        "token_budgets": sorted(int(b) for b in regime.get("token_budget", [])),
        "runs": regime.get("runs", []),
        "own_encoder": _own_block(encoder, encoder_folder, records),
        "status": "refused" if problems else "complete",
        "refusals": problems,
        "revision": revision,
    }
    write_record(bank / f"bank-{config}.json", manifest)
    return manifest


# Command line --------------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m protea.core.own_encoder.vectors")
    parser.add_argument("--bank", type=Path, required=True)
    parser.add_argument("--lexicon", required=True)
    parser.add_argument("--encoder", type=Path, required=True)
    parser.add_argument("--encoder-sha256", required=True)
    parser.add_argument("--first", type=int, required=True)
    parser.add_argument("--last", type=int, required=True)
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--token-budget", type=int, default=DEFAULT_TOKEN_BUDGET)
    parser.add_argument("--stop-after", type=int, default=None)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--revision", required=True)
    return parser


def _missing(bank: Path, config: str) -> int:
    shards = read_record(bank / "shards.json")["shards"]
    out = bank / "vectors" / config
    return sum(1 for s in shards if not (out / f"shard-{int(s['shard']):05d}.json").exists())


def main(argv: Sequence[str] | None = None) -> int:
    """0 done, 2 refused, 3 manifest refused, 4 stopped for time (resubmit the range).

    The manifest is written as soon as no shard of the bank is missing; a range that
    leaves shards for other jobs (a pilot of one shard) ends without one.
    """
    args = _parser().parse_args(argv)
    run = VectorsRun(
        bank=args.bank,
        lexicon=args.lexicon,
        encoder=args.encoder,
        encoder_sha256=args.encoder_sha256,
        first=args.first,
        last=args.last,
        device=args.device,
        token_budget=args.token_budget,
        stop_after=args.stop_after,
        run_id=args.run_id,
        revision=args.revision,
    )
    try:
        record = vectors_pass(run)
        if record["shards_left"]:
            return 4
        missing = _missing(run.bank, record["config_id"])
        if missing:
            print(f"the manifest waits for {missing} shards of the bank", flush=True)
            return 0
        manifest = manifest_pass(run.bank, run.lexicon, run.encoder, run.revision)
    except CorpusRefused as refused:
        print(f"REFUSED: {refused}", file=sys.stderr)
        return 2
    return 0 if manifest["status"] == "complete" else 3


if __name__ == "__main__":
    raise SystemExit(main())
