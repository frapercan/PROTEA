"""The lexicon bank of the photos, computed away from the server (ADR-D63).

It decodes the population an embedding bank prepared (``sequences.parquet`` and
``shards.json`` of ADR-D62: every member sequence of the photos, by md5, in
length order, one row group per shard) with one lexicon on one card, through
the platform's decoder (``protea.core.lexicon``):

``cut`` writes ``lexicon/<name>/cut/shard-NNNNN.parquet``: per sequence its md5,
the bits of its best path, its segments (start, end, theme) and every residue's
column on that path.

``mass`` writes ``lexicon/<name>/mass/shard-NNNNN.parquet``: per sequence its md5
and its theme mass, sparse (themes and their shares of the length).

A shard is written whole, then its record. A shard with a record is skipped, and
a pass stops before a shard it could not finish in the time it has, so a job of
the cluster's two hours resumes where the last one ended. The lexicon's file
must hash to the sha256 the job names before anything is read.

``manifest`` writes ``lexicon/<name>/lexicon-bank.json``: the lexicon (sha256,
step, themes, columns), the population (the prepare record), the parity record
measured against the laboratory's code, every shard's sha256 and rows, and the
runs' software and card. Its status is ``complete``, or ``refused`` when a shard
is missing or changed, or the shards were cut under two cards or software sets.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from protea.core import file_stage
from protea.core.file_stage import file_sha256, read_record, write_record

PASSES = ("cut", "mass")
_SOFTWARE = ("torch", "triton", "numpy", "pyarrow")


class LexiconBankRefused(RuntimeError):
    """The inputs cannot make a lexicon bank; nothing is written past the refusal."""


def lexicon_dir(bank: Path, name: str) -> Path:
    return bank / "lexicon" / name


def _lists(rows: Sequence[Any], kind: pa.DataType) -> pa.Array:
    """A list column from Python or numpy rows, through one flat array and its offsets."""
    offsets = np.zeros(len(rows) + 1, dtype=np.int64)
    offsets[1:] = np.cumsum([len(r) for r in rows])
    flat = np.concatenate([np.asarray(r) for r in rows]) if rows else np.zeros(0)
    return pa.LargeListArray.from_arrays(pa.array(offsets), pa.array(flat, type=kind))


def cut_table(md5s: list[str], records: list[dict]) -> pa.Table:
    """The cut of a shard as columns: md5, best_bits, segment starts, ends and themes, columns."""
    segs = [r["segments"] for r in records]
    return pa.table(
        {
            "md5": pa.array(md5s, pa.string()),
            "best_bits": pa.array([r["best_bits"] for r in records], pa.float32()),
            "seg_start": _lists([[s[0] for s in g] for g in segs], pa.int32()),
            "seg_end": _lists([[s[1] for s in g] for g in segs], pa.int32()),
            "seg_theme": _lists([[s[2] for s in g] for g in segs], pa.int32()),
            "columns": _lists([r["columns"] for r in records], pa.int16()),
        }
    )


def mass_table(md5s: list[str], masses: list[tuple[np.ndarray, np.ndarray]]) -> pa.Table:
    """The theme mass of a shard as columns: md5, themes and their shares."""
    return pa.table(
        {
            "md5": pa.array(md5s, pa.string()),
            "themes": _lists([m[0] for m in masses], pa.int32()),
            "shares": _lists([m[1] for m in masses], pa.float16()),
        }
    )


def checked_lexicon(path: Path, expected_sha256: str) -> str:
    """The lexicon's sha256, refused unless it is the one the job names."""
    got = file_sha256(path)
    if got != expected_sha256:
        raise LexiconBankRefused(f"{path} hashes to {got}, not to {expected_sha256}")
    return got


def _shard_rows(bank: Path, shard: int) -> tuple[list[str], list[str]]:
    table = pq.ParquetFile(bank / "sequences.parquet").read_row_group(
        shard, columns=["md5", "sequence"]
    )
    return table.column("md5").to_pylist(), table.column("sequence").to_pylist()


def _decode_shard(bank: Path, shard: int, ctx: dict[str, Any]) -> pa.Table:
    from protea.core.lexicon.decode import cut, theme_mass

    md5s, sequences = _shard_rows(bank, shard)
    if ctx["pass"] == "cut":
        return cut_table(md5s, cut(ctx["lexicon"], sequences, ctx["device"], ctx["budget"]))
    return mass_table(md5s, theme_mass(ctx["lexicon"], sequences, ctx["device"], ctx["budget"]))


def write_shard(out: Path, shard: int, table: pa.Table, record: dict[str, Any]) -> dict[str, Any]:
    """The shard's parquet, written whole, then its record with the file's sha256."""
    path = out / f"shard-{shard:05d}.parquet"
    partial = path.with_suffix(".partial")
    pq.write_table(table, partial, compression="zstd")
    partial.rename(path)
    record = {**record, "shard": shard, "rows": table.num_rows, "sha256": file_sha256(path)}
    write_record(path.with_suffix(".json"), record)
    return record


def _todo(bank: Path, out: Path, run: dict[str, Any]) -> list[int]:
    shards = read_record(bank / "shards.json")["shards"]
    wanted = [s["shard"] for s in shards if run["first"] <= s["shard"] <= run["last"]]
    return [k for k in wanted if not (out / f"shard-{k:05d}.json").exists()]


def _context(run: dict[str, Any]) -> dict[str, Any]:
    from protea.core.lexicon.decode import CUT_BUDGET, MASS_BUDGET, Budget
    from protea.core.lexicon.model import load_lexicon

    base = CUT_BUDGET if run["pass"] == "cut" else MASS_BUDGET
    budget = Budget(**{**base.__dict__, **run.get("budget", {})})
    return {
        "pass": run["pass"],
        "lexicon": load_lexicon(Path(run["lexicon"]), run["device"]),
        "device": run["device"],
        "budget": budget,
    }


def decode_pass(bank: Path, name: str, run: dict[str, Any]) -> dict[str, Any]:
    """Decode shards ``run['first']`` to ``run['last']`` with one pass; skip done ones, stop before the time.

    ``run`` holds ``pass`` (cut or mass), ``lexicon`` and ``lexicon_sha256``, ``first``,
    ``last``, ``device``, ``stop_after`` (seconds, or None), ``run_id`` and an optional
    ``budget`` (fields of ``protea.core.lexicon.decode.Budget``).
    """
    started = time.monotonic()
    sha = checked_lexicon(Path(run["lexicon"]), run["lexicon_sha256"])
    out = lexicon_dir(bank, name) / run["pass"]
    out.mkdir(parents=True, exist_ok=True)
    todo = _todo(bank, out, run)
    ctx = _context(run) if todo else {}
    done: list[dict[str, Any]] = []
    slowest, stopped = 0.0, False
    for shard in todo:
        if (
            run.get("stop_after")
            and done
            and time.monotonic() - started + 1.25 * slowest > run["stop_after"]
        ):
            stopped = True
            break
        t0 = time.monotonic()
        table = _decode_shard(bank, shard, ctx)
        seconds = round(time.monotonic() - t0, 1)
        done.append(write_shard(out, shard, table, {"seconds": seconds, "run": run["run_id"]}))
        slowest = max(slowest, seconds)
        print(
            f"{time.strftime('%H:%M:%S')} shard {shard} rows {table.num_rows} {seconds} s",
            flush=True,
        )
    record = {
        "pass": run["pass"],
        "lexicon_sha256": sha,
        "run_id": run["run_id"],
        "range": [run["first"], run["last"]],
        "budget": ctx["budget"].__dict__ if ctx else None,
        "environment": file_stage.environment(run["device"], _SOFTWARE),
        "shards_done": [r["shard"] for r in done],
        "shards_left": [k for k in todo if k not in {r["shard"] for r in done}],
        "stopped_for_time": stopped,
        "seconds": round(time.monotonic() - started),
    }
    if ctx:
        (out / "runs").mkdir(exist_ok=True)
        write_record(out / "runs" / f"{run['run_id']}.json", record)
    return record


def _pass_problems(out: Path, shards: list[dict[str, Any]]) -> tuple[list[str], list[dict]]:
    """Every shard of one pass present, unchanged and complete; its records."""
    problems, records = [], []
    for shard in shards:
        stem = f"shard-{shard['shard']:05d}"
        if not (out / f"{stem}.json").exists() or not (out / f"{stem}.parquet").exists():
            problems.append(f"{out.name}/{stem} missing")
            continue
        record = read_record(out / f"{stem}.json")
        if file_sha256(out / f"{stem}.parquet") != record["sha256"]:
            problems.append(f"{out.name}/{stem} does not hash to its record")
        if record["rows"] != shard["rows"]:
            problems.append(
                f"{out.name}/{stem} holds {record['rows']} rows, the shard {shard['rows']}"
            )
        records.append(record)
    return problems, records


def _regime(out: Path, records: list[dict]) -> dict[str, list[str]]:
    runs = [read_record(out / "runs" / f"{r}.json") for r in sorted({x["run"] for x in records})]
    return {
        "card": sorted({r["environment"]["card"] for r in runs}),
        "software": sorted(
            {json.dumps(r["environment"]["software"], sort_keys=True) for r in runs}
        ),
        "lexicon_sha256": sorted({r["lexicon_sha256"] for r in runs}),
        "runs": [r["run_id"] for r in runs],
    }


def _first(values: list[str]) -> str | None:
    return values[0] if values else None


def manifest_pass(bank: Path, name: str, lexicon: dict[str, Any], revision: str) -> dict[str, Any]:
    """Write ``lexicon-bank.json`` from the population, the parity record and both passes' shards.

    ``lexicon`` holds the ``sha256`` the jobs checked, and ``parity``, the path of the
    parity record measured against the laboratory's code.
    """
    root = lexicon_dir(bank, name)
    prepare = read_record(bank / "prepare.json")
    shards = read_record(bank / "shards.json")["shards"]
    problems, passes = [], {}
    for name_ in PASSES:
        missing, records = _pass_problems(root / name_, shards)
        problems += missing
        regime = _regime(root / name_, records) if records and not missing else {}
        for key in ("card", "software", "lexicon_sha256"):
            if len(regime.get(key, [])) > 1:
                problems.append(f"{name_} was computed under {len(regime[key])} values of {key}")
        if regime and regime["lexicon_sha256"] != [lexicon["sha256"]]:
            problems.append(f"{name_} was computed with another lexicon")
        passes[name_] = {
            "shards": [
                {"shard": r["shard"], "rows": r["rows"], "sha256": r["sha256"]} for r in records
            ],
            "runs": regime.get("runs", []),
            "card": _first(regime.get("card", [])),
            "software": _first(regime.get("software", [])),
        }
    parity = read_record(Path(lexicon["parity"])) if lexicon.get("parity") else None
    if parity is None:
        problems.append("no parity record against the laboratory's code")
    manifest = {
        "table": "lexicon_bank",
        "name": name,
        "lexicon_sha256": lexicon["sha256"],
        "photos": prepare["photos"],
        "sequences": prepare["sequences"],
        "residues": prepare["residues"],
        "sequences_parquet_sha256": prepare["sequences_parquet_sha256"],
        "passes": passes,
        "parity": parity,
        "status": "refused" if problems else "complete",
        "refusals": problems,
        "revision": revision,
    }
    write_record(root / "lexicon-bank.json", manifest)
    return manifest


# Command line --------------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m protea.core.lexicon_bank.build")
    sub = parser.add_subparsers(dest="pass_name", required=True)
    for name in PASSES:
        p = sub.add_parser(name)
        p.add_argument("--bank", type=Path, required=True)
        p.add_argument("--name", required=True)
        p.add_argument("--lexicon", type=Path, required=True)
        p.add_argument("--lexicon-sha256", required=True)
        p.add_argument("--first", type=int, required=True)
        p.add_argument("--last", type=int, required=True)
        p.add_argument("--device", default="cuda")
        p.add_argument("--run-id", required=True)
        p.add_argument("--stop-after", type=int, default=None)
        p.add_argument("--chunk-gb", type=float, default=None)
    p_man = sub.add_parser("manifest")
    p_man.add_argument("--bank", type=Path, required=True)
    p_man.add_argument("--name", required=True)
    p_man.add_argument("--lexicon-sha256", required=True)
    p_man.add_argument("--parity", type=Path, default=None)
    p_man.add_argument("--revision", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.pass_name == "manifest":
            lexicon = {"sha256": args.lexicon_sha256, "parity": args.parity}
            manifest = manifest_pass(args.bank, args.name, lexicon, args.revision)
            return 0 if manifest["status"] == "complete" else 3
        run = {
            "pass": args.pass_name,
            "lexicon": str(args.lexicon),
            "lexicon_sha256": args.lexicon_sha256,
            "first": args.first,
            "last": args.last,
            "device": args.device,
            "run_id": args.run_id,
            "stop_after": args.stop_after,
            "budget": {"chunk_gb": args.chunk_gb} if args.chunk_gb else {},
        }
        record = decode_pass(args.bank, args.name, run)
        return 4 if record["shards_left"] else 0
    except LexiconBankRefused as refused:
        print(f"REFUSED: {refused}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
