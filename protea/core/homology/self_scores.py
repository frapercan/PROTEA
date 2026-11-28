"""The self-alignment bit scores of homology tables' queries, built away from the server.

A homology table drops the hit of a sequence with itself, and a homology transfer
that weighs its voters by bit score needs that number for a twin: a member that
shares the target's sequence under another accession. Its bit score is the
target's self-alignment score, which this file stage computes for every query of
one or more tables, with no database and nothing sent: the photos are already on
the cluster.

``prepare`` reads each photo as :mod:`protea.core.homology.build` reads it (every
input checked against the photo's manifest) and takes its queries exactly as the
table's ``prepare`` pass takes them: the superset's unique sequences, by md5. It
records, per cut, the sha256 of the bytes that pass wrote as ``queries.fasta``, so
the loader can prove the scores cover exactly the table's queries, and writes the
union of the cuts' queries once. ``--sample N`` keeps the first N query md5s of the
first photo, in sorted order, and writes that photo's bank too, for the check.

``align`` checks the MMseqs2 binary against its sha256, makes one database of the
union, a pair database that lists each sequence with itself, and runs ``align``
with the tool's scoring (the table's search ran at its defaults) and no e-value
cut, then ``convertalis``. A sequence MMseqs2 cannot score is counted and named:
no row, or a bit score that is not a positive finite number within what the
alignment can hold (BLOSUM62 is written in half bits and its largest entry is 11,
so no alignment holds more than 5.5 bits per aligned residue; a run of ``X``
aligned with itself is answered with about 1.7e9).

``check``, in a sampled run, searches the sampled queries against the photo's bank
with ``easy-search`` at its defaults, keeps the hit of each query with itself, and
compares its bit score with the forced alignment's: the two must be equal.

``manifest`` writes, per cut, ``<cut>/self-scores.parquet`` (``query_md5``,
``fident``, ``alnlen``, ``evalue``, ``bits``) and ``<cut>/self-scores.json``: the
photo, the table's query bytes' sha256, MMseqs2 (version, binary sha256, command
lines), the counts and the sha256 of the parquet. Its status is ``complete``,
``measurement`` for a sampled run (the loader takes only a complete one) or
``refused``.
"""

from __future__ import annotations

import argparse
import hashlib
import math
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from protea.core.homology.build import (
    MMSEQS_FORMAT,
    HomologyRefused,
    _file_sha256,
    _read_record,
    _sequences_by_accession,
    _write_record,
    checked_photo,
)
from protea.core.row_alignment import lookup_by

#: What a cut's output folder holds, and what the loader verifies.
OUTPUTS: tuple[str, ...] = ("self-scores.parquet",)
MANIFEST = "self-scores.json"
#: The largest BLOSUM62 entry (11, W with W) in bits: the matrix is in half bits.
MAX_BITS_PER_RESIDUE = 5.5
SCORE_COLUMNS: tuple[str, ...] = ("query_md5", "fident", "alnlen", "evalue", "bits")
_M8_COLUMNS = ("query", "target", "fident", "alnlen", "qcov", "tcov", "evalue", "bits")
_M8_TYPES = {
    "query": pa.string(),
    "target": pa.string(),
    "fident": pa.float32(),
    "alnlen": pa.int32(),
    "qcov": pa.float32(),
    "tcov": pa.float32(),
    "evalue": pa.float64(),
    "bits": pa.float64(),
}
#: Sequences named in a record when they cannot be scored, at most.
_EXAMPLES = 20


def photo_queries(
    photo: Path, with_bank: bool = False
) -> tuple[dict[str, Any], dict[str, str], dict[str, str]]:
    """The checked photo, its queries by md5 as the table's prepare pass takes them, its bank.

    The bank (every member's unique sequence) is read only when asked for: a
    sampled run checks its queries against it.
    """
    source = checked_photo(photo)
    md5_of, sequence_of = _sequences_by_accession(photo)
    members = pq.read_table(photo / "members.parquet", columns=["accession", "superset"])
    flagged = list(
        zip(
            members.column("accession").to_pylist(),
            members.column("superset").to_pylist(),
            strict=True,
        )
    )
    lost = [a for a, s in flagged if s and a not in md5_of]
    if lost:
        raise HomologyRefused(f"{len(lost)} superset members have no sequence")
    queries = {md5_of[a]: sequence_of[md5_of[a]] for a, s in flagged if s}
    bank = (
        {md5_of[a]: sequence_of[md5_of[a]] for a, _ in flagged if a in md5_of} if with_bank else {}
    )
    return source, queries, bank


def fasta_bytes_sha256(sequences: dict[str, str]) -> str:
    """The sha256 of the FASTA the table's prepare pass writes for these sequences, sorted by md5."""
    digest = hashlib.sha256()
    for md5 in sorted(sequences):
        digest.update(f">{md5}\n{sequences[md5]}\n".encode("ascii"))
    return digest.hexdigest()


def _write_fasta(path: Path, sequences: dict[str, str]) -> int:
    with open(path, "w", encoding="ascii") as handle:
        for md5 in sorted(sequences):
            handle.write(f">{md5}\n{sequences[md5]}\n")
    return len(sequences)


def prepare_pass(photos: Sequence[Path], out: Path, sample: int | None = None) -> dict[str, Any]:
    """Write ``union.fasta`` (and, sampled, ``bank.fasta``) and ``prepare.json``."""
    out.mkdir(parents=True, exist_ok=True)
    cuts: list[dict[str, Any]] = []
    union: dict[str, str] = {}
    for index, photo in enumerate(photos):
        source, queries, bank = photo_queries(photo, with_bank=sample is not None and index == 0)
        cuts.append(
            {
                "photo": source,
                "queries": len(queries),
                "queries_fasta_sha256": fasta_bytes_sha256(queries),
                "query_md5s_sha256": hashlib.sha256(
                    "".join(f"{m}\n" for m in sorted(queries)).encode("ascii")
                ).hexdigest(),
            }
        )
        if sample is not None and index == 0:
            kept = sorted(queries)[:sample]
            union = {md5: queries[md5] for md5 in kept}
            _write_fasta(out / "bank.fasta", bank)
        elif sample is None:
            union.update(queries)
        (out / f"queries-{source['cut_release']}.txt").write_text(
            "".join(f"{m}\n" for m in sorted(queries)), encoding="ascii"
        )
    record = {
        "pass": "prepare",
        "cuts": cuts,
        "union": _write_fasta(out / "union.fasta", union),
        "sample": sample,
    }
    _write_record(out / "prepare.json", record)
    return record


def _run(command: list[str], log: Path) -> None:
    with open(log, "a", encoding="utf-8") as handle:
        subprocess.run(command, stdout=handle, stderr=subprocess.STDOUT, check=True)


def align_commands(mmseqs: Path, out: Path, threads: int) -> list[list[str]]:
    """The four MMseqs2 calls: the database, the pairs, the forced alignment, its table."""
    db, pairs, aligned = str(out / "union-db"), str(out / "pairs-db"), str(out / "self-db")
    return [
        [str(mmseqs), "createdb", str(out / "union.fasta"), db],
        [str(mmseqs), "tsv2db", str(out / "pairs.tsv"), pairs, "--output-dbtype", "7"],
        [
            str(mmseqs),
            "align",
            db,
            db,
            pairs,
            aligned,
            "-e",
            "inf",
            "--alignment-mode",
            "3",
            "--threads",
            str(threads),
        ],
        [
            str(mmseqs),
            "convertalis",
            db,
            db,
            aligned,
            str(out / "self.m8"),
            "--format-output",
            MMSEQS_FORMAT,
            "--threads",
            str(threads),
        ],
    ]


def _pairs_from_lookup(out: Path) -> int:
    """Each sequence's database key paired with itself, the pair database's input."""
    keys = [line.split("\t", 1)[0] for line in (out / "union-db.lookup").read_text().splitlines()]
    (out / "pairs.tsv").write_text("".join(f"{k}\t{k}\n" for k in keys), encoding="ascii")
    return len(keys)


def read_m8(path: Path) -> pa.Table:
    """An MMseqs2 tab file in the table's eight columns; an empty file is an empty table."""
    if not path.stat().st_size:
        return pa.table({name: pa.array([], _M8_TYPES[name]) for name in _M8_COLUMNS})
    read = pcsv.ReadOptions(column_names=list(_M8_COLUMNS))
    parse = pcsv.ParseOptions(delimiter="\t")
    convert = pcsv.ConvertOptions(column_types=_M8_TYPES)
    return pcsv.read_csv(path, read_options=read, parse_options=parse, convert_options=convert)


def usable(bits: float, alnlen: int) -> bool:
    """True for a bit score the alignment can hold: positive, finite, at most 5.5 per residue."""
    return math.isfinite(bits) and 0.0 < bits <= MAX_BITS_PER_RESIDUE * max(alnlen, 1)


def self_scores(m8: Path, asked: Sequence[str]) -> tuple[pa.Table, list[str]]:
    """The usable self rows, one per query md5 asked about, and the md5s left without one."""
    table = read_m8(m8)
    best: dict[str, tuple[float, int, float, float]] = {}
    for query, target, fident, alnlen, evalue, bits in zip(
        *(table[c].to_pylist() for c in ("query", "target", "fident", "alnlen", "evalue", "bits")),
        strict=True,
    ):
        if query == target and usable(float(bits), int(alnlen)):
            best[query] = (float(fident), int(alnlen), float(evalue), float(bits))
    scored = sorted(best)
    out = pa.table(
        {
            "query_md5": pa.array(scored, pa.string()),
            "fident": pa.array([best[m][0] for m in scored], pa.float32()),
            "alnlen": pa.array([best[m][1] for m in scored], pa.int32()),
            "evalue": pa.array([best[m][2] for m in scored], pa.float64()),
            "bits": pa.array([best[m][3] for m in scored], pa.float64()),
        }
    )
    return out, sorted(set(asked) - set(best))


def align_pass(out: Path, mmseqs: Path, expected_sha256: str, threads: int) -> dict[str, Any]:
    """Run the forced self alignment of the union and write ``self.parquet`` and ``align.json``."""
    binary_sha256 = _file_sha256(mmseqs)
    if binary_sha256 != expected_sha256:
        raise HomologyRefused(f"{mmseqs} hashes to {binary_sha256}, not {expected_sha256}")
    version = subprocess.run(
        [str(mmseqs), "version"], capture_output=True, text=True, check=True, timeout=60
    ).stdout.strip()
    commands = align_commands(mmseqs, out, threads)
    _run(commands[0], out / "mmseqs.log")
    keys = _pairs_from_lookup(out)
    for command in commands[1:]:
        _run(command, out / "mmseqs.log")
    asked = [
        line[1:].strip()
        for line in (out / "union.fasta").read_text().splitlines()
        if line[:1] == ">"
    ]
    table, unscored = self_scores(out / "self.m8", asked)
    pq.write_table(table, out / "self.parquet", compression="zstd")
    record = {
        "pass": "align",
        "mmseqs": {"version": version, "binary_sha256": binary_sha256},
        "command": [[Path(p).name if "/" in p else p for p in c] for c in commands],
        "threads": threads,
        "pairs": keys,
        "scored": table.num_rows,
        "unscored": len(unscored),
        "unscored_examples": unscored[:_EXAMPLES],
    }
    _write_record(out / "align.json", record)
    return record


def check_pass(out: Path, mmseqs: Path, tmp: Path, threads: int) -> dict[str, Any]:
    """In a sampled run: each sampled query's own hit in ``easy-search`` against the bank."""
    command = [
        str(mmseqs),
        "easy-search",
        str(out / "union.fasta"),
        str(out / "bank.fasta"),
        str(out / "check.m8"),
        str(tmp),
        "--threads",
        str(threads),
        "--format-output",
        MMSEQS_FORMAT,
    ]
    _run(command, out / "mmseqs.log")
    searched = read_m8(out / "check.m8")
    own = {
        q: float(b)
        for q, t, b in zip(
            searched["query"].to_pylist(),
            searched["target"].to_pylist(),
            searched["bits"].to_pylist(),
            strict=True,
        )
        if q == t
    }
    forced = pq.read_table(out / "self.parquet", columns=["query_md5", "bits"])
    pairs = zip(forced["query_md5"].to_pylist(), forced["bits"].to_pylist(), strict=True)
    aligned = {
        md5: bits
        for md5, (_, bits) in lookup_by(pairs, lambda row: row[0], context="self scores").items()
    }
    both = sorted(set(own) & set(aligned))
    differ = [m for m in both if own[m] != aligned[m]]
    record = {
        "pass": "check",
        "command": [Path(p).name if "/" in p else p for p in command],
        "sampled": len(aligned),
        "searched_with_own_hit": len(own),
        "compared": len(both),
        "equal": len(both) - len(differ),
        "differ": len(differ),
        "differ_examples": [(m, own[m], aligned[m]) for m in differ[:_EXAMPLES]],
        "forced_only": len(set(aligned) - set(own)),
    }
    _write_record(out / "check.json", record)
    return record


def manifest_pass(out: Path, revision: str) -> list[dict[str, Any]]:
    """Write each cut's ``self-scores.parquet`` and ``self-scores.json``."""
    prepare = _read_record(out / "prepare.json")
    align = _read_record(out / "align.json") if (out / "align.json").exists() else {}
    check = _read_record(out / "check.json") if (out / "check.json").exists() else None
    scores = pq.read_table(out / "self.parquet") if (out / "self.parquet").exists() else None
    manifests = []
    for cut in prepare["cuts"]:
        release = int(cut["photo"]["cut_release"])
        folder = out / str(release)
        folder.mkdir(parents=True, exist_ok=True)
        wanted = (out / f"queries-{release}.txt").read_text().split()
        status = "refused"
        outputs: dict[str, str] = {}
        without: list[str] = []
        if scores is not None and align:
            mine = scores.filter(pc.is_in(scores["query_md5"], value_set=pa.array(wanted)))
            pq.write_table(mine, folder / "self-scores.parquet", compression="zstd")
            outputs = {name: _file_sha256(folder / name) for name in OUTPUTS}
            without = sorted(set(wanted) - set(mine["query_md5"].to_pylist()))
            status = "measurement" if prepare.get("sample") is not None else "complete"
        manifest = {
            "table": "homology_self_scores",
            "cut_release": release,
            "photo": cut["photo"],
            "queries": cut["queries"],
            "queries_fasta_sha256": cut["queries_fasta_sha256"],
            "query_md5s_sha256": cut["query_md5s_sha256"],
            "queries_without_self_score": len(without) if status == "complete" else None,
            "without_self_score_examples": without[:_EXAMPLES],
            "mmseqs": align.get("mmseqs"),
            "command": align.get("command"),
            "outputs": outputs,
            "counts": {
                "prepare": {k: v for k, v in prepare.items() if k != "cuts"},
                "align": align,
            },
            "check": check,
            "status": status,
            "revision": revision,
        }
        _write_record(folder / MANIFEST, manifest)
        manifests.append(manifest)
    return manifests


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m protea.core.homology.self_scores")
    sub = parser.add_subparsers(dest="pass_name", required=True)
    p_prep = sub.add_parser("prepare")
    p_prep.add_argument("--photo", type=Path, action="append", required=True)
    p_prep.add_argument("--out", type=Path, required=True)
    p_prep.add_argument("--sample", type=int, default=None)
    for name in ("align", "check"):
        p_run = sub.add_parser(name)
        p_run.add_argument("--out", type=Path, required=True)
        p_run.add_argument("--mmseqs", type=Path, required=True)
        p_run.add_argument("--threads", type=int, required=True)
        if name == "align":
            p_run.add_argument("--mmseqs-sha256", required=True)
        else:
            p_run.add_argument("--tmp", type=Path, required=True)
    p_man = sub.add_parser("manifest")
    p_man.add_argument("--out", type=Path, required=True)
    p_man.add_argument("--revision", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.pass_name == "prepare":
            prepare_pass(args.photo, args.out, args.sample)
        elif args.pass_name == "align":
            align_pass(args.out, args.mmseqs, args.mmseqs_sha256, args.threads)
        elif args.pass_name == "check":
            record = check_pass(args.out, args.mmseqs, args.tmp, args.threads)
            return 0 if record["differ"] == 0 else 4
        else:
            manifests = manifest_pass(args.out, args.revision)
            return 0 if all(m["status"] in ("complete", "measurement") for m in manifests) else 3
    except HomologyRefused as refused:
        print(f"REFUSED: {refused}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
