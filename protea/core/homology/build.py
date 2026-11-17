"""The homology table of a cut, built away from the server (ADR-D61).

Three passes over a photo that is already on the cluster (ADR-D59), with no
database and nothing sent:

``prepare`` reads the photo's members and its two sequence tables, checks each
file against the photo's manifest, and writes two FASTA files keyed by sequence
md5: the queries, which are the superset's unique sequences, and the bank, which
is every member's unique sequence (neighbours included). A member with no
sequence in the photo is counted and named, never guessed. ``--sample N`` keeps
the N first query md5s in sorted order. The md5 is uniform, so that is a random
sample any rerun repeats, and it serves the measured first job.

``search`` checks the MMseqs2 binary against its sha256 and runs ``easy-search``
with the tool's defaults. Threads and the memory split are resources, not
results. It writes the hits as a parquet: ``query_md5``, ``target_md5``,
``fident``, ``alnlen``, ``qcov``, ``tcov``, ``evalue`` and ``bits``. The rows
are sorted by query and then by bit score, descending. The hit of a sequence
with itself is dropped and counted. No threshold of ours is applied: identity
bands, coverage cuts and e-value cuts belong to the readers.

``manifest`` writes ``homology.json``. It names the photo it read (cut and
manifest sha256), MMseqs2 (version, binary sha256, command line), the counts
and the sha256 of every output. Its status is ``complete``, ``measurement``
for a sampled run (the loader takes only a complete table) or ``refused``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from protea.core.marks import MARK_RELEASE

#: The photo outputs the table reads, each checked against the photo's manifest.
PHOTO_INPUTS: tuple[str, ...] = (
    "members.parquet",
    "sequences.parquet",
    "unreviewed_sequences.parquet",
)
#: What this builder writes and the loader verifies.
OUTPUTS: tuple[str, ...] = ("hits.parquet",)
#: MMseqs2's output columns, in this order, and the names the table gives them.
MMSEQS_FORMAT = "query,target,fident,alnlen,qcov,tcov,evalue,bits"
HIT_COLUMNS: tuple[str, ...] = (
    "query_md5",
    "target_md5",
    "fident",
    "alnlen",
    "qcov",
    "tcov",
    "evalue",
    "bits",
)
_HIT_TYPES = {
    "query_md5": pa.string(),
    "target_md5": pa.string(),
    "fident": pa.float32(),
    "alnlen": pa.int32(),
    "qcov": pa.float32(),
    "tcov": pa.float32(),
    "evalue": pa.float64(),
    "bits": pa.float32(),
}
#: Members named in a record when their sequence is missing, at most.
_EXAMPLES = 20


class HomologyRefused(RuntimeError):
    """The inputs cannot make a table; nothing is written past the refusal."""


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 22), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_record(path: Path, record: dict[str, Any]) -> None:
    path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_record(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


# Prepare -------------------------------------------------------------------------


def checked_photo(photo: Path) -> dict[str, Any]:
    """The photo's manifest, refused unless it is a complete full photo up to the mark.

    Every input this builder reads must still hash to what the manifest recorded:
    a table built from other bytes than the photo the platform loaded would be a
    table about another photo.
    """
    manifest_path = photo / "photo.json"
    manifest = _read_record(manifest_path)
    cut = int(manifest.get("cut_release", 0))
    if cut > MARK_RELEASE:
        raise HomologyRefused(f"cut {cut} is after the mark {MARK_RELEASE}")
    if manifest.get("status") != "complete":
        raise HomologyRefused(f"the photo of cut {cut} is {manifest.get('status')!r}")
    if manifest.get("kind", "full") != "full":
        raise HomologyRefused(
            f"the photo of cut {cut} is {manifest.get('kind')!r}: a cut that only closes "
            "a window has no members to search against (ADR-D61, decision 1)"
        )
    outputs = manifest.get("outputs", {})
    for name in PHOTO_INPUTS:
        if _file_sha256(photo / name) != outputs.get(name):
            raise HomologyRefused(f"{name} no longer hashes to what the photo recorded")
    return {
        "cut_release": cut,
        "manifest_sha256": _file_sha256(manifest_path),
        "revision": manifest.get("revision"),
    }


def _sequences_by_accession(photo: Path) -> tuple[dict[str, str], dict[str, str]]:
    """``accession -> md5`` and ``md5 -> sequence`` over the photo's two sequence tables.

    Every md5 is recomputed from its sequence; a pair that disagrees refuses.
    """
    md5_of: dict[str, str] = {}
    sequence_of: dict[str, str] = {}
    for name in ("sequences.parquet", "unreviewed_sequences.parquet"):
        table = pq.read_table(photo / name, columns=["accession", "md5", "sequence"])
        for accession, md5, sequence in zip(
            table.column("accession").to_pylist(),
            table.column("md5").to_pylist(),
            table.column("sequence").to_pylist(),
            strict=True,
        ):
            if hashlib.md5(sequence.encode(), usedforsecurity=False).hexdigest() != md5:
                raise HomologyRefused(f"the sequence of {accession} does not hash to {md5}")
            md5_of[accession] = md5
            sequence_of[md5] = sequence
    return md5_of, sequence_of


def _write_fasta(path: Path, md5s: Iterable[str], sequence_of: dict[str, str]) -> int:
    count = 0
    with open(path, "w", encoding="ascii") as handle:
        for md5 in md5s:
            handle.write(f">{md5}\n{sequence_of[md5]}\n")
            count += 1
    return count


def prepare_pass(photo: Path, out: Path, sample: int | None = None) -> dict[str, Any]:
    """Write ``queries.fasta`` and ``bank.fasta`` and their record, ``prepare.json``."""
    source = checked_photo(photo)
    md5_of, sequence_of = _sequences_by_accession(photo)
    members = pq.read_table(photo / "members.parquet", columns=["accession", "superset"])
    accessions = members.column("accession").to_pylist()
    superset = members.column("superset").to_pylist()
    no_sequence = sorted(a for a in accessions if a not in md5_of)
    lost_superset = [a for a, s in zip(accessions, superset, strict=True) if s and a not in md5_of]
    if lost_superset:
        raise HomologyRefused(f"{len(lost_superset)} superset members have no sequence")
    queries = sorted({md5_of[a] for a, s in zip(accessions, superset, strict=True) if s})
    if sample is not None:
        queries = queries[:sample]
    bank = sorted({md5_of[a] for a in accessions if a in md5_of})
    out.mkdir(parents=True, exist_ok=True)
    record = {
        "pass": "prepare",
        "photo": source,
        "members": len(accessions),
        "superset_members": sum(bool(s) for s in superset),
        "members_without_sequence": len(no_sequence),
        "members_without_sequence_examples": no_sequence[:_EXAMPLES],
        "queries": _write_fasta(out / "queries.fasta", queries, sequence_of),
        "bank": _write_fasta(out / "bank.fasta", bank, sequence_of),
        "sample": sample,
    }
    record["queries_fasta_sha256"] = _file_sha256(out / "queries.fasta")
    record["bank_fasta_sha256"] = _file_sha256(out / "bank.fasta")
    _write_record(out / "prepare.json", record)
    return record


# Search --------------------------------------------------------------------------


def mmseqs_command(
    mmseqs: Path, out: Path, tmp: Path, threads: int, split_memory_limit: str | None
) -> list[str]:
    """``easy-search`` with the tool's defaults; only resources and the columns are set."""
    command = [
        str(mmseqs),
        "easy-search",
        str(out / "queries.fasta"),
        str(out / "bank.fasta"),
        str(out / "hits.m8"),
        str(tmp),
        "--threads",
        str(threads),
        "--format-output",
        MMSEQS_FORMAT,
    ]
    if split_memory_limit:
        command += ["--split-memory-limit", split_memory_limit]
    return command


def hits_table(m8: Path) -> tuple[pa.Table, int]:
    """The hits read from MMseqs2's tab file, self hits dropped, sorted; and how many dropped."""
    read = pcsv.ReadOptions(column_names=list(HIT_COLUMNS))
    parse = pcsv.ParseOptions(delimiter="\t")
    convert = pcsv.ConvertOptions(column_types=_HIT_TYPES)
    table = pcsv.read_csv(m8, read_options=read, parse_options=parse, convert_options=convert)
    own = pc.equal(table.column("query_md5"), table.column("target_md5"))
    kept = table.filter(pc.invert(own))
    order = pc.sort_indices(
        kept,
        sort_keys=[("query_md5", "ascending"), ("bits", "descending"), ("target_md5", "ascending")],
    )
    return kept.take(order), table.num_rows - kept.num_rows


def search_pass(
    out: Path, mmseqs: Path, expected_sha256: str, run: dict[str, Any]
) -> dict[str, Any]:
    """Run MMseqs2 on the prepared FASTA files and write ``hits.parquet`` and ``search.json``.

    ``run`` holds the resources: ``threads``, ``split_memory_limit`` and ``tmp``.
    """
    binary_sha256 = _file_sha256(mmseqs)
    if binary_sha256 != expected_sha256:
        raise HomologyRefused(f"{mmseqs} hashes to {binary_sha256}, not {expected_sha256}")
    version = subprocess.run(
        [str(mmseqs), "version"], capture_output=True, text=True, check=True, timeout=60
    ).stdout.strip()
    command = mmseqs_command(
        mmseqs, out, Path(run["tmp"]), int(run["threads"]), run.get("split_memory_limit")
    )
    started = time.monotonic()
    with open(out / "mmseqs.log", "w", encoding="utf-8") as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
    seconds = round(time.monotonic() - started)
    hits, self_hits = hits_table(out / "hits.m8")
    pq.write_table(hits, out / "hits.parquet", compression="zstd")
    (out / "hits.m8").unlink()
    queries = int(_read_record(out / "prepare.json")["queries"])
    with_hits = pc.count_distinct(hits.column("query_md5")).as_py() if hits.num_rows else 0
    record = {
        "pass": "search",
        "mmseqs": {"version": version, "binary_sha256": binary_sha256},
        "command": [Path(part).name if "/" in part else part for part in command],
        "threads": int(run["threads"]),
        "split_memory_limit": run.get("split_memory_limit"),
        "seconds": seconds,
        "hits": hits.num_rows,
        "self_hits_dropped": self_hits,
        "queries_with_hits": with_hits,
        "queries_without_hits": queries - with_hits,
    }
    _write_record(out / "search.json", record)
    return record


# Manifest ------------------------------------------------------------------------


def manifest_pass(out: Path, revision: str) -> dict[str, Any]:
    """Write ``homology.json`` from the two pass records and the sha256 of every output."""
    prepare = _read_record(out / "prepare.json")
    search = _read_record(out / "search.json") if (out / "search.json").exists() else {}
    missing = [name for name in OUTPUTS if not (out / name).exists()]
    if missing or not search:
        status = "refused"
    elif prepare.get("sample") is not None:
        status = "measurement"
    else:
        status = "complete"
    manifest = {
        "table": "homology",
        "cut_release": prepare["photo"]["cut_release"],
        "photo": prepare["photo"],
        "mmseqs": search.get("mmseqs"),
        "command": search.get("command"),
        "outputs": {name: _file_sha256(out / name) for name in OUTPUTS if name not in missing},
        "counts": {"prepare": prepare, "search": search},
        "status": status,
        "refusals": {"missing_outputs": missing, "search_ran": bool(search)},
        "revision": revision,
    }
    _write_record(out / "homology.json", manifest)
    return manifest


# Command line --------------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m protea.core.homology.build")
    sub = parser.add_subparsers(dest="pass_name", required=True)
    p_prep = sub.add_parser("prepare")
    p_prep.add_argument("--photo", type=Path, required=True)
    p_prep.add_argument("--out", type=Path, required=True)
    p_prep.add_argument("--sample", type=int, default=None)
    p_search = sub.add_parser("search")
    p_search.add_argument("--out", type=Path, required=True)
    p_search.add_argument("--mmseqs", type=Path, required=True)
    p_search.add_argument("--mmseqs-sha256", required=True)
    p_search.add_argument("--threads", type=int, required=True)
    p_search.add_argument("--split-memory-limit", default=None)
    p_search.add_argument("--tmp", type=Path, required=True)
    p_man = sub.add_parser("manifest")
    p_man.add_argument("--out", type=Path, required=True)
    p_man.add_argument("--revision", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.pass_name == "prepare":
            prepare_pass(args.photo, args.out, args.sample)
        elif args.pass_name == "search":
            run = {
                "threads": args.threads,
                "split_memory_limit": args.split_memory_limit,
                "tmp": args.tmp,
            }
            search_pass(args.out, args.mmseqs, args.mmseqs_sha256, run)
        else:
            manifest = manifest_pass(args.out, args.revision)
            return 0 if manifest["status"] in ("complete", "measurement") else 3
    except HomologyRefused as refused:
        print(f"REFUSED: {refused}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
