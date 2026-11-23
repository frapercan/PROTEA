"""The segment families of a cut, built away from the server (ADR-D65, decision 2).

Passes over files already on the cluster, with no database and no label read:

``prepare`` checks the photo (complete, full, up to the mark, unchanged) and the
lexicon bank (its manifest complete and every cut shard it reads hashing as
recorded), and names the two populations by membership alone: the donors are
the sequence md5s of the photo's ``truth`` members, the targets those of its
superset members that are not donors. It streams the bank's population shard by
shard and, for every segment of a donor or a target, writes its core (the
segment's residues minus every non-match run longer than its own match count) to
``donors.fasta`` or ``targets.fasta`` under its index, and the segment to
``segments.parquet`` (md5, start, end, theme, core length, donor, index).

``search`` and ``families`` run MMseqs2 (``protea.core.segment_families.searches``).

``manifest`` writes ``segment-families.json``: the photo, the lexicon bank, the
MMseqs2 binary and commands, the counts and the sha256 of every output.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from protea.core.file_stage import file_sha256, read_record, write_record
from protea.core.homology.build import HomologyRefused, checked_photo
from protea.core.segment_families.algorithms import core_mask

#: What the families of one scope are written as.
SCOPES = ("strict", "permissive")
SEGMENT_SCHEMA = pa.schema(
    [
        ("md5", pa.string()),
        ("start", pa.int32()),
        ("end", pa.int32()),
        ("theme", pa.int32()),
        ("core_length", pa.int32()),
        ("donor", pa.bool_()),
        ("index", pa.int64()),
    ]
)


class FamiliesRefused(RuntimeError):
    """The inputs cannot make families; nothing is written past the refusal."""


def member_md5s(photo: Path) -> tuple[set[str], set[str], dict[str, int]]:
    """(donor md5s, target md5s, counts): truth members, and superset members that are not donors."""
    md5_of: dict[str, str] = {}
    for name in ("sequences.parquet", "unreviewed_sequences.parquet"):
        table = pq.read_table(photo / name, columns=["accession", "md5"])
        md5_of.update(
            zip(table.column("accession").to_pylist(), table.column("md5").to_pylist(), strict=True)
        )
    members = pq.read_table(photo / "members.parquet", columns=["accession", "superset", "truth"])
    rows = zip(
        *(members.column(c).to_pylist() for c in ("accession", "superset", "truth")), strict=True
    )
    donors, superset, missing = set(), set(), 0
    for accession, in_superset, in_truth in rows:
        md5 = md5_of.get(accession)
        if md5 is None:
            missing += in_superset or in_truth
            continue
        if in_truth:
            donors.add(md5)
        if in_superset:
            superset.add(md5)
    counts = {
        "donor_sequences": len(donors),
        "superset_sequences": len(superset),
        "missing": missing,
    }
    return donors, superset - donors, counts


def checked_lexicon_bank(bank: Path, name: str) -> dict[str, Any]:
    """The lexicon bank's manifest, refused unless it is complete and is this lexicon's."""
    path = bank / "lexicon" / name / "lexicon-bank.json"
    if not path.is_file():
        raise FamiliesRefused(f"{path} does not exist: the lexicon bank must be complete first")
    manifest = read_record(path)
    if manifest.get("status") != "complete" or manifest.get("name") != name:
        raise FamiliesRefused(f"the lexicon bank {name} is not complete")
    manifest["manifest_sha256"] = file_sha256(path)
    return manifest


def _shard(bank: Path, name: str, shard: dict[str, Any]) -> tuple[pa.Table, pa.Table]:
    """One shard's cut and its sequences, the cut checked against the bank's record."""
    k = int(shard["shard"])
    path = bank / "lexicon" / name / "cut" / f"shard-{k:05d}.parquet"
    if file_sha256(path) != shard["sha256"]:
        raise FamiliesRefused(f"cut shard {k} does not hash as the lexicon bank recorded")
    cut = pq.read_table(path, columns=["md5", "seg_start", "seg_end", "seg_theme", "columns"])
    sequences = pq.ParquetFile(bank / "sequences.parquet").read_row_group(
        k, columns=["md5", "sequence"]
    )
    if cut.column("md5").to_pylist() != sequences.column("md5").to_pylist():
        raise FamiliesRefused(f"cut shard {k} is not the population's shard {k}")
    return cut, sequences


class _Writer:
    """The two FASTA files and the segment rows, filled shard by shard."""

    def __init__(self, out: Path) -> None:
        self.files = {
            True: open(out / "donors.fasta", "w"),
            False: open(out / "targets.fasta", "w"),
        }
        self.next = {True: 0, False: 0}
        self.rows: dict[str, list[Any]] = {f.name: [] for f in SEGMENT_SCHEMA}

    def add(self, md5: str, segment: tuple[int, int, int], core: str, donor: bool) -> None:
        index = self.next[donor]
        self.files[donor].write(f">{index}\n{core}\n")
        self.next[donor] = index + 1
        for field, value in zip(
            SEGMENT_SCHEMA.names, (md5, *segment, len(core), donor, index), strict=True
        ):
            self.rows[field].append(value)

    def close(self, out: Path) -> None:
        for handle in self.files.values():
            handle.close()
        pq.write_table(
            pa.table(self.rows, schema=SEGMENT_SCHEMA), out / "segments.parquet", compression="zstd"
        )


def _cores(
    writer: _Writer, cut: pa.Table, sequences: pa.Table, populations: tuple[set[str], set[str]]
) -> None:
    donors, targets = populations
    columns = cut.to_pydict()
    for i, (md5, seq) in enumerate(
        zip(columns["md5"], sequences.column("sequence").to_pylist(), strict=True)
    ):
        donor = md5 in donors
        if not donor and md5 not in targets:
            continue
        residue_columns = np.asarray(columns["columns"][i])
        for start, end, theme in zip(
            columns["seg_start"][i], columns["seg_end"][i], columns["seg_theme"][i], strict=True
        ):
            keep = core_mask(residue_columns[start:end])
            core = "".join(a for a, k in zip(seq[start:end], keep, strict=True) if k)
            writer.add(md5, (start, end, theme), core, donor)


def prepare_pass(photo: Path, bank: Path, name: str, out: Path) -> dict[str, Any]:
    """Write the cores of the donors' and the targets' segments and ``prepare.json``."""
    try:
        source = checked_photo(photo)
    except HomologyRefused as refused:
        raise FamiliesRefused(str(refused)) from refused
    lexicon = checked_lexicon_bank(bank, name)
    donors, targets, counts = member_md5s(photo)
    out.mkdir(parents=True, exist_ok=True)
    writer = _Writer(out)
    seen: set[str] = set()
    for shard in lexicon["passes"]["cut"]["shards"]:
        cut, sequences = _shard(bank, name, shard)
        seen.update(cut.column("md5").to_pylist())
        _cores(writer, cut, sequences, (donors, targets))
    writer.close(out)
    absent = (donors | targets) - seen
    if absent:
        raise FamiliesRefused(f"{len(absent)} member sequences are not in the lexicon bank")
    record = {
        "pass": "prepare",
        "photo": source,
        "lexicon": {
            "name": name,
            "manifest_sha256": lexicon["manifest_sha256"],
            "sha256": lexicon["lexicon_sha256"],
        },
        **counts,
        "target_sequences": len(targets),
        "donor_cores": writer.next[True],
        "target_cores": writer.next[False],
        "outputs": {
            f: file_sha256(out / f) for f in ("donors.fasta", "targets.fasta", "segments.parquet")
        },
    }
    write_record(out / "prepare.json", record)
    return record


def manifest_pass(out: Path, revision: str) -> dict[str, Any]:
    """Write ``segment-families.json`` from the pass records and the sha256 of every output."""
    prepare = read_record(out / "prepare.json")
    search = read_record(out / "search.json") if (out / "search.json").exists() else {}
    scopes = {
        s: read_record(out / f"families-{s}.json")
        for s in SCOPES
        if (out / f"families-{s}.json").exists()
    }
    outputs = ["segments.parquet", *(f"families-{s}.parquet" for s in scopes)]
    missing = [o for o in outputs if not (out / o).exists()]
    status = "complete" if search and len(scopes) == len(SCOPES) and not missing else "refused"
    manifest = {
        "table": "segment_families",
        "cut_release": prepare["photo"]["cut_release"],
        "photo": prepare["photo"],
        "lexicon": prepare["lexicon"],
        "mmseqs": search.get("mmseqs"),
        "commands": search.get("commands"),
        "counts": {"prepare": prepare, "search": search, "families": scopes},
        "outputs": {o: file_sha256(out / o) for o in outputs if o not in missing},
        "status": status,
        "refusals": {
            "missing_outputs": missing,
            "search_ran": bool(search),
            "scopes": sorted(scopes),
        },
        "revision": revision,
    }
    write_record(out / "segment-families.json", manifest)
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m protea.core.segment_families.build")
    sub = parser.add_subparsers(dest="pass_name", required=True)
    p_prep = sub.add_parser("prepare")
    p_prep.add_argument("--photo", type=Path, required=True)
    p_prep.add_argument("--bank", type=Path, required=True)
    p_prep.add_argument("--name", required=True)
    p_prep.add_argument("--out", type=Path, required=True)
    for pass_name in ("search", "families"):
        p = sub.add_parser(pass_name)
        p.add_argument("--out", type=Path, required=True)
        p.add_argument("--mmseqs", type=Path, required=True)
        p.add_argument("--mmseqs-sha256", required=True)
        p.add_argument("--threads", type=int, required=True)
        p.add_argument("--tmp", type=Path, required=True)
        if pass_name == "families":
            p.add_argument("--scope", choices=SCOPES, required=True)
    p_man = sub.add_parser("manifest")
    p_man.add_argument("--out", type=Path, required=True)
    p_man.add_argument("--revision", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    from protea.core.segment_families import searches

    args = _parser().parse_args(argv)
    try:
        if args.pass_name == "prepare":
            prepare_pass(args.photo, args.bank, args.name, args.out)
        elif args.pass_name in ("search", "families"):
            tool = searches.Tool(args.mmseqs, args.mmseqs_sha256, args.threads, args.tmp)
            if args.pass_name == "search":
                searches.search_pass(args.out, tool)
            else:
                searches.families_pass(args.out, tool, args.scope)
        else:
            return 0 if manifest_pass(args.out, args.revision)["status"] == "complete" else 3
    except FamiliesRefused as refused:
        print(f"REFUSED: {refused}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
