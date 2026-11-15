"""Step 1 of ADR-D59: measure a cut before building its photo.

Three passes. Each is a pure function of its inputs and writes its own record,
so a pass can run where its source is (a compute node streaming a 190 GB
archive) and be re-run alone:

``superset``
    The paired release's Swiss-Prot, read from UniProt's ``uniprot_sprot-only``
    archive with the parser ``load_swissprot_release`` uses, written as the
    canonical table that operation verifies. Its sha256 is the claim the server
    checks when it loads the same release.
``neighbours``
    The UniRef XML on stdin, pre-filtered with
    :data:`~protea.core.cut_photo.formats.UNIREF_LINE_FILTER`: every member of
    every cluster that holds a superset protein.
``gaf``
    The GOA release on stdin, decompressed: the rows of the superset and of the
    neighbours, counted by (set, code, reference, assigned_by, negation), plus
    the keys each protein carries. The channel table is written FROM this
    inventory; nothing here assigns a channel.

Every pass refuses a cut after the mark and a source published after its cut.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import sys
import tarfile
from collections import Counter
from collections.abc import Iterable, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from protea.core.cut_photo.formats import (
    AUTOMATIC_CODE,
    CURATED_INFERENCE_CODES,
    GAF_ASSIGNED_BY,
    GAF_EVIDENCE,
    GAF_QUALIFIER,
    GAF_REFERENCE,
    NO_DATA_CODE,
    PROPAGATED_CODES,
    TRUTH_CODES,
    is_negated,
    iter_uniref_clusters,
    reference_class,
)
from protea.core.swissprot_table import canonical_table, parse_swissprot_fasta

#: The last GOA release a photo may be built from: the 227 instance. Every later
#: release holds the truth of the test, which no photo reads.
MARK_RELEASE = 227

#: The member of UniProt's ``uniprot_sprot-only`` archive that carries the
#: canonical sequences; the same one ``load_swissprot_release`` reads, pinned
#: by a test.
SWISSPROT_FASTA_MEMBER = "uniprot_sprot.fasta.gz"

_GAF_PREFIX = "UniProtKB\t"

#: One bit per code family, to summarise what a protein carries.
FAMILY_BITS: dict[str, int] = {
    "truth": 1,
    "curated": 2,
    "propagated": 4,
    "automatic": 8,
    "no_data": 16,
    "unknown": 32,
}


class PhotoGuardError(ValueError):
    """A source or a cut the photo is not allowed to read."""


def check_dates(*, source: str, published: date, cut_release: int, cut_date: date) -> None:
    """Refuse a cut after the mark and a source published after its cut."""
    if cut_release > MARK_RELEASE:
        raise PhotoGuardError(f"cut {cut_release} is after the mark {MARK_RELEASE}")
    if published > cut_date:
        raise PhotoGuardError(
            f"{source} was published on {published.isoformat()}, "
            f"after the cut ({cut_date.isoformat()})"
        )


def code_family(code: str) -> str:
    """The ADR-D49 family of an evidence code; an unlisted code is ``unknown``."""
    if code in TRUTH_CODES:
        return "truth"
    if code in CURATED_INFERENCE_CODES:
        return "curated"
    if code in PROPAGATED_CODES:
        return "propagated"
    if code == AUTOMATIC_CODE:
        return "automatic"
    if code == NO_DATA_CODE:
        return "no_data"
    return "unknown"


def pattern_of(mask: int) -> str:
    """What a protein's code families say about how a corpus can see it.

    The order is the order of the tiers: a protein with a truth row is in the
    truth tier whatever else it carries, and ``only_electronic_or_phylogenetic``
    is the population ADR-D49's two live tiers cannot see.
    """
    if mask & FAMILY_BITS["truth"]:
        return "truth"
    if mask & FAMILY_BITS["curated"]:
        return "curated"
    if mask & (FAMILY_BITS["propagated"] | FAMILY_BITS["automatic"]):
        return "only_electronic_or_phylogenetic"
    if mask & FAMILY_BITS["no_data"]:
        return "only_no_data"
    return "only_unknown"


def read_superset(path: Path) -> frozenset[str]:
    """The accessions of a canonical Swiss-Prot table."""
    with open(path, encoding="utf-8") as handle:
        return frozenset(line.split("\t", 1)[0] for line in handle if line.strip())


def read_neighbours(path: Path) -> frozenset[str]:
    """The neighbour accessions of a ``neighbours.parquet`` that are not superset members."""
    table = pq.read_table(path, columns=["accession", "in_superset"])
    keep = table.filter(pc.invert(table.column("in_superset")))
    return frozenset(keep.column("accession").to_pylist())


def superset_pass(
    archive: Path,
    *,
    release: str,
    published: date,
    cut_release: int,
    cut_date: date,
    out: Path,
) -> dict[str, Any]:
    """Write ``superset.tsv`` and ``superset.json`` for one archived release."""
    check_dates(
        source=f"UniProt {release}", published=published, cut_release=cut_release, cut_date=cut_date
    )
    with tarfile.open(archive, "r:gz") as tar:
        handle = tar.extractfile(SWISSPROT_FASTA_MEMBER)
        if handle is None:
            raise ValueError(f"{archive} has no {SWISSPROT_FASTA_MEMBER}")
        with gzip.GzipFile(fileobj=handle) as fasta:
            members = parse_swissprot_fasta(fasta)
    blob = canonical_table(members)
    out.mkdir(parents=True, exist_ok=True)
    (out / "superset.tsv").write_bytes(blob)
    record = {
        "pass": "superset",
        "release": release,
        "published": published.isoformat(),
        "cut_release": cut_release,
        "cut_date": cut_date.isoformat(),
        "accessions": len(members),
        "sha256": hashlib.sha256(blob).hexdigest(),
        "archive": {
            "path": str(archive),
            "bytes": archive.stat().st_size,
            "sha256": _file_sha256(archive),
        },
    }
    _write_record(out / "superset.json", record)
    return record


def neighbours_pass(
    lines: Iterable[str],
    *,
    superset: frozenset[str],
    release: str,
    published: date,
    cut_release: int,
    cut_date: date,
    out: Path,
    batch_rows: int = 1_000_000,
) -> dict[str, Any]:
    """Write ``neighbours.parquet`` and ``neighbours.json`` from a filtered UniRef stream."""
    check_dates(
        source=f"UniRef {release}", published=published, cut_release=cut_release, cut_date=cut_date
    )
    out.mkdir(parents=True, exist_ok=True)
    schema = pa.schema(
        [("cluster", pa.string()), ("accession", pa.string()), ("in_superset", pa.bool_())]
    )
    clusters = clusters_with = members_with = 0
    seen: set[str] = set()
    buf_cluster: list[str] = []
    buf_accession: list[str] = []
    buf_flag: list[bool] = []
    with pq.ParquetWriter(out / "neighbours.parquet", schema, compression="zstd") as writer:

        def flush() -> None:
            if buf_accession:
                writer.write_table(pa.table([buf_cluster, buf_accession, buf_flag], schema=schema))
                buf_cluster.clear()
                buf_accession.clear()
                buf_flag.clear()

        for cluster, accessions in iter_uniref_clusters(lines):
            clusters += 1
            hits = [acc for acc in accessions if acc in superset]
            if not hits:
                continue
            clusters_with += 1
            members_with += len(accessions)
            seen.update(hits)
            for acc in accessions:
                buf_cluster.append(cluster)
                buf_accession.append(acc)
                buf_flag.append(acc in superset)
            if len(buf_accession) >= batch_rows:
                flush()
        flush()
    record = {
        "pass": "neighbours",
        "release": release,
        "published": published.isoformat(),
        "cut_release": cut_release,
        "cut_date": cut_date.isoformat(),
        "clusters": clusters,
        "clusters_with_superset": clusters_with,
        "members_in_those_clusters": members_with,
        "neighbours_not_in_superset": members_with - len(seen),
        "superset_in_a_cluster": len(seen),
        "superset_in_no_cluster": len(superset) - len(seen),
    }
    _write_record(out / "neighbours.json", record)
    return record


def gaf_pass(
    lines: Iterable[str],
    *,
    superset: frozenset[str],
    neighbours: frozenset[str],
    release: int,
    published: date,
    cut_release: int,
    cut_date: date,
    out: Path,
    batch_rows: int = 2_000_000,
) -> dict[str, Any]:
    """Count the GOA rows of the superset and the neighbours, and the keys of each protein.

    Writes ``groups.tsv`` (rows per set, code, reference, assigned_by and
    negation), ``keys.tsv`` and ``protein_keys.parquet`` (the keys each protein
    carries, with their row counts) and ``gaf.json``.

    Both per-protein summaries are exact whatever the order of the file. GOA is
    not strictly ordered by accession (GOA 211: 276,053 places where the order
    goes back, reported as ``order_breaks``), so a run of rows is not a protein:
    ``runs_by_set`` counts runs, ``proteins_by_set`` counts proteins, and
    ``protein_keys.parquet`` holds one row per (run, key), to be summed by
    (accession, key).
    """
    check_dates(
        source=f"GOA {release}", published=published, cut_release=cut_release, cut_date=cut_date
    )
    out.mkdir(parents=True, exist_ok=True)
    groups: Counter[tuple[str, str, str, str, bool]] = Counter()
    key_ids: dict[tuple[str, str, str, bool], int] = {}
    outside_codes: Counter[str] = Counter()
    unknown_codes: Counter[str] = Counter()
    superset_masks: dict[str, int] = {}
    neighbour_masks: dict[str, int] = {}
    counters = Counter(lines=0, uniprot_rows=0, malformed=0, order_breaks=0)
    runs_by_set: Counter[str] = Counter()
    schema = pa.schema([("accession", pa.string()), ("key_id", pa.int32()), ("rows", pa.int64())])
    buf_acc: list[str] = []
    buf_key: list[int] = []
    buf_rows: list[int] = []
    current: str | None = None
    current_set = ""
    current_keys: Counter[int] = Counter()
    current_mask = 0

    with pq.ParquetWriter(out / "protein_keys.parquet", schema, compression="zstd") as writer:

        def flush_buffers() -> None:
            if buf_acc:
                writer.write_table(pa.table([buf_acc, buf_key, buf_rows], schema=schema))
                buf_acc.clear()
                buf_key.clear()
                buf_rows.clear()

        def close_run() -> None:
            if current is None:
                return
            for kid, n in current_keys.items():
                buf_acc.append(current)
                buf_key.append(kid)
                buf_rows.append(n)
            runs_by_set[current_set] += 1
            masks = superset_masks if current_set == "superset" else neighbour_masks
            masks[current] = masks.get(current, 0) | current_mask
            if len(buf_acc) >= batch_rows:
                flush_buffers()

        for line in lines:
            counters["lines"] += 1
            if not line.startswith(_GAF_PREFIX):
                continue
            counters["uniprot_rows"] += 1
            accession = line[len(_GAF_PREFIX) : line.index("\t", len(_GAF_PREFIX))]
            if accession in superset:
                member = "superset"
            elif accession in neighbours:
                member = "neighbour"
            else:
                head = line.split("\t", GAF_EVIDENCE + 1)
                if len(head) > GAF_EVIDENCE:
                    outside_codes[head[GAF_EVIDENCE]] += 1
                continue
            cols = line.rstrip("\n").split("\t")
            if len(cols) <= GAF_ASSIGNED_BY:
                counters["malformed"] += 1
                continue
            code = cols[GAF_EVIDENCE]
            family = code_family(code)
            if family == "unknown":
                unknown_codes[code] += 1
            ref = reference_class(cols[GAF_REFERENCE])
            by = cols[GAF_ASSIGNED_BY]
            negated = is_negated(cols[GAF_QUALIFIER])
            groups[(member, code, ref, by, negated)] += 1
            key = (code, ref, by, negated)
            kid = key_ids.get(key)
            if kid is None:
                kid = key_ids[key] = len(key_ids)
            if accession != current:
                close_run()
                if current is not None and accession < current:
                    counters["order_breaks"] += 1
                current = accession
                current_set = member
                current_keys = Counter()
                current_mask = 0
            current_keys[kid] += 1
            current_mask |= FAMILY_BITS[family]
        close_run()
        flush_buffers()

    superset_patterns = Counter(pattern_of(mask) for mask in superset_masks.values())
    superset_patterns["no_rows"] = len(superset) - len(superset_masks)
    neighbour_patterns = Counter(pattern_of(mask) for mask in neighbour_masks.values())
    _write_tsv(
        out / "groups.tsv",
        ("set", "code", "reference", "assigned_by", "negated", "rows"),
        sorted(
            ((*key, n) for key, n in groups.items()),
            key=lambda row: (-row[-1], row[:-1]),
        ),
    )
    _write_tsv(
        out / "keys.tsv",
        ("key_id", "code", "reference", "assigned_by", "negated"),
        sorted(((kid, *key) for key, kid in key_ids.items()), key=lambda row: row[0]),
    )
    rows_by_set: Counter[str] = Counter()
    for (member, *_rest), n in groups.items():
        rows_by_set[member] += n
    rows_by_set["outside"] = sum(outside_codes.values())
    record = {
        "pass": "gaf",
        "release": release,
        "published": published.isoformat(),
        "cut_release": cut_release,
        "cut_date": cut_date.isoformat(),
        **dict(counters),
        "rows_by_set": dict(rows_by_set),
        "runs_by_set": dict(runs_by_set),
        "proteins_by_set": {"superset": len(superset_masks), "neighbour": len(neighbour_masks)},
        "superset_members": len(superset),
        "superset_by_pattern": dict(superset_patterns),
        "neighbours_listed": len(neighbours),
        "neighbours_by_pattern": dict(neighbour_patterns),
        "neighbours_with_no_rows": len(neighbours) - len(neighbour_masks),
        "outside_rows_by_code": dict(outside_codes),
        "unknown_codes": dict(unknown_codes),
        "keys": len(key_ids),
    }
    _write_record(out / "gaf.json", record)
    return record


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 22), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_record(path: Path, record: dict[str, Any]) -> None:
    path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_tsv(path: Path, header: Sequence[str], rows: Iterable[Sequence[Any]]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\t".join(header) + "\n")
        for row in rows:
            handle.write("\t".join(str(value) for value in row) + "\n")


def _stdin_lines() -> io.TextIOWrapper:
    """Stdin as UTF-8 text with a 16 MiB buffer; a bad byte is replaced, never fatal.

    The fields the passes read (accession, code, reference, assigned_by) are
    ASCII, so a replacement character can only land in a free-text column.
    """
    raw = open(sys.stdin.fileno(), "rb", buffering=1 << 24, closefd=False)
    return io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="\n")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m protea.core.cut_photo.measure")
    sub = parser.add_subparsers(dest="pass_name", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--published", required=True, type=date.fromisoformat)
        p.add_argument("--cut-release", required=True, type=int)
        p.add_argument("--cut-date", required=True, type=date.fromisoformat)
        p.add_argument("--out", required=True, type=Path)
        p.add_argument("--revision", default="", help="code revision, recorded as given")

    p_sup = sub.add_parser("superset")
    p_sup.add_argument("--archive", required=True, type=Path)
    p_sup.add_argument("--release", required=True)
    common(p_sup)

    p_nei = sub.add_parser("neighbours", help="filtered UniRef XML lines on stdin")
    p_nei.add_argument("--superset", required=True, type=Path)
    p_nei.add_argument("--release", required=True)
    common(p_nei)

    p_gaf = sub.add_parser("gaf", help="decompressed GAF lines on stdin")
    p_gaf.add_argument("--superset", required=True, type=Path)
    p_gaf.add_argument("--neighbours", required=True, type=Path)
    p_gaf.add_argument("--release", required=True, type=int)
    common(p_gaf)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    shared = {
        "published": args.published,
        "cut_release": args.cut_release,
        "cut_date": args.cut_date,
        "out": args.out,
    }
    if args.pass_name == "superset":
        record = superset_pass(args.archive, release=args.release, **shared)
    elif args.pass_name == "neighbours":
        record = neighbours_pass(
            _stdin_lines(), superset=read_superset(args.superset), release=args.release, **shared
        )
    else:
        record = gaf_pass(
            _stdin_lines(),
            superset=read_superset(args.superset),
            neighbours=read_neighbours(args.neighbours),
            release=args.release,
            **shared,
        )
    stamp = {"revision": args.revision, "argv": list(sys.argv if argv is None else argv)}
    run_path = args.out / f"{record['pass']}.run.json"
    _write_record(run_path, stamp)
    print(json.dumps(record, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
