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
from dataclasses import dataclass
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


@dataclass(frozen=True)
class PassSource:
    """The release a pass reads, when it was published, the cut it serves and where it writes.

    ``release`` is UniProt's name (``2022_03``) for the superset and the
    neighbours, and GOA's number (211) for the GAF pass; it is recorded as given.
    """

    release: str | int
    published: date
    cut_release: int
    cut_date: date
    out: Path

    def check(self, family: str) -> None:
        """Refuse the cut or the source, naming the source as ``<family> <release>``."""
        check_dates(
            source=f"{family} {self.release}",
            published=self.published,
            cut_release=self.cut_release,
            cut_date=self.cut_date,
        )

    def head(self, pass_name: str) -> dict[str, Any]:
        """The fields every pass record starts with."""
        return {
            "pass": pass_name,
            "release": self.release,
            "published": self.published.isoformat(),
            "cut_release": self.cut_release,
            "cut_date": self.cut_date.isoformat(),
        }


class _ParquetBuffer:
    """Rows buffered column by column and written to a Parquet file in batches."""

    def __init__(self, writer: pq.ParquetWriter, schema: pa.Schema, batch_rows: int) -> None:
        self._writer = writer
        self._schema = schema
        self._batch_rows = batch_rows
        self._columns: list[list[Any]] = [[] for _ in schema]

    def append(self, *values: Any) -> None:
        for column, value in zip(self._columns, values, strict=True):
            column.append(value)
        if len(self._columns[0]) >= self._batch_rows:
            self.flush()

    def flush(self) -> None:
        if self._columns[0]:
            self._writer.write_table(pa.table(self._columns, schema=self._schema))
            for column in self._columns:
                column.clear()


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


def superset_pass(archive: Path, source: PassSource) -> dict[str, Any]:
    """Write ``superset.tsv`` and ``superset.json`` for one archived release."""
    source.check("UniProt")
    with tarfile.open(archive, "r:gz") as tar:
        handle = tar.extractfile(SWISSPROT_FASTA_MEMBER)
        if handle is None:
            raise ValueError(f"{archive} has no {SWISSPROT_FASTA_MEMBER}")
        with gzip.GzipFile(fileobj=handle) as fasta:
            members = parse_swissprot_fasta(fasta)
    blob = canonical_table(members)
    source.out.mkdir(parents=True, exist_ok=True)
    (source.out / "superset.tsv").write_bytes(blob)
    record = {
        **source.head("superset"),
        "accessions": len(members),
        "sha256": hashlib.sha256(blob).hexdigest(),
        "archive": {
            "path": str(archive),
            "bytes": archive.stat().st_size,
            "sha256": _file_sha256(archive),
        },
    }
    _write_record(source.out / "superset.json", record)
    return record


def neighbours_pass(
    lines: Iterable[str],
    *,
    superset: frozenset[str],
    source: PassSource,
    batch_rows: int = 1_000_000,
) -> dict[str, Any]:
    """Write ``neighbours.parquet`` and ``neighbours.json`` from a filtered UniRef stream."""
    source.check("UniRef")
    source.out.mkdir(parents=True, exist_ok=True)
    schema = pa.schema(
        [("cluster", pa.string()), ("accession", pa.string()), ("in_superset", pa.bool_())]
    )
    clusters = clusters_with = members_with = 0
    seen: set[str] = set()
    with pq.ParquetWriter(source.out / "neighbours.parquet", schema, compression="zstd") as writer:
        rows = _ParquetBuffer(writer, schema, batch_rows)
        for cluster, accessions in iter_uniref_clusters(lines):
            clusters += 1
            hits = [acc for acc in accessions if acc in superset]
            if not hits:
                continue
            clusters_with += 1
            members_with += len(accessions)
            seen.update(hits)
            for acc in accessions:
                rows.append(cluster, acc, acc in superset)
        rows.flush()
    record = {
        **source.head("neighbours"),
        "clusters": clusters,
        "clusters_with_superset": clusters_with,
        "members_in_those_clusters": members_with,
        "neighbours_not_in_superset": members_with - len(seen),
        "superset_in_a_cluster": len(seen),
        "superset_in_no_cluster": len(superset) - len(seen),
    }
    _write_record(source.out / "neighbours.json", record)
    return record


class _GafInventory:
    """The running counts of the GAF pass, fed one line at a time.

    A run is a stretch of consecutive rows of one accession. GOA is not strictly
    ordered by accession, so one protein can come in several runs; its family
    mask is OR-ed across them and its keys are written once per run, to be
    summed by (accession, key).
    """

    def __init__(
        self, superset: frozenset[str], neighbours: frozenset[str], keys_out: _ParquetBuffer
    ) -> None:
        self.superset = superset
        self.neighbours = neighbours
        self.keys_out = keys_out
        self.groups: Counter[tuple[str, str, str, str, bool]] = Counter()
        self.key_ids: dict[tuple[str, str, str, bool], int] = {}
        self.outside_codes: Counter[str] = Counter()
        self.unknown_codes: Counter[str] = Counter()
        self.masks: dict[str, dict[str, int]] = {"superset": {}, "neighbour": {}}
        self.counters = Counter(lines=0, uniprot_rows=0, malformed=0, order_breaks=0)
        self.runs_by_set: Counter[str] = Counter()
        self._current: str | None = None
        self._current_set = ""
        self._current_keys: Counter[int] = Counter()
        self._current_mask = 0

    def feed(self, line: str) -> None:
        """Count one GAF line."""
        self.counters["lines"] += 1
        if not line.startswith(_GAF_PREFIX):
            return
        self.counters["uniprot_rows"] += 1
        accession = line[len(_GAF_PREFIX) : line.index("\t", len(_GAF_PREFIX))]
        if accession in self.superset:
            member = "superset"
        elif accession in self.neighbours:
            member = "neighbour"
        else:
            head = line.split("\t", GAF_EVIDENCE + 1)
            if len(head) > GAF_EVIDENCE:
                self.outside_codes[head[GAF_EVIDENCE]] += 1
            return
        cols = line.rstrip("\n").split("\t")
        if len(cols) <= GAF_ASSIGNED_BY:
            self.counters["malformed"] += 1
            return
        self._count(accession, member, cols)

    def _count(self, accession: str, member: str, cols: list[str]) -> None:
        code = cols[GAF_EVIDENCE]
        family = code_family(code)
        if family == "unknown":
            self.unknown_codes[code] += 1
        key = (
            code,
            reference_class(cols[GAF_REFERENCE]),
            cols[GAF_ASSIGNED_BY],
            is_negated(cols[GAF_QUALIFIER]),
        )
        self.groups[(member, *key)] += 1
        kid = self.key_ids.setdefault(key, len(self.key_ids))
        if accession != self._current:
            self._close_run()
            if self._current is not None and accession < self._current:
                self.counters["order_breaks"] += 1
            self._current, self._current_set = accession, member
            self._current_keys, self._current_mask = Counter(), 0
        self._current_keys[kid] += 1
        self._current_mask |= FAMILY_BITS[family]

    def _close_run(self) -> None:
        if self._current is None:
            return
        for kid, n in self._current_keys.items():
            self.keys_out.append(self._current, kid, n)
        self.runs_by_set[self._current_set] += 1
        masks = self.masks[self._current_set]
        masks[self._current] = masks.get(self._current, 0) | self._current_mask

    def finish(self) -> None:
        """Close the last run and write what the buffer still holds."""
        self._close_run()
        self._current = None
        self.keys_out.flush()

    def write_tables(self, out: Path) -> None:
        """Write ``groups.tsv`` and ``keys.tsv``."""
        _write_tsv(
            out / "groups.tsv",
            ("set", "code", "reference", "assigned_by", "negated", "rows"),
            sorted(
                ((*key, n) for key, n in self.groups.items()),
                key=lambda row: (-row[-1], row[:-1]),
            ),
        )
        _write_tsv(
            out / "keys.tsv",
            ("key_id", "code", "reference", "assigned_by", "negated"),
            sorted(((kid, *key) for key, kid in self.key_ids.items()), key=lambda row: row[0]),
        )

    def summary(self) -> dict[str, Any]:
        """The counts ``gaf.json`` records after the source fields."""
        superset_masks, neighbour_masks = self.masks["superset"], self.masks["neighbour"]
        superset_patterns = Counter(pattern_of(mask) for mask in superset_masks.values())
        superset_patterns["no_rows"] = len(self.superset) - len(superset_masks)
        rows_by_set: Counter[str] = Counter()
        for (member, *_rest), n in self.groups.items():
            rows_by_set[member] += n
        rows_by_set["outside"] = sum(self.outside_codes.values())
        return {
            **dict(self.counters),
            "rows_by_set": dict(rows_by_set),
            "runs_by_set": dict(self.runs_by_set),
            "proteins_by_set": {"superset": len(superset_masks), "neighbour": len(neighbour_masks)},
            "superset_members": len(self.superset),
            "superset_by_pattern": dict(superset_patterns),
            "neighbours_listed": len(self.neighbours),
            "neighbours_by_pattern": dict(
                Counter(pattern_of(mask) for mask in neighbour_masks.values())
            ),
            "neighbours_with_no_rows": len(self.neighbours) - len(neighbour_masks),
            "outside_rows_by_code": dict(self.outside_codes),
            "unknown_codes": dict(self.unknown_codes),
            "keys": len(self.key_ids),
        }


def gaf_pass(
    lines: Iterable[str],
    *,
    superset: frozenset[str],
    neighbours: frozenset[str],
    source: PassSource,
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
    source.check("GOA")
    source.out.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([("accession", pa.string()), ("key_id", pa.int32()), ("rows", pa.int64())])
    with pq.ParquetWriter(
        source.out / "protein_keys.parquet", schema, compression="zstd"
    ) as writer:
        inventory = _GafInventory(superset, neighbours, _ParquetBuffer(writer, schema, batch_rows))
        for line in lines:
            inventory.feed(line)
        inventory.finish()
    inventory.write_tables(source.out)
    record = {**source.head("gaf"), **inventory.summary()}
    _write_record(source.out / "gaf.json", record)
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


def _source(args: argparse.Namespace) -> PassSource:
    return PassSource(
        release=args.release,
        published=args.published,
        cut_release=args.cut_release,
        cut_date=args.cut_date,
        out=args.out,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    source = _source(args)
    if args.pass_name == "superset":
        record = superset_pass(args.archive, source)
    elif args.pass_name == "neighbours":
        record = neighbours_pass(
            _stdin_lines(), superset=read_superset(args.superset), source=source
        )
    else:
        record = gaf_pass(
            _stdin_lines(),
            superset=read_superset(args.superset),
            neighbours=read_neighbours(args.neighbours),
            source=source,
        )
    stamp = {"revision": args.revision, "argv": list(sys.argv if argv is None else argv)}
    run_path = args.out / f"{record['pass']}.run.json"
    _write_record(run_path, stamp)
    print(json.dumps(record, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
