"""Step 2 of ADR-D59: build the photo of a cut.

Four passes, each a pure function of its inputs that writes its own record, so a
pass runs where its source is (a cluster job streaming a GOA release) and can be
re-run alone. None opens a database connection.

``members``
    The base of the cut: the superset (the paired release's Swiss-Prot, from the
    canonical table the ``superset`` measure pass wrote) and every accession GOA t
    admits by ADR-D49's codes (truth and curated inference; a ``NOT`` row admits,
    the qualifier is never read). GOA on stdin.
``rows``
    Every GOA t row of a member, with its evidence channel
    (:func:`protea.core.cut_photo.channels.channel_of`), and the profile of each
    UniRef50 cluster that holds a superset protein: how many of its members
    outside the base carry each GO term in each profile channel. GOA on stdin,
    read a second time. A code or an electronic reference the channel table does
    not list is counted, written out and refuses the photo.
``sequences``
    The sequence of every superset member, from the paired release's Swiss-Prot
    FASTA (ADR-D59, decision 5).
``manifest``
    The photo's manifest: the cut, the sources, the sha256 of every output and
    whether the photo is refused. Assembled from the other passes' records.

The two tables the passes read from the measurement (``superset.tsv`` and
``neighbours.parquet``) are named by the record the measure pass wrote beside
each one: the pass refuses a table whose record is missing, was written for
another cut or names a source the guard refuses, and records its release and
sha256, so the manifest says which Swiss-Prot and which UniRef the photo is of.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
import tarfile
from collections import Counter
from collections.abc import Iterable, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from protea.core.cut_photo.channels import (
    CHANNELS,
    ELECTRONIC_REFERENCES,
    GOREF_LIST_READ_ON,
    PROFILE_CHANNELS,
    UnknownEvidence,
    channel_of,
)
from protea.core.cut_photo.formats import (
    CURATED_INFERENCE_CODES,
    GAF_ASPECT,
    GAF_ASSIGNED_BY,
    GAF_DATE,
    GAF_EVIDENCE,
    GAF_GO,
    GAF_ID,
    GAF_QUALIFIER,
    GAF_REFERENCE,
    GAF_TAXON,
    GAF_WITH_FROM,
    TRUTH_CODES,
    is_negated,
    reference_class,
)
from protea.core.cut_photo.measure import (
    SWISSPROT_FASTA_MEMBER,
    PassSource,
    PhotoGuardError,
    _file_sha256,
    _ParquetBuffer,
    _stdin_lines,
    _write_record,
)
from protea.core.row_alignment import lookup_by
from protea.core.swissprot_table import canonical_table, iter_swissprot_records, read_local_table

_GAF_PREFIX = "UniProtKB\t"
#: How many distinct raw references ``unknown_evidence.tsv`` shows for each key.
UNKNOWN_EXAMPLES = 3
ADMITTING_CODES = TRUTH_CODES | CURATED_INFERENCE_CODES

#: The outputs a complete photo has, in the order the manifest lists them.
OUTPUTS: tuple[str, ...] = (
    "members.parquet",
    "rows.parquet",
    "clusters.parquet",
    "profiles.parquet",
    "sequences.parquet",
)


def channel_table_digest() -> str:
    """The sha256 of the declared electronic reference table, recorded per photo."""
    blob = json.dumps(sorted(ELECTRONIC_REFERENCES.items()), sort_keys=True).encode()
    return hashlib.sha256(blob).hexdigest()


def upstream_source(record_path: Path, family: str, source: PassSource) -> dict[str, Any]:
    """The release an input table was measured from, read from the record beside it.

    The record must have been written for this cut, and its source must pass the
    guard the measure pass asked (published on or before the cut, a UniProt or
    UniRef release under the ceiling), so a table measured for another cut or
    from a later release refuses here instead of entering the photo.
    """
    if not record_path.exists():
        raise PhotoGuardError(f"{record_path} is missing: the {family} release cannot be named")
    record = json.loads(record_path.read_text(encoding="utf-8"))
    written_for = (int(record["cut_release"]), str(record["cut_date"]))
    if written_for != (source.cut_release, source.cut_date.isoformat()):
        raise PhotoGuardError(
            f"{record_path} was written for cut {written_for[0]} ({written_for[1]}), "
            f"not {source.cut_release} ({source.cut_date.isoformat()})"
        )
    PassSource(
        release=str(record["release"]),
        published=date.fromisoformat(str(record["published"])),
        cut_release=source.cut_release,
        cut_date=source.cut_date,
        out=source.out,
    ).check(family)
    return {"release": str(record["release"]), "published": str(record["published"])}


def _gaf_columns(lines: Iterable[str]) -> Iterable[list[str]]:
    for line in lines:
        if line.startswith(_GAF_PREFIX):
            cols = line.rstrip("\n").split("\t")
            if len(cols) > GAF_ASSIGNED_BY:
                yield cols


def members_pass(
    lines: Iterable[str],
    *,
    superset_table: Path,
    source: PassSource,
    source_file: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Write ``members.parquet``: the superset and every accession GOA t admits.

    ``source_file`` is the GOA file the lines were decompressed from, as the caller
    declares it (``url`` and ``sha256``); the manifest refuses a photo without it.
    """
    source.check("GOA")
    superset_source = upstream_source(superset_table.with_name("superset.json"), "UniProt", source)
    superset = read_local_table(str(superset_table))
    superset_sha256 = hashlib.sha256(canonical_table(superset)).hexdigest()
    recorded = json.loads(superset_table.with_name("superset.json").read_text(encoding="utf-8"))
    if recorded.get("sha256") != superset_sha256:
        raise PhotoGuardError(
            f"{superset_table} hashes to {superset_sha256}, its record says {recorded.get('sha256')}"
        )
    truth: set[str] = set()
    curated: set[str] = set()
    for cols in _gaf_columns(lines):
        code = cols[GAF_EVIDENCE]
        if code in TRUTH_CODES:
            truth.add(cols[GAF_ID])
        elif code in CURATED_INFERENCE_CODES:
            curated.add(cols[GAF_ID])
    accessions = sorted(set(superset) | truth | curated)
    table = pa.table(
        {
            "accession": accessions,
            "superset": [a in superset for a in accessions],
            "truth": [a in truth for a in accessions],
            "curated": [a in curated for a in accessions],
            "md5": [superset.get(a) for a in accessions],
        }
    )
    source.out.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, source.out / "members.parquet", compression="zstd")
    record = {
        **source.head("members"),
        "members": len(accessions),
        "superset": len(superset),
        "by_truth": len(truth),
        "by_curated_only": len(curated - truth),
        "outside_superset": len(set(accessions) - set(superset)),
        "superset_source": {**superset_source, "sha256": superset_sha256},
        "source_file": dict(source_file or {}),
    }
    _write_record(source.out / "members.json", record)
    return record


_ROW_SCHEMA = pa.schema(
    [
        ("accession", pa.string()),
        ("go_id", pa.string()),
        ("qualifier", pa.string()),
        ("code", pa.string()),
        ("reference", pa.string()),
        ("with_from", pa.string()),
        ("aspect", pa.string()),
        ("taxon", pa.string()),
        ("date", pa.string()),
        ("assigned_by", pa.string()),
        ("channel", pa.string()),
    ]
)


class _RowsAndProfiles:
    """The running state of the rows pass: member rows out, neighbour profiles counted."""

    def __init__(
        self, members: dict[str, bool], cluster_of: dict[str, str], rows_out: _ParquetBuffer
    ):
        self.members = members
        self.cluster_of = cluster_of
        self.rows_out = rows_out
        self.seen: set[tuple[str, str, str, bool]] = set()
        self.profile: Counter[tuple[str, str, str, bool]] = Counter()
        self.unknown: Counter[tuple[str, str, str, str]] = Counter()
        self.examples: dict[tuple[str, str, str, str], list[str]] = {}
        self.counts: Counter[str] = Counter()

    def feed(self, cols: list[str]) -> None:
        accession = cols[GAF_ID]
        reviewed = self.members.get(accession)
        cluster = None if reviewed is not None else self.cluster_of.get(accession)
        if reviewed is None and cluster is None:
            return
        code, reference = cols[GAF_EVIDENCE], cols[GAF_REFERENCE]
        kind = reference_class(reference)
        channel: str
        try:
            channel = channel_of(code, kind, bool(reviewed))
        except UnknownEvidence:
            self._count_unknown(code, kind, reference, cols[GAF_ASSIGNED_BY])
            channel = "unknown"
        if reviewed is not None:
            self._member_row(cols, channel)
        elif channel in PROFILE_CHANNELS and cluster is not None:
            self._profile_row(cluster, accession, cols, channel)

    def _count_unknown(self, code: str, kind: str, reference: str, assigned_by: str) -> None:
        """Count a row the channel table cannot place, keeping a few raw references.

        The key holds the reference CLASS (the whole ``GO_REF`` id, ``publication``
        or ``other``) beside the raw prefix: a prefix alone would turn every unknown
        ``GO_REF`` into the same ``GO_REF`` key, and the examples are what the
        decision to classify them is read from.
        """
        prefix = reference.split(":", 1)[0] if reference else ""
        key = (code, kind, prefix, assigned_by)
        self.unknown[key] += 1
        shown = self.examples.setdefault(key, [])
        if len(shown) < UNKNOWN_EXAMPLES and reference not in shown:
            shown.append(reference)

    def _member_row(self, cols: list[str], channel: str) -> None:
        self.counts["member_rows"] += 1
        self.counts[f"member_rows_{channel}"] += 1
        self.rows_out.append(
            cols[GAF_ID],
            cols[GAF_GO],
            cols[GAF_QUALIFIER],
            cols[GAF_EVIDENCE],
            cols[GAF_REFERENCE],
            cols[GAF_WITH_FROM],
            cols[GAF_ASPECT],
            cols[GAF_TAXON],
            cols[GAF_DATE],
            cols[GAF_ASSIGNED_BY],
            channel,
        )

    def _profile_row(self, cluster: str, accession: str, cols: list[str], channel: str) -> None:
        self.counts["neighbour_profile_rows"] += 1
        negated = is_negated(cols[GAF_QUALIFIER])
        once = (accession, cols[GAF_GO], channel, negated)
        if once in self.seen:
            return
        self.seen.add(once)
        self.profile[(cluster, cols[GAF_GO], channel, negated)] += 1


def _read_members(path: Path) -> dict[str, bool]:
    table = pq.read_table(path, columns=["accession", "superset"])
    pairs = zip(
        table.column("accession").to_pylist(), table.column("superset").to_pylist(), strict=True
    )
    by_accession = lookup_by(pairs, lambda pair: pair[0], context=f"{path.name} accessions")
    return {accession: bool(pair[1]) for accession, pair in by_accession.items()}


def _read_clusters(path: Path, members: dict[str, bool]) -> tuple[dict[str, str], dict[str, str]]:
    """``(cluster of each neighbour outside the base, cluster of each superset member)``.

    UniRef puts an accession in one cluster, so an accession listed twice is
    refused rather than kept with its last cluster; and a protein the table
    marks as superset must be a superset member of this photo, or the table was
    measured against another release.
    """
    table = pq.read_table(path, columns=["cluster", "accession", "in_superset"])
    neighbours: dict[str, str] = {}
    superset: dict[str, str] = {}
    for cluster, accession, in_superset in zip(
        table.column("cluster").to_pylist(),
        table.column("accession").to_pylist(),
        table.column("in_superset").to_pylist(),
        strict=True,
    ):
        if accession in superset or accession in neighbours:
            raise PhotoGuardError(f"{path.name} lists {accession} in more than one cluster")
        if in_superset:
            if members.get(accession) is not True:
                raise PhotoGuardError(
                    f"{path.name} marks {accession} as Swiss-Prot, this photo's superset does not"
                )
            superset[accession] = cluster
        elif accession not in members:
            neighbours[accession] = cluster
    return neighbours, superset


def rows_pass(
    lines: Iterable[str],
    *,
    members_path: Path,
    neighbours_path: Path,
    source: PassSource,
    source_file: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Write the member rows with their channel, the clusters and the neighbour profiles."""
    source.check("GOA")
    neighbours_source = upstream_source(
        neighbours_path.with_name("neighbours.json"), "UniRef", source
    )
    members = _read_members(members_path)
    neighbours, superset_clusters = _read_clusters(neighbours_path, members)
    out = source.out
    with pq.ParquetWriter(out / "rows.parquet", _ROW_SCHEMA, compression="zstd") as writer:
        state = _RowsAndProfiles(
            members, neighbours, _ParquetBuffer(writer, _ROW_SCHEMA, 1_000_000)
        )
        for cols in _gaf_columns(lines):
            state.feed(cols)
        state.rows_out.flush()
    _write_clusters(out, superset_clusters)
    _write_profiles(out, state.profile)
    _write_unknown(out, state.unknown, state.examples)
    record = {
        **source.head("rows"),
        **dict(state.counts),
        "clusters_with_superset": len(set(superset_clusters.values())),
        "neighbours_outside_base": len(neighbours),
        "profile_keys": len(state.profile),
        "unknown_evidence_rows": sum(state.unknown.values()),
        "unknown_evidence_keys": len(state.unknown),
        "neighbours_source": {**neighbours_source, "sha256": _file_sha256(neighbours_path)},
        "source_file": dict(source_file or {}),
        "channel_table": {
            "goref_list_read_on": GOREF_LIST_READ_ON,
            "sha256": channel_table_digest(),
        },
    }
    _write_record(out / "rows.json", record)
    return record


def _write_clusters(out: Path, superset_clusters: dict[str, str]) -> None:
    accessions = sorted(superset_clusters)
    table = pa.table(
        {"accession": accessions, "cluster": [superset_clusters[a] for a in accessions]}
    )
    pq.write_table(table, out / "clusters.parquet", compression="zstd")


def _write_profiles(out: Path, profile: Counter[tuple[str, str, str, bool]]) -> None:
    keys = sorted(profile)
    table = pa.table(
        {
            "cluster": [k[0] for k in keys],
            "go_id": [k[1] for k in keys],
            "channel": [k[2] for k in keys],
            "negated": [k[3] for k in keys],
            "members": [profile[k] for k in keys],
        }
    )
    pq.write_table(table, out / "profiles.parquet", compression="zstd")


def _write_unknown(
    out: Path,
    unknown: Counter[tuple[str, str, str, str]],
    examples: dict[tuple[str, str, str, str], list[str]],
) -> None:
    """One line per key, most rows first, with up to ``UNKNOWN_EXAMPLES`` raw references."""
    with open(out / "unknown_evidence.tsv", "w", encoding="utf-8") as handle:
        handle.write("code\treference_class\treference_prefix\tassigned_by\trows\texamples\n")
        for key, n in sorted(unknown.items(), key=lambda kv: (-kv[1], kv[0])):
            code, kind, prefix, by = key
            shown = " ; ".join(examples.get(key, []))
            handle.write(f"{code}\t{kind}\t{prefix}\t{by}\t{n}\t{shown}\n")


def sequences_pass(
    archive: Path, source: PassSource, *, source_url: str = "", expected_sha256: str = ""
) -> dict[str, Any]:
    """Write ``sequences.parquet``: every Swiss-Prot sequence of the paired release.

    The archive's sha256 is computed here; ``expected_sha256``, when given, must
    match it, so a caller that checked the download can say so.
    """
    source.check("UniProt")
    archive_sha256 = _file_sha256(archive)
    if expected_sha256 and expected_sha256 != archive_sha256:
        raise PhotoGuardError(f"{archive} hashes to {archive_sha256}, not {expected_sha256}")
    accessions: list[str] = []
    digests: list[str] = []
    sequences: list[str] = []
    table_of: dict[str, str] = {}
    with tarfile.open(archive, "r:gz") as tar:
        handle = tar.extractfile(SWISSPROT_FASTA_MEMBER)
        if handle is None:
            raise ValueError(f"{archive} has no {SWISSPROT_FASTA_MEMBER}")
        with gzip.GzipFile(fileobj=handle) as fasta:
            for accession, digest, sequence in iter_swissprot_records(fasta):
                if accession in table_of:
                    raise ValueError(f"{accession} appears twice in {archive}")
                table_of[accession] = digest
                accessions.append(accession)
                digests.append(digest)
                sequences.append(sequence)
    table = pa.table({"accession": accessions, "md5": digests, "sequence": sequences})
    source.out.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, source.out / "sequences.parquet", compression="zstd")
    record = {
        **source.head("sequences"),
        "sequences": len(accessions),
        "archive_sha256": archive_sha256,
        "source_file": {"url": source_url, "sha256": archive_sha256},
        "superset_sha256": hashlib.sha256(canonical_table(table_of)).hexdigest(),
    }
    _write_record(source.out / "sequences.json", record)
    return record


def photo_sources(records: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """The photo's sources, named by what they are, from the pass records."""
    members, rows, sequences = records["members"], records["rows"], records["sequences"]
    return {
        "goa": {
            "release": members["release"],
            "published": members["published"],
            "url": members.get("source_file", {}).get("url", ""),
            "sha256": members.get("source_file", {}).get("sha256", ""),
        },
        "uniprot": {
            "release": sequences["release"],
            "published": sequences["published"],
            "url": sequences.get("source_file", {}).get("url", ""),
            "archive_sha256": sequences["archive_sha256"],
            "superset_sha256": members["superset_source"]["sha256"],
        },
        "uniref": dict(rows["neighbours_source"]),
    }


def source_disagreements(records: dict[str, dict[str, Any]]) -> list[str]:
    """What the passes say about the same source and do not agree on.

    The two GOA passes must have read the same release from the same declared
    file (ADR-D59 records each source's URL and sha256, so a GOA file with no
    declared digest refuses too), and the superset table and the sequences must
    be the same UniProt release: as many entries, and the same canonical table
    of accessions and md5s.
    """
    members, rows, sequences = records["members"], records["rows"], records["sequences"]
    found: list[str] = []
    goa_file = members.get("source_file", {})
    if not goa_file.get("url") or len(goa_file.get("sha256", "")) != 64:
        found.append(f"the GOA file is not declared with its URL and sha256: {goa_file}")
    if rows.get("source_file", {}) != goa_file:
        found.append(f"GOA file: members pass {goa_file}, rows pass {rows.get('source_file')}")
    if not sequences.get("source_file", {}).get("url"):
        found.append("the UniProt archive is not declared with its URL")
    for key in ("release", "published", "cut_release", "cut_date"):
        if members[key] != rows[key]:
            found.append(f"GOA {key}: members pass {members[key]}, rows pass {rows[key]}")
        if key.startswith("cut") and members[key] != sequences[key]:
            found.append(f"{key}: members pass {members[key]}, sequences pass {sequences[key]}")
    superset = members["superset_source"]
    for key in ("release", "published"):
        if str(superset[key]) != str(sequences[key]):
            found.append(f"UniProt {key}: superset {superset[key]}, sequences {sequences[key]}")
    if int(members["superset"]) != int(sequences["sequences"]):
        found.append(
            f"superset holds {members['superset']} entries, sequences {sequences['sequences']}"
        )
    if superset["sha256"] != sequences["superset_sha256"]:
        found.append(
            f"the superset table hashes to {superset['sha256']}, the sequences' accessions "
            f"and md5s to {sequences['superset_sha256']}"
        )
    return found


def manifest_pass(out: Path, revision: str) -> dict[str, Any]:
    """Write ``photo.json`` from the pass records and the sha256 of every output."""
    records = {
        name: json.loads((out / f"{name}.json").read_text())
        for name in ("members", "rows", "sequences")
    }
    missing = [name for name in OUTPUTS if not (out / name).exists()]
    unknown = int(records["rows"].get("unknown_evidence_rows", 0))
    sources = photo_sources(records)
    disagreements = source_disagreements(records)
    manifest = {
        "photo": "cut-photo",
        "cut_release": records["members"]["cut_release"],
        "cut_date": records["members"]["cut_date"],
        "sources": sources,
        "channels": list(CHANNELS),
        "channel_table": records["rows"]["channel_table"],
        "outputs": {name: _file_sha256(out / name) for name in OUTPUTS if name not in missing},
        "counts": {name: records[name] for name in records},
        "status": "refused" if unknown or missing or disagreements else "complete",
        "refusals": {
            "unknown_evidence_rows": unknown,
            "missing_outputs": missing,
            "source_disagreements": disagreements,
        },
        "revision": revision,
    }
    _write_record(out / "photo.json", manifest)
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m protea.core.cut_photo.build")
    sub = parser.add_subparsers(dest="pass_name", required=True)

    def source(p: argparse.ArgumentParser) -> None:
        p.add_argument("--release", required=True)
        p.add_argument("--published", required=True)
        p.add_argument("--cut-release", required=True, type=int)
        p.add_argument("--cut-date", required=True)
        p.add_argument("--out", required=True, type=Path)

    def source_file(p: argparse.ArgumentParser, *, sha256_required: bool) -> None:
        p.add_argument("--source-url", required=True, help="where the source was downloaded from")
        p.add_argument(
            "--source-sha256",
            required=sha256_required,
            default="",
            help="the sha256 of the source file, checked by the caller (GOA) or here (UniProt)",
        )

    p_mem = sub.add_parser("members", help="decompressed GAF lines on stdin")
    p_mem.add_argument("--superset-table", required=True, type=Path)
    source(p_mem)
    source_file(p_mem, sha256_required=True)
    p_rows = sub.add_parser("rows", help="decompressed GAF lines on stdin")
    p_rows.add_argument("--members", required=True, type=Path)
    p_rows.add_argument("--neighbours", required=True, type=Path)
    source(p_rows)
    source_file(p_rows, sha256_required=True)
    p_seq = sub.add_parser("sequences")
    p_seq.add_argument("--archive", required=True, type=Path)
    source(p_seq)
    source_file(p_seq, sha256_required=False)
    p_man = sub.add_parser("manifest")
    p_man.add_argument("--out", required=True, type=Path)
    p_man.add_argument("--revision", required=True)
    return parser


def _pass_source(args: argparse.Namespace) -> PassSource:
    release: str | int = (
        int(args.release) if args.pass_name in ("members", "rows") else args.release
    )
    return PassSource(
        release=release,
        published=date.fromisoformat(args.published),
        cut_release=args.cut_release,
        cut_date=date.fromisoformat(args.cut_date),
        out=args.out,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.pass_name == "manifest":
        record = manifest_pass(args.out, args.revision)
        print(json.dumps({"status": record["status"], "refusals": record["refusals"]}))
        return 0 if record["status"] == "complete" else 3
    src = _pass_source(args)
    declared = {"url": args.source_url, "sha256": args.source_sha256.strip().lower()}
    if args.pass_name == "members":
        record = members_pass(
            _stdin_lines(), superset_table=args.superset_table, source=src, source_file=declared
        )
    elif args.pass_name == "rows":
        record = rows_pass(
            _stdin_lines(),
            members_path=args.members,
            neighbours_path=args.neighbours,
            source=src,
            source_file=declared,
        )
    else:
        record = sequences_pass(
            args.archive, src, source_url=declared["url"], expected_sha256=declared["sha256"]
        )
    print(json.dumps(record, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
