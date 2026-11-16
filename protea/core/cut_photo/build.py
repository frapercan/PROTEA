"""Step 2 of ADR-D59: build the photo of a cut.

Five passes, each a pure function of its inputs that writes its own record, so a
pass runs where its source is (a cluster job streaming a GOA release) and can be
re-run alone. None opens a database connection.

``members`` (:mod:`protea.core.cut_photo.members`)
    Who the corpus at the cut is: the superset, every accession GOA t admits by
    ADR-D49's codes, and every annotated UniRef50 neighbour of a superset protein,
    each with its sets and its cluster. GOA on stdin.
``rows`` (:mod:`protea.core.cut_photo.rows`)
    Every GOA t row of a member, with its evidence channel. GOA on stdin, read a
    second time. A key the channel table does not list refuses the photo.
``sequences``, ``unreviewed`` (:mod:`protea.core.cut_photo.sequences`)
    Every member's sequence from the paired UniProt release: Swiss-Prot's for the
    superset, TrEMBL's for the rest (ADR-D59, decision 5).
``manifest``
    The photo's manifest: the cut, the sources, the sha256 of every output and
    whether the photo is refused. Assembled from the other passes' records.

The two tables the members pass reads from the measurement (``superset.tsv`` and
``neighbours.parquet``) are named by the record the measure pass wrote beside each
one: the pass refuses a table whose record is missing, was written for another cut
or names a source the guard refuses, and records its release and sha256, so the
manifest says which Swiss-Prot and which UniRef the photo is of.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any

from protea.core.cut_photo.channels import CHANNELS
from protea.core.cut_photo.formats import EMPTY_GO_REF_BY_WITH_FROM
from protea.core.cut_photo.measure import PassSource, _file_sha256, _stdin_lines, _write_record
from protea.core.cut_photo.members import members_pass
from protea.core.cut_photo.photo_io import channel_table_digest, declared_go_release
from protea.core.cut_photo.rows import rows_pass
from protea.core.cut_photo.sequences import (
    KNOWLEDGEBASE_STREAM_MD5,
    sequences_pass,
    unreviewed_pass,
)

#: Kept importable from here: the loader, the tests and the cluster scripts name them so.
__all__ = [
    "EMPTY_GO_REF_BY_WITH_FROM",
    "KNOWLEDGEBASE_STREAM_MD5",
    "OUTPUTS",
    "OUTPUTS_BY_KIND",
    "channel_table_digest",
    "declared_go_release",
    "main",
    "manifest_pass",
    "members_pass",
    "rows_pass",
    "sequences_pass",
    "unreviewed_pass",
]

#: The outputs a complete photo has, in the order the manifest lists them.
OUTPUTS: tuple[str, ...] = (
    "members.parquet",
    "rows.parquet",
    "sequences.parquet",
    "unreviewed_sequences.parquet",
)
#: The outputs each kind of photo must have. A truth-only photo ends a window and
#: is read for its truth and its Swiss-Prot list, so it has no TrEMBL sequences.
OUTPUTS_BY_KIND: dict[str, tuple[str, ...]] = {
    "full": OUTPUTS,
    "truth_only": ("members.parquet", "rows.parquet", "sequences.parquet"),
}
_MD5 = re.compile(r"[0-9a-f]{32}")


def photo_sources(records: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """The photo's sources, named by what they are, from the pass records."""
    members, sequences = records["members"], records["sequences"]
    return {
        "goa": {
            "release": members["release"],
            "published": members["published"],
            "url": members.get("source_file", {}).get("url", ""),
            "sha256": members.get("source_file", {}).get("sha256", ""),
            "date_generated": members.get("gaf_header", {}).get("date-generated", ""),
            "go_version": members.get("gaf_header", {}).get("go-version", ""),
            "declared_go_release": declared_go_release(members.get("gaf_header", {})),
        },
        "uniprot": {
            "release": sequences["release"],
            "published": sequences["published"],
            "url": sequences.get("source_file", {}).get("url", ""),
            "archive_sha256": sequences["archive_sha256"],
            "superset_sha256": members["superset_source"]["sha256"],
            "knowledgebase": dict(records.get("unreviewed", {}).get("source_file", {})),
        },
        "uniref": dict(members.get("uniref_source", {})),
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
    header = members.get("gaf_header", {})
    if not declared_go_release(header):
        found.append(f"the GAF declares no GO release in its go-version: {header}")
    if rows.get("gaf_header", {}) != header:
        found.append(f"GAF header: members pass {header}, rows pass {rows.get('gaf_header')}")
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
    found += _unreviewed_disagreements(records)
    return found


def _unreviewed_disagreements(records: dict[str, dict[str, Any]]) -> list[str]:
    """The TrEMBL pass read the paired release's archive, checked, for the photo's members.

    Its archive is declared by URL and published md5, the stream the job read hashed
    to that md5, the release and the cut are the sequences pass's, and it looked for
    exactly the members the members pass put outside the superset.
    """
    unreviewed = records.get("unreviewed")
    if not unreviewed:
        return []
    members, sequences = records["members"], records["sequences"]
    found: list[str] = []
    declared = unreviewed.get("source_file", {})
    if not declared.get("url") or not _MD5.fullmatch(str(declared.get("md5", ""))):
        found.append(f"the knowledgebase archive is not declared with its URL and md5: {declared}")
    elif str(unreviewed["release"]) not in declared["url"]:
        found.append(
            f"the knowledgebase archive {declared['url']} is not UniProt {unreviewed['release']}"
        )
    streamed = unreviewed.get("stream_md5", "")
    if streamed != declared.get("md5"):
        found.append(
            f"the knowledgebase stream hashed to {streamed or 'nothing recorded'}, "
            f"UniProt publishes {declared.get('md5')}"
        )
    for key in ("release", "published", "cut_release", "cut_date"):
        if str(unreviewed[key]) != str(sequences[key]):
            found.append(f"{key}: sequences pass {sequences[key]}, TrEMBL pass {unreviewed[key]}")
    if int(unreviewed["members_outside_superset"]) != int(members["outside_superset"]):
        found.append(
            f"the TrEMBL pass looked for {unreviewed['members_outside_superset']} members, "
            f"the members pass put {members['outside_superset']} outside the superset"
        )
    return found


def manifest_pass(out: Path, revision: str) -> dict[str, Any]:
    """Write ``photo.json`` from the pass records and the sha256 of every output."""
    records = {
        name: json.loads((out / f"{name}.json").read_text())
        for name in ("members", "rows", "sequences", "unreviewed")
        if (out / f"{name}.json").exists() or name != "unreviewed"
    }
    stream = out / KNOWLEDGEBASE_STREAM_MD5
    if "unreviewed" in records and stream.exists():
        records["unreviewed"]["stream_md5"] = stream.read_text(encoding="utf-8").strip()
    kind = str(records["members"].get("kind", "full"))
    outputs = OUTPUTS_BY_KIND[kind]
    missing = [name for name in outputs if not (out / name).exists()]
    unknown = int(records["rows"].get("unknown_evidence_rows", 0))
    sources = photo_sources(records)
    disagreements = source_disagreements(records)
    manifest = {
        "photo": "cut-photo",
        "kind": kind,
        "cut_release": records["members"]["cut_release"],
        "cut_date": records["members"]["cut_date"],
        "sources": sources,
        "channels": list(CHANNELS),
        "channel_table": records["rows"]["channel_table"],
        "outputs": {name: _file_sha256(out / name) for name in outputs if name not in missing},
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
    kind = p_mem.add_mutually_exclusive_group(required=True)
    kind.add_argument("--neighbours", type=Path, help="the measured cluster table")
    kind.add_argument(
        "--truth-only", action="store_true", help="a cut that only ends a window: no neighbours"
    )
    source(p_mem)
    source_file(p_mem, sha256_required=True)
    p_rows = sub.add_parser("rows", help="decompressed GAF lines on stdin")
    p_rows.add_argument("--members", required=True, type=Path)
    source(p_rows)
    source_file(p_rows, sha256_required=True)
    p_seq = sub.add_parser("sequences")
    p_seq.add_argument("--archive", required=True, type=Path)
    source(p_seq)
    source_file(p_seq, sha256_required=False)
    p_unr = sub.add_parser("unreviewed", help="decompressed TrEMBL FASTA on stdin")
    p_unr.add_argument("--members", required=True, type=Path)
    source(p_unr)
    p_unr.add_argument("--source-url", required=True, help="the knowledgebase archive's URL")
    p_unr.add_argument("--source-md5", required=True, help="the md5 UniProt publishes for it")
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
    if args.pass_name == "unreviewed":
        record = unreviewed_pass(
            sys.stdin.buffer,
            members_path=args.members,
            source=src,
            source_file={"url": args.source_url, "md5": args.source_md5.strip().lower()},
        )
        print(json.dumps(record, sort_keys=True))
        return 0
    declared = {"url": args.source_url, "sha256": args.source_sha256.strip().lower()}
    if args.pass_name == "members":
        record = members_pass(
            _stdin_lines(),
            superset_table=args.superset_table,
            neighbours_path=args.neighbours,
            source=src,
            source_file=declared,
        )
    elif args.pass_name == "rows":
        record = rows_pass(
            _stdin_lines(), members_path=args.members, source=src, source_file=declared
        )
    else:
        record = sequences_pass(
            args.archive, src, source_url=declared["url"], expected_sha256=declared["sha256"]
        )
    print(json.dumps(record, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
