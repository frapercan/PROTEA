"""The sequence passes of ADR-D59's photo (decision 5): every member's sequence
from the paired UniProt release.

``sequences``
    Every superset member's, from the release's Swiss-Prot FASTA.
``unreviewed``
    Every other member's (the accessions GOA admits outside Swiss-Prot and the
    annotated neighbours), from the release's TrEMBL, which UniProt publishes for a
    past release only as a flat file inside its full knowledgebase archive
    (``uniprot_trembl.dat.gz``); the job streams it in.
"""

from __future__ import annotations

import gzip
import hashlib
import tarfile
from pathlib import Path
from typing import Any, BinaryIO

import pyarrow as pa
import pyarrow.parquet as pq

from protea.core.cut_photo.measure import (
    SWISSPROT_FASTA_MEMBER,
    PassSource,
    PhotoGuardError,
    _file_sha256,
    _write_record,
)
from protea.core.cut_photo.photo_io import _read_members
from protea.core.swissprot_table import (
    canonical_table,
    iter_flat_file_sequences,
    iter_swissprot_records,
)

#: Written beside the photo by the job that streams the paired release's knowledgebase
#: archive into the ``unreviewed`` pass: the md5 of that stream, which the manifest
#: checks against the md5 UniProt publishes for the archive.
KNOWLEDGEBASE_STREAM_MD5 = "knowledgebase-stream.md5"

#: How many accessions the paired release does not hold a record names.
MISSING_SHOWN = 20


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


def unreviewed_pass(
    stream: BinaryIO,
    *,
    members_path: Path,
    source: PassSource,
    source_file: dict[str, str],
) -> dict[str, Any]:
    """Write ``unreviewed_sequences.parquet``: the paired release's sequence of every
    member outside the superset (ADR-D59, decision 5).

    ``stream`` is the paired release's TrEMBL flat file (``uniprot_trembl.dat``),
    decompressed: a past release publishes TrEMBL in no other form. The job streams it
    out of the release's knowledgebase archive and takes the archive's md5 while it
    reads (``KNOWLEDGEBASE_STREAM_MD5``); the record declares the archive by URL and by
    the md5 UniProt publishes, and the manifest refuses the photo unless the two agree.
    A member the release does not hold is counted and named, never guessed: a GOA
    release can cite an accession created after its paired UniProt release.
    """
    source.check("UniProt")
    members = _read_members(members_path)
    wanted = {accession for accession, in_superset in members.items() if not in_superset}
    found: dict[str, tuple[str, str]] = {}
    keys = frozenset(accession.encode() for accession in wanted)
    for accession, digest, sequence in iter_flat_file_sequences(stream, keys):
        if accession in found:
            raise PhotoGuardError(f"{accession} appears twice in the TrEMBL stream")
        found[accession] = (digest, sequence)
    ordered = sorted(found)
    table = pa.table(
        {
            "accession": ordered,
            "md5": [found[a][0] for a in ordered],
            "sequence": [found[a][1] for a in ordered],
        }
    )
    source.out.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, source.out / "unreviewed_sequences.parquet", compression="zstd")
    missing = sorted(wanted - found.keys())
    record = {
        **source.head("unreviewed"),
        "members_outside_superset": len(wanted),
        "sequences": len(found),
        "missing": len(missing),
        "missing_examples": missing[:MISSING_SHOWN],
        "source_file": {"url": source_file.get("url", ""), "md5": source_file.get("md5", "")},
    }
    _write_record(source.out / "unreviewed.json", record)
    return record
