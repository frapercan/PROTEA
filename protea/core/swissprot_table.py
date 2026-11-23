"""Swiss-Prot release tables: the pure half of ``load_swissprot_release``.

Split out of :mod:`protea.core.operations.load_swissprot_release` so that code
running away from the server can produce the same bytes the loader verifies.
ADR-D59's photo builder runs on a compute node with no database stack, and it
must emit a Swiss-Prot table whose canonical digest the loader will accept: one
parser and one canonical form, imported by both, is what makes "the node's
table" and "the archive the server streams" the same claim instead of two
claims that happen to agree. Standard library only, on purpose.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from typing import Any


def canonical_table(members: dict[str, str]) -> bytes:
    """The bytes a release resolves to, whatever route produced it.

    One ``"<accession>\\t<md5>\\n"`` line per entry, sorted by accession. The
    sort is what makes the digest a property of the release rather than of the
    order a file happened to be written in.
    """
    return "".join(f"{acc}\t{members[acc]}\n" for acc in sorted(members)).encode()


def iter_swissprot_records(lines: Any) -> Iterator[tuple[str, str, str]]:
    """``(accession, md5, sequence)`` for each record of a Swiss-Prot FASTA stream.

    ``lines`` yields ``bytes``. The header is ``>sp|<accession>|<ENTRY_NAME>``
    and the md5 is taken over the sequence lines joined with nothing, each
    stripped, encoded UTF-8 -- the same convention the reference tables use, so
    the digests are comparable. This is the one parser: the loader's table and a
    photo's sequences are both read through it.

    :raises ValueError: on a record with no sequence or an unparseable header.
    """
    accession: str | None = None
    chunks: list[str] = []

    def record() -> tuple[str, str, str]:
        if not chunks:
            raise ValueError(f"{accession} has a header but no sequence")
        sequence = "".join(chunks)
        # md5 is UniProt's own sequence fingerprint, not a security primitive:
        # the digests have to match the ones the reference tables carry, so the
        # algorithm is fixed by the data and not a choice made here.
        digest = hashlib.md5(sequence.encode(), usedforsecurity=False).hexdigest()
        return str(accession), digest, sequence

    for raw in lines:
        line = raw.decode("utf-8", "strict").strip()
        if not line:
            continue
        if line.startswith(">"):
            if accession is not None:
                yield record()
            parts = line.split("|")
            if len(parts) < 3:
                raise ValueError(f"unparseable FASTA header: {line[:80]!r}")
            accession = parts[1]
            chunks = []
        else:
            chunks.append(line)
    if accession is not None:
        yield record()


#: The line that ends every entry of a UniProt flat file, with the newlines around it.
_FLAT_FILE_END = b"\n//\n"
#: Bytes read at a time from a flat-file stream.
_FLAT_FILE_CHUNK = 1 << 26


def iter_flat_file_sequences(
    stream: Any, wanted: frozenset[bytes]
) -> Iterator[tuple[str, str, str]]:
    """``(accession, md5, sequence)`` of the wanted entries of a UniProt flat file stream.

    ``stream`` is a binary stream of a decompressed ``uniprot_*.dat`` (anything with
    ``read``), the only form in which UniProt publishes a past release's TrEMBL. An
    entry's accession is the first one on its first ``AC`` line; its sequence is the
    lines after ``SQ`` up to ``//``, whitespace removed, and the md5 is taken as
    :func:`iter_swissprot_records` takes it, so the two formats give the same digest
    for the same sequence. Entries are cut on the ``//`` line and kept or dropped on
    their accession while still bytes, because a release holds hundreds of millions
    of them and a photo wants a few hundred thousand.

    :raises ValueError: on a wanted entry with no ``SQ`` line or no sequence.
    """
    tail = b""
    while chunk := stream.read(_FLAT_FILE_CHUNK):
        data = tail + chunk
        cut = data.rfind(_FLAT_FILE_END)
        if cut < 0:
            tail = data
            continue
        yield from _wanted_entries(data[:cut], wanted)
        tail = data[cut + len(_FLAT_FILE_END) :]
    if tail.strip():
        yield from _wanted_entries(tail.rstrip().removesuffix(b"\n//"), wanted)


def _wanted_entries(block: bytes, wanted: frozenset[bytes]) -> Iterator[tuple[str, str, str]]:
    for entry in block.split(_FLAT_FILE_END):
        start = entry.find(b"\nAC   ")
        if start < 0:
            continue
        end = entry.find(b";", start)
        accession = entry[start + 6 : end].strip()
        if accession not in wanted:
            continue
        sq = entry.find(b"\nSQ   ", end)
        lines = entry.find(b"\n", sq + 1) if sq >= 0 else -1
        sequence = entry[lines:].translate(None, b" \t\r\n").decode() if lines >= 0 else ""
        if not sequence:
            raise ValueError(f"{accession.decode()} has no sequence in the flat file")
        # md5 is UniProt's own sequence fingerprint, as in iter_swissprot_records.
        digest = hashlib.md5(sequence.encode(), usedforsecurity=False).hexdigest()
        yield accession.decode(), digest, sequence


def parse_swissprot_fasta(lines: Any) -> dict[str, str]:
    """Accession to md5 of its sequence, from a Swiss-Prot FASTA stream.

    :raises ValueError: on a record with no sequence, or a duplicated
        accession. Both would otherwise make the count silently wrong.
    """
    members: dict[str, str] = {}
    for accession, digest, _ in iter_swissprot_records(lines):
        if accession in members:
            raise ValueError(f"{accession} appears twice in the FASTA")
        members[accession] = digest
    return members


def read_local_table(path: str) -> dict[str, str]:
    """Accession to md5 from an already-extracted two-column table."""
    members: dict[str, str] = {}
    with open(path, encoding="utf-8") as handle:
        for number, raw in enumerate(handle, start=1):
            line = raw.rstrip("\n")
            if not line:
                continue
            accession, _, digest = line.partition("\t")
            if not accession or len(digest) != 32:
                raise ValueError(
                    f"{path}:{number}: expected 'accession<TAB>md5', got {line[:80]!r}"
                )
            if accession in members:
                raise ValueError(f"{path}:{number}: {accession} appears twice")
            members[accession] = digest
    return members
