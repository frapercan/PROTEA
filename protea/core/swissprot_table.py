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
from typing import Any


def canonical_table(members: dict[str, str]) -> bytes:
    """The bytes a release resolves to, whatever route produced it.

    One ``"<accession>\\t<md5>\\n"`` line per entry, sorted by accession. The
    sort is what makes the digest a property of the release rather than of the
    order a file happened to be written in.
    """
    return "".join(f"{acc}\t{members[acc]}\n" for acc in sorted(members)).encode()


def parse_swissprot_fasta(lines: Any) -> dict[str, str]:
    """Accession to md5 of its sequence, from a Swiss-Prot FASTA stream.

    ``lines`` yields ``bytes``. The header is ``>sp|<accession>|<ENTRY_NAME>``
    and the md5 is taken over the sequence lines joined with nothing, each
    stripped, encoded UTF-8 -- the same convention the reference tables use, so
    the digests are comparable.

    :raises ValueError: on a record with no sequence, or a duplicated
        accession. Both would otherwise make the count silently wrong.
    """
    members: dict[str, str] = {}
    accession: str | None = None
    chunks: list[str] = []

    def flush() -> None:
        if accession is None:
            return
        if not chunks:
            raise ValueError(f"{accession} has a header but no sequence")
        # md5 is UniProt's own sequence fingerprint, not a security primitive:
        # the digests have to match the ones the reference tables carry, so the
        # algorithm is fixed by the data and not a choice made here.
        digest = hashlib.md5("".join(chunks).encode(), usedforsecurity=False).hexdigest()
        if accession in members:
            raise ValueError(f"{accession} appears twice in the FASTA")
        members[accession] = digest

    for raw in lines:
        line = raw.decode("utf-8", "strict").strip()
        if not line:
            continue
        if line.startswith(">"):
            flush()
            parts = line.split("|")
            if len(parts) < 3:
                raise ValueError(f"unparseable FASTA header: {line[:80]!r}")
            accession = parts[1]
            chunks = []
        else:
            chunks.append(line)
    flush()
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
