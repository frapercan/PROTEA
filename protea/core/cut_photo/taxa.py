"""The organism of each member of a photo, read from its own GOA rows.

A GOA row names the protein's taxon in column 13 (``taxon:9606``). When the row
is about an interaction with another organism a second one follows a bar
(``taxon:9606|taxon:562``), and the protein's own is the first.

- A member whose rows name two own taxa is left without one and counted, never
  guessed.
- A member without rows at the cut (a Swiss-Prot entry nothing annotates yet)
  has none.

The taxon is dated with the cut: it comes from the GOA release of the photo,
not from today's UniProt.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

#: Members named in the counts when their rows disagree on their own taxon, at most.
_EXAMPLES = 20
_PREFIX = "taxon:"


def own_taxon(text: str | None) -> int | None:
    """The protein's own NCBI taxon in a GOA taxon field, or None when it reads as none."""
    if not text:
        return None
    first = text.split("|", 1)[0].strip()
    if not first.startswith(_PREFIX):
        return None
    try:
        return int(first[len(_PREFIX) :])
    except ValueError:
        return None


def member_taxa(
    rows_path: Path, batch_rows: int = 1_000_000
) -> tuple[dict[str, int], dict[str, Any]]:
    """``accession -> own taxon`` over a photo's ``rows.parquet``, and what the read counted."""
    taxa: dict[str, int] = {}
    disagree: set[str] = set()
    unreadable = 0
    handle = pq.ParquetFile(rows_path)
    for batch in handle.iter_batches(batch_size=batch_rows, columns=["accession", "taxon"]):
        accessions = batch.column("accession").to_pylist()
        for accession, text in zip(accessions, batch.column("taxon").to_pylist(), strict=True):
            taxon = own_taxon(text)
            if taxon is None:
                unreadable += 1
                continue
            if taxa.setdefault(accession, taxon) != taxon:
                disagree.add(accession)
    for accession in disagree:
        taxa.pop(accession, None)
    return taxa, {
        "members_with_taxon": len(taxa),
        "members_with_two_own_taxa": len(disagree),
        "members_with_two_own_taxa_examples": sorted(disagree)[:_EXAMPLES],
        "rows_without_a_readable_taxon": unreadable,
    }
