"""Wire formats behind a cut photo (ADR-D59).

Three formats, read as text lines so the builder can sit at the end of a shell
pipe that decompresses or unpacks its source:

- the GAF row of a GOA release;
- the UniRef XML, pre-filtered to the three kinds of line membership needs;
- the reference field of a GAF row, reduced to the ``GO_REF`` that names the
  pipeline or method behind the annotation.

Standard library plus ``protea.core.evidence_codes`` only.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator

from protea.core.evidence_codes import EXPERIMENTAL

#: 0-indexed GAF 2.x columns. ``extract_goa_universe`` reads the accession, the
#: evidence code and the object type at the same indices, and
#: ``tests/test_cut_photo_formats.py`` pins the three against its constants.
GAF_DB = 0
GAF_ID = 1
GAF_QUALIFIER = 3
GAF_GO = 4
GAF_REFERENCE = 5
GAF_EVIDENCE = 6
GAF_WITH_FROM = 7
GAF_ASPECT = 8
GAF_TYPE = 11
GAF_TAXON = 12
GAF_DATE = 13
GAF_ASSIGNED_BY = 14

#: ADR-D49's first tier. It IS the platform's set, imported rather than restated.
TRUTH_CODES: frozenset[str] = EXPERIMENTAL

#: ADR-D49's second tier. Restated because its home module
#: (``_universe_sources``) imports the database layer; a test pins the two equal.
CURATED_INFERENCE_CODES: frozenset[str] = frozenset(
    {"ISS", "ISO", "ISA", "ISM", "IGC", "RCA", "NAS", "IKR", "IRD"}
)

#: The codes ADR-D49 does not admit by, each for its own reason. A photo keeps
#: their rows for its members; only ``ND`` stays unused.
AUTOMATIC_CODE = "IEA"
PROPAGATED_CODES: frozenset[str] = frozenset({"IBA", "IBD"})
NO_DATA_CODE = "ND"

#: Every code the partition knows. A row with any other code is counted under
#: its own name and reported, never folded into a known one.
KNOWN_CODES: frozenset[str] = (
    TRUTH_CODES
    | CURATED_INFERENCE_CODES
    | PROPAGATED_CODES
    | frozenset({AUTOMATIC_CODE, NO_DATA_CODE})
)

_GO_REF = re.compile(r"GO_REF:\d{7}")


def reference_class(field: str) -> str:
    """Name the source behind a GAF reference field.

    A ``GO_REF`` names a pipeline or a curation method, so it is returned as is
    (the first one when the field lists several). A citation of a publication
    (``PMID``, ``DOI``) is ``"publication"``; anything else is ``"other"``. That
    keeps the inventory small without hiding any pipeline, because every
    electronic annotation cites a ``GO_REF``.
    """
    match = _GO_REF.search(field)
    if match:
        return match.group(0)
    for part in field.split("|"):
        if part.startswith(("PMID:", "DOI:")):
            return "publication"
    return "other"


def is_negated(qualifier: str) -> bool:
    """True when a GAF qualifier holds ``NOT``."""
    return "NOT" in qualifier.split("|")


def iter_gaf_columns(lines: Iterable[str]) -> Iterator[list[str]]:
    """The columns of every UniProtKB data row of a GAF.

    Comments, blank lines and rows of other databases (ComplexPortal,
    RNAcentral) are skipped; a row too short to hold ``assigned_by`` is skipped
    too, because none of its channel fields could be read.
    """
    for line in lines:
        if not line or line[0] == "!":
            continue
        cols = line.rstrip("\n").split("\t")
        if len(cols) <= GAF_ASSIGNED_BY or cols[GAF_DB] != "UniProtKB":
            continue
        yield cols


#: The ``grep -E`` pattern that keeps exactly the lines
#: :func:`iter_uniref_clusters` reads. UniProt writes the XML one element per
#: line at column 0, so the anchors hold.
UNIREF_LINE_FILTER = r'^<entry id=|^<dbReference |type="UniProtKB accession"'


def iter_uniref_clusters(lines: Iterable[str]) -> Iterator[tuple[str, list[str]]]:
    """``(cluster, accessions)`` for every UniRef entry, the representative included.

    A member is a ``dbReference`` of type ``UniProtKB ID``, and only its FIRST
    ``UniProtKB accession`` is taken: the following ones are secondary
    accessions of the same entry, and counting them would inflate membership.
    A UniParc member carries no UniProtKB accession and is skipped by
    construction. A cluster ends where the next ``<entry`` starts, so the
    closing tags need not survive the pre-filter.
    """
    cluster: str | None = None
    members: list[str] = []
    want = False
    for line in lines:
        text = line.lstrip()
        if text.startswith("<entry "):
            if cluster is not None:
                yield cluster, members
            cluster = _attribute(text, "id")
            members = []
            want = False
        elif text.startswith("<dbReference "):
            want = 'type="UniProtKB ID"' in text
        elif want and 'type="UniProtKB accession"' in text:
            members.append(_attribute(text, "value"))
            want = False
    if cluster is not None:
        yield cluster, members


def _attribute(line: str, name: str) -> str:
    """The value of one double-quoted attribute; raises when it is absent."""
    key = f'{name}="'
    start = line.index(key) + len(key)
    return line[start : line.index('"', start)]
