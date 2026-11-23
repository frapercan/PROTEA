"""The population a prediction set was asked about, recorded by its digest.

A set names the proteins it predicted for in one of two ways: a query set
(``query_set_id``), or a list handed to the run that wrote it, which is how a
window's arms ask about that window's targets. The list itself stays in the
job's payload; the set records its size and its digest, so two sets asked about
the same proteins share a value a comparison can check
(``protea.core.method_seal``), and a set asked about other proteins does not.

The digest is the sha256 of the distinct accessions, sorted, one per line with
its newline: the bytes of a plain sorted list file, so a list kept on disk can
be checked against a set with ``sha256sum``.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import Any


def population_of(accessions: Iterable[str]) -> dict[str, Any]:
    """``{"proteins": n, "sha256": hex}`` of the distinct accessions."""
    distinct = sorted(set(accessions))
    body = "".join(f"{accession}\n" for accession in distinct).encode()
    return {"proteins": len(distinct), "sha256": hashlib.sha256(body).hexdigest()}
