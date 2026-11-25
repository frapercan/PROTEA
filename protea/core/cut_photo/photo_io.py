"""What the photo's passes share (ADR-D59): the GAF reader, the records of the
measured inputs, the member and cluster tables, and the channel table's digest.

Nothing here is a pass; ``protea.core.cut_photo.build`` runs them and writes the
manifest.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Collection, Iterable
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from protea.core.cut_photo.channels import ELECTRONIC_REFERENCES
from protea.core.cut_photo.formats import EMPTY_GO_REF_BY_WITH_FROM, GAF_ASSIGNED_BY
from protea.core.cut_photo.measure import PassSource, PhotoGuardError
from protea.core.row_alignment import lookup_by

_GAF_PREFIX = "UniProtKB\t"


def channel_table_digest() -> str:
    """The sha256 of the declared electronic reference table, recorded per photo.

    It covers the reading of an empty ``GO_REF`` through its with/from too, since
    that rule decides a channel as much as the table does.
    """
    declared = {
        "references": sorted(ELECTRONIC_REFERENCES.items()),
        "empty_go_ref_by_with_from": sorted(EMPTY_GO_REF_BY_WITH_FROM.items()),
    }
    blob = json.dumps(declared, sort_keys=True).encode()
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


#: A GAF header field: ``!go-version: <url>``, ``!date-generated: ...``. GOA's GAF 2.1
#: files capitalise their keys (``!GO-version:``, ``!Generated:``), so a key is matched
#: in any case and stored in lower case: a case-sensitive match read the 2.1 headers of
#: cuts 160, 177 and 194 as declaring no GO release, which they all do.
_HEADER_FIELD = re.compile(r"^!([A-Za-z][A-Za-z-]*):\s*(.*?)\s*$")

#: The GO release a GAF says it was built against, inside its ``go-version`` URL.
_DECLARED_GO_RELEASE = re.compile(r"releases/(\d{4}-\d{2}-\d{2})")


def _gaf_columns(lines: Iterable[str], header: dict[str, str] | None = None) -> Iterable[list[str]]:
    """The columns of every UniProtKB row; ``header`` collects the ``!key: value`` lines.

    Only lines that are not UniProtKB rows are looked at for the header, so the
    rows pay one ``startswith`` each, as before.
    """
    for line in lines:
        if line.startswith(_GAF_PREFIX):
            cols = line.rstrip("\n").split("\t")
            if len(cols) > GAF_ASSIGNED_BY:
                yield cols
        elif header is not None and line.startswith("!"):
            field = _HEADER_FIELD.match(line)
            if field:
                header.setdefault(field.group(1).lower(), field.group(2))


def declared_go_release(header: dict[str, str]) -> str:
    """The GO release date the GAF declares (``YYYY-MM-DD``), or ``""`` when it says none."""
    found = _DECLARED_GO_RELEASE.search(header.get("go-version", ""))
    return found.group(1) if found else ""


def go_release_rule(header: dict[str, str]) -> str:
    """Which rule picks the photo's GO graph (ADR-D59, amended 2026-11-16).

    ``declared``: the newest loaded graph not after the GO release the GAF declares.
    ``cut_date``: the GAF declares no GO release at all, so the newest loaded graph
    not after the cut's date. GOA's GAF 2.1 files do declare one, capitalised
    (``!GO-version:``): cut 160 declares 2016-10-29, the graph ADR-D60 gives it.
    ``unreadable``: a ``go-version`` line that names no release, which refuses the
    photo.
    """
    if "go-version" not in header:
        return "cut_date"
    return "declared" if declared_go_release(header) else "unreadable"


def _read_members(path: Path) -> dict[str, bool]:
    table = pq.read_table(path, columns=["accession", "superset"])
    pairs = zip(
        table.column("accession").to_pylist(), table.column("superset").to_pylist(), strict=True
    )
    by_accession = lookup_by(pairs, lambda pair: pair[0], context=f"{path.name} accessions")
    return {accession: bool(pair[1]) for accession, pair in by_accession.items()}


def _read_clusters(path: Path, superset: Collection[str]) -> dict[str, str]:
    """The UniRef50 cluster of every accession in a cluster that holds a superset protein.

    UniRef puts an accession in one cluster, so an accession listed twice is
    refused rather than kept with its last cluster; and a protein the table
    marks as superset must be in this photo's superset, or the table was
    measured against another release.
    """
    table = pq.read_table(path, columns=["cluster", "accession", "in_superset"])
    cluster_of: dict[str, str] = {}
    for cluster, accession, in_superset in zip(
        table.column("cluster").to_pylist(),
        table.column("accession").to_pylist(),
        table.column("in_superset").to_pylist(),
        strict=True,
    ):
        if accession in cluster_of:
            raise PhotoGuardError(f"{path.name} lists {accession} in more than one cluster")
        if in_superset and accession not in superset:
            raise PhotoGuardError(
                f"{path.name} marks {accession} as Swiss-Prot, this photo's superset does not"
            )
        cluster_of[accession] = cluster
    return cluster_of
