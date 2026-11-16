"""The members pass of ADR-D59's photo: who the corpus at the cut is (decision 1).

Four sets, each from a source dated on or before the cut:

``superset``
    The paired release's Swiss-Prot, from the canonical table the ``superset``
    measure pass wrote.
``truth``, ``curated``
    Every accession GOA t admits by ADR-D49's codes. A ``NOT`` row admits; the
    qualifier is never read.
``neighbour``
    A member of a UniRef50 cluster that holds a superset protein, in no other set,
    that carries at least one GOA t row in a channel of
    :data:`protea.core.cut_photo.channels.NEIGHBOUR_CHANNELS` (PAINT's or electronic
    orthology). The author, 2026-11-16: the corpus is fed with interesting homologs
    with their annotations, all their GOA t rows kept.

Each member is written with the sets it entered by, its UniRef50 cluster when it is
in one, and the md5 of its Swiss-Prot sequence when it is in the superset. GOA on
stdin.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from protea.core.cut_photo.channels import (
    ELECTRONIC_REFERENCES,
    NEIGHBOUR_CHANNELS,
    PHYLOGENETIC_CODES,
    UnknownEvidence,
    channel_of,
)
from protea.core.cut_photo.formats import (
    AUTOMATIC_CODE,
    CURATED_INFERENCE_CODES,
    GAF_EVIDENCE,
    GAF_ID,
    GAF_REFERENCE,
    GAF_WITH_FROM,
    TRUTH_CODES,
    reference_class,
)
from protea.core.cut_photo.measure import PassSource, PhotoGuardError, _file_sha256, _write_record
from protea.core.cut_photo.photo_io import _gaf_columns, _read_clusters, upstream_source
from protea.core.swissprot_table import canonical_table, read_local_table

#: The electronic references whose channel is orthology. A row that names none of them
#: cannot admit a neighbour, so the hundreds of millions of signature rows a GOA release
#: holds for neighbours are dropped on a substring test before any is classified.
_ORTHOLOGY_REFERENCES = tuple(
    reference for reference, (kind, _title) in ELECTRONIC_REFERENCES.items() if kind == "orthology"
)


def admits_a_neighbour(cols: list[str]) -> bool:
    """True when this GOA row puts a neighbour in a channel that admits it.

    Read exactly as the rows pass reads it (``channel_of`` on the row's reference
    class, the protein not reviewed); the codes and the substring test only skip
    rows whose channel cannot be one of :data:`NEIGHBOUR_CHANNELS`.
    """
    code, reference = cols[GAF_EVIDENCE], cols[GAF_REFERENCE]
    if code not in PHYLOGENETIC_CODES and not (
        code == AUTOMATIC_CODE and any(ref in reference for ref in _ORTHOLOGY_REFERENCES)
    ):
        return False
    try:
        channel = channel_of(code, reference_class(reference, cols[GAF_WITH_FROM]), False)
    except UnknownEvidence:
        return False
    return channel in NEIGHBOUR_CHANNELS


def _superset(table_path: Path, source: PassSource) -> tuple[dict[str, str], dict[str, Any]]:
    """The superset (accession to md5) and its source, checked against its record."""
    named = upstream_source(table_path.with_name("superset.json"), "UniProt", source)
    superset = read_local_table(str(table_path))
    digest = hashlib.sha256(canonical_table(superset)).hexdigest()
    recorded = json.loads(table_path.with_name("superset.json").read_text(encoding="utf-8"))
    if recorded.get("sha256") != digest:
        raise PhotoGuardError(
            f"{table_path} hashes to {digest}, its record says {recorded.get('sha256')}"
        )
    return superset, {**named, "sha256": digest}


def members_pass(
    lines: Iterable[str],
    *,
    superset_table: Path,
    neighbours_path: Path,
    source: PassSource,
    source_file: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Write ``members.parquet``: every member of the photo, with its sets and cluster.

    ``neighbours_path`` is the cluster table the ``neighbours`` measure pass wrote
    (every member of a UniRef50 cluster that holds a superset protein), named by the
    record beside it. ``source_file`` is the GOA file the lines were decompressed
    from, as the caller declares it (``url`` and ``sha256``); the manifest refuses a
    photo without it.
    """
    source.check("GOA")
    superset, superset_source = _superset(superset_table, source)
    uniref = upstream_source(neighbours_path.with_name("neighbours.json"), "UniRef", source)
    cluster_of = _read_clusters(neighbours_path, superset)
    truth: set[str] = set()
    curated: set[str] = set()
    annotated: set[str] = set()
    header: dict[str, str] = {}
    for cols in _gaf_columns(lines, header):
        accession, code = cols[GAF_ID], cols[GAF_EVIDENCE]
        if code in TRUTH_CODES:
            truth.add(accession)
        elif code in CURATED_INFERENCE_CODES:
            curated.add(accession)
        elif accession in cluster_of and accession not in superset and admits_a_neighbour(cols):
            annotated.add(accession)
    base = set(superset) | truth | curated
    neighbours = annotated - base
    sets = {"truth": truth, "curated": curated, "neighbour": neighbours}
    _write_members(source.out, sorted(base | neighbours), superset, sets, cluster_of)
    record = {
        **source.head("members"),
        "members": len(base) + len(neighbours),
        "superset": len(superset),
        "by_truth": len(truth),
        "by_curated_only": len(curated - truth),
        "by_neighbour": len(neighbours),
        "outside_superset": len(base - set(superset)) + len(neighbours),
        "clusters_with_superset": len({cluster_of[a] for a in superset if a in cluster_of}),
        "cluster_members_outside_base": len(cluster_of.keys() - base),
        "superset_source": superset_source,
        "uniref_source": {**uniref, "sha256": _file_sha256(neighbours_path)},
        "source_file": dict(source_file or {}),
        "gaf_header": header,
    }
    _write_record(source.out / "members.json", record)
    return record


def _write_members(
    out: Path,
    accessions: list[str],
    superset: dict[str, str],
    sets: dict[str, set[str]],
    cluster_of: dict[str, str],
) -> None:
    """One row per member: its sets (``superset`` and the three in ``sets``), cluster, md5."""
    table = pa.table(
        {
            "accession": accessions,
            "superset": [a in superset for a in accessions],
            **{name: [a in members for a in accessions] for name, members in sets.items()},
            "cluster": [cluster_of.get(a) for a in accessions],
            "md5": [superset.get(a) for a in accessions],
        }
    )
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out / "members.parquet", compression="zstd")
