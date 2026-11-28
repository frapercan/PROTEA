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

A truth-only photo (``neighbours_path`` None) has the first three sets and no
neighbours: the photo of a cut that only ends a window, from which a method takes
what each target gained and whether it is still reviewed, never what it predicts
from (the author, 2026-11-16, for cut 205).

The sets a member entered by are its admission flags, and two pure readings of them
live here too: a :class:`FlagRule` chooses members by flag (``any_of``, ``all_of``,
``none_of``), which is how a declared set of photo members and every breadth level of
a donor pool are written, and :func:`admission_route` gives a member the one route
its first flag names, in the order truth, superset, curated, neighbour.
"""

from __future__ import annotations

import hashlib
import itertools
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
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
    neighbours_path: Path | None,
    source: PassSource,
    source_file: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Write ``members.parquet``: every member of the photo, with its sets and cluster.

    ``neighbours_path`` is the cluster table the ``neighbours`` measure pass wrote
    (every member of a UniRef50 cluster that holds a superset protein), named by the
    record beside it; None builds a truth-only photo. ``source_file`` is the GOA file
    the lines were decompressed from, as the caller declares it (``url`` and
    ``sha256``); the manifest refuses a photo without it.
    """
    source.check("GOA")
    superset, superset_source = _superset(superset_table, source)
    cluster_of: dict[str, str] = {}
    uniref: dict[str, Any] = {}
    if neighbours_path is not None:
        named = upstream_source(neighbours_path.with_name("neighbours.json"), "UniRef", source)
        cluster_of = _read_clusters(neighbours_path, superset)
        uniref = {"uniref_source": {**named, "sha256": _file_sha256(neighbours_path)}}
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
        "kind": "full" if neighbours_path is not None else "truth_only",
        "members": len(base) + len(neighbours),
        "superset": len(superset),
        "by_truth": len(truth),
        "by_curated_only": len(curated - truth),
        "by_neighbour": len(neighbours),
        "outside_superset": len(base - set(superset)) + len(neighbours),
        "clusters_with_superset": len({cluster_of[a] for a in superset if a in cluster_of}),
        "cluster_members_outside_base": len(cluster_of.keys() - base),
        "superset_source": superset_source,
        **uniref,
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


#: The four sets a photo member can have entered by (the flag columns of
#: ``cut_photo_member``), in the order its admission route reads them.
ADMISSION_FLAGS: tuple[str, ...] = ("truth", "superset", "curated", "neighbour")

#: The route each flag names when it is a member's first: experimental, then
#: reviewed, then curated inference, then the UniRef50 neighbours.
ADMISSION_ROUTES: dict[str, str] = {
    "truth": "experimental",
    "superset": "reviewed",
    "curated": "curated_inference",
    "neighbour": "uniref_neighbour",
}

#: Every combination of flags a member can carry, as the members pass writes them:
#: at least one set, and a neighbour by that set alone.
MEMBER_FLAG_COMBINATIONS: tuple[tuple[bool, ...], ...] = tuple(
    combination
    for combination in itertools.product((False, True), repeat=len(ADMISSION_FLAGS))
    if any(combination[:3]) != combination[3]
)


def _flags_of(flags: Mapping[str, Any]) -> tuple[bool, ...]:
    """The four flags of a member, in the order of :data:`ADMISSION_FLAGS`."""
    missing = [flag for flag in ADMISSION_FLAGS if flag not in flags]
    if missing:
        raise ValueError(f"a member's flags name all four sets; {missing} missing")
    return tuple(bool(flags[flag]) for flag in ADMISSION_FLAGS)


def admission_route(flags: Mapping[str, Any]) -> str:
    """The route of a member: the one its first flag names, in the order truth, superset,
    curated, neighbour.

    Every member has exactly one, since it entered by at least one set; flags that name
    no set are refused.
    """
    for flag, carried in zip(ADMISSION_FLAGS, _flags_of(flags), strict=True):
        if carried:
            return ADMISSION_ROUTES[flag]
    raise ValueError("a member that entered by no set has no route, and no photo holds one")


def _flag_list(name: str, values: Iterable[str]) -> tuple[str, ...]:
    """The flags of one list of a rule, in the order of :data:`ADMISSION_FLAGS`."""
    given = list(values)
    unknown = sorted(set(given) - set(ADMISSION_FLAGS))
    if unknown:
        raise ValueError(f"{name} names {unknown}: the admission flags are {ADMISSION_FLAGS}")
    if len(set(given)) != len(given):
        raise ValueError(f"{name} names a flag twice: {given}")
    return tuple(flag for flag in ADMISSION_FLAGS if flag in given)


@dataclass(frozen=True)
class FlagRule:
    """Photo members chosen by their admission flags.

    A member is admitted when it carries at least one flag of ``any_of`` (no condition
    when it is empty), every flag of ``all_of`` and no flag of ``none_of``. The flags
    are kept in the order of :data:`ADMISSION_FLAGS`, so two rules naming the same flags
    in another order are one rule. A flag both required and excluded is refused, and so
    is a rule no member could ever satisfy.
    """

    any_of: tuple[str, ...] = ()
    all_of: tuple[str, ...] = ()
    none_of: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("any_of", "all_of", "none_of"):
            object.__setattr__(self, name, _flag_list(name, getattr(self, name)))
        contradicted = sorted(set(self.none_of) & (set(self.any_of) | set(self.all_of)))
        if contradicted:
            raise ValueError(f"none_of excludes {contradicted}, which the rule also asks for")
        if not self.admitted_combinations():
            raise ValueError(
                f"no photo member can satisfy {self.in_words()}: a neighbour entered by no "
                "other set, and every member entered by at least one"
            )

    def admits(self, flags: Mapping[str, Any]) -> bool:
        """Whether a member with these flags is in the set."""
        carried = {flag: on for flag, on in zip(ADMISSION_FLAGS, _flags_of(flags), strict=True)}
        return (
            (not self.any_of or any(carried[flag] for flag in self.any_of))
            and all(carried[flag] for flag in self.all_of)
            and not any(carried[flag] for flag in self.none_of)
        )

    def admitted_combinations(self) -> frozenset[tuple[bool, ...]]:
        """The combinations of :data:`MEMBER_FLAG_COMBINATIONS` the rule admits.

        Two rules that admit the same combinations choose the same members of any photo.
        """
        return frozenset(
            combination
            for combination in MEMBER_FLAG_COMBINATIONS
            if self.admits(
                {flag: on for flag, on in zip(ADMISSION_FLAGS, combination, strict=True)}
            )
        )

    @property
    def admits_every_member(self) -> bool:
        """Whether the rule chooses the whole photo, whatever its spelling."""
        return self.admitted_combinations() == frozenset(MEMBER_FLAG_COMBINATIONS)

    def in_words(self) -> str:
        """The rule as a phrase a description can carry."""
        if not (self.any_of or self.all_of or self.none_of):
            return "every member"
        parts = []
        if self.any_of:
            parts.append("entered by " + " or ".join(self.any_of))
        if self.all_of:
            parts.append("entered by " + " and ".join(self.all_of))
        if self.none_of:
            parts.append("not entered by " + " or ".join(self.none_of))
        return "the members " + ", ".join(parts)

    def declared(self) -> dict[str, list[str]]:
        """The rule as a job result records it."""
        return {
            "any_of": list(self.any_of),
            "all_of": list(self.all_of),
            "none_of": list(self.none_of),
        }


#: The cumulative breadth levels of a donor pool, by the label set names and
#: descriptions carry: experimental (truth), experimental or reviewed (truth or
#: superset), plus curated inference, and every member of the photo.
BREADTH_LEVELS: dict[str, FlagRule] = {
    "experimental": FlagRule(any_of=("truth",)),
    "experimental-or-reviewed": FlagRule(any_of=("truth", "superset")),
    "plus-curated-inference": FlagRule(any_of=("truth", "superset", "curated")),
    "every-member": FlagRule(),
}


def breadth_level_of(rule: FlagRule) -> str | None:
    """The breadth level whose members a rule chooses, or None when it chooses none of them."""
    chosen = rule.admitted_combinations()
    for label, level in BREADTH_LEVELS.items():
        if level.admitted_combinations() == chosen:
            return label
    return None
