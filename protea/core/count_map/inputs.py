"""What a count map reads at one cut, and how it becomes matrices (ADR-D65, decisions 1 and 4).

The donors are the ``truth`` members of the photo whose annotation set starts
the window, each with its state at the cut (``protea.core._cut_state``). The
targets are the window's own: the proteins its truth scores, or its declared
target set. Both are named by their sequence's md5 in the photo, which is how
the lexicon bank and the segment families name their units. Per aspect, the
columns are the pivot's terms of that aspect without its root, and a donor whose
state holds none of them is not a donor of the aspect.

What a protein's reading leaves out (:data:`EXCLUSIONS`): ``sequence``, the
donors that share its sequence (itself, its merged aliases and its identical
twins), as the laboratory read it; or ``accession``, the protein's own accession
only, the rule the platform's retrieval follows, under which a twin under
another accession stays a donor. The fit leaves out of each donor's own reading
the same group, so m is fitted for the reading it serves.
"""

from __future__ import annotations

import io
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, NamedTuple

import numpy as np
import pyarrow.parquet as pq
import scipy.sparse as sp
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core._artifact_identity import fetch_and_verify
from protea.core.count_map.reading import Targets

#: The three roots, never a column: every annotated protein holds its aspect's.
ROOTS = frozenset({"GO:0008150", "GO:0003674", "GO:0005575"})
ASPECTS = ("P", "F", "C")
#: What a protein's own reading leaves out: the donors sharing its sequence, or its accession.
EXCLUSIONS = ("sequence", "accession")


@dataclass(frozen=True)
class Columns:
    """One aspect's columns: the pivot's terms of the aspect without its root, sorted."""

    aspect: str
    terms: tuple[str, ...]

    def index(self) -> dict[str, int]:
        return {t: i for i, t in enumerate(self.terms)}


def columns_of(aspect_by_term: Mapping[str, str]) -> dict[str, Columns]:
    """Every aspect's columns from the pivot's aspect per term."""
    out = {}
    for aspect in ASPECTS:
        terms = sorted(t for t, a in aspect_by_term.items() if a == aspect and t not in ROOTS)
        out[aspect] = Columns(aspect, tuple(terms))
    return out


def member_md5s(
    session: Session, photo_id: uuid.UUID, accessions: Iterable[str] | None = None
) -> dict[str, str]:
    """accession -> sequence md5 of the photo's members: its truth members, or the ones named."""
    if accessions is None:
        rows = session.execute(
            text(
                "SELECT accession, sequence_md5 FROM cut_photo_member "
                "WHERE photo_id = :p AND truth AND sequence_md5 IS NOT NULL"
            ),
            {"p": photo_id},
        )
    else:
        rows = session.execute(
            text(
                "SELECT accession, sequence_md5 FROM cut_photo_member "
                "WHERE photo_id = :p AND accession = ANY(:a) AND sequence_md5 IS NOT NULL"
            ),
            {"p": photo_id, "a": sorted(set(accessions))},
        )
    return {str(a): str(m) for a, m in rows}


def theme_units(
    store: Any, shards: list[dict[str, Any]], wanted: set[str]
) -> dict[str, np.ndarray]:
    """md5 -> the sorted themes of its segments, read from the bank's verified cut shards."""
    out: dict[str, np.ndarray] = {}
    for shard in shards:
        if shard["pass"] != "cut":
            continue
        blob = fetch_and_verify(store, shard["uri"], shard["sha256"], f"cut shard {shard['shard']}")
        table = pq.read_table(io.BytesIO(blob), columns=["md5", "seg_theme"])
        md5s = table.column("md5").to_pylist()
        themes = table.column("seg_theme").to_pylist()
        for md5, row in zip(md5s, themes, strict=True):
            if md5 in wanted:
                out[md5] = np.unique(np.asarray(row, dtype=np.int64))
    return out


def family_units(
    store: Any, outputs: list[dict[str, Any]], scope: str, wanted: set[str]
) -> dict[str, np.ndarray]:
    """md5 -> the sorted families of its cores in ``scope``, read from the verified families file.

    A core no family took (``family`` -1) and a masked one (a promiscuous
    component, ADR-D65) give no unit.
    """
    name = f"families-{scope}.parquet"
    entry = next((o for o in outputs if o["name"] == name), None)
    if entry is None:
        raise ValueError(f"the segment families hold no {name}")
    blob = fetch_and_verify(store, entry["uri"], entry["sha256"], name)
    table = pq.read_table(io.BytesIO(blob), columns=["md5", "family", "masked"])
    md5 = np.asarray(table.column("md5").to_pylist(), dtype=object)
    family = table.column("family").to_numpy()
    keep = (family >= 0) & ~table.column("masked").to_numpy(zero_copy_only=False)
    keep &= np.fromiter((m in wanted for m in md5), bool, len(md5))
    out: dict[str, list[int]] = {}
    for m, f in zip(md5[keep], family[keep], strict=True):
        out.setdefault(m, []).append(int(f))
    return {m: np.unique(np.asarray(fs, dtype=np.int64)) for m, fs in out.items()}


def unit_matrix(md5s: list[str], units: Mapping[str, np.ndarray], n_units: int) -> sp.csr_matrix:
    """Rows x units, binary: each row carries the units of its md5 (none when the bank has none)."""
    rows = [np.full(len(units.get(m, ())), i, dtype=np.int64) for i, m in enumerate(md5s)]
    cols = [np.asarray(units.get(m, ()), dtype=np.int64) for m in md5s]
    r = np.concatenate(rows) if rows else np.zeros(0, np.int64)
    c = np.concatenate(cols) if cols else np.zeros(0, np.int64)
    return sp.csr_matrix((np.ones(len(r)), (r, c)), shape=(len(md5s), n_units))


def state_matrix(
    proteins: list[str], sets: Mapping[str, Iterable[str]], index: Mapping[str, int]
) -> sp.csr_matrix:
    """Proteins x columns, binary: each protein's terms that are columns of the aspect."""
    r, c = [], []
    for i, protein in enumerate(proteins):
        cols = sorted({index[t] for t in sets.get(protein, ()) if t in index})
        r.extend([i] * len(cols))
        c.extend(cols)
    return sp.csr_matrix(
        (np.ones(len(r)), (np.asarray(r, np.int64), np.asarray(c, np.int64))),
        shape=(len(proteins), len(index)),
    )


class Who(NamedTuple):
    """The donors and the targets of a map, by accession, and what a reading leaves out."""

    donors: list[str]
    targets: list[str]
    exclusion: str = "sequence"


@dataclass(frozen=True)
class AspectProblem:
    """One aspect's donors and targets as matrices, with who each row is."""

    donors: list[str]
    carriers: sp.csr_matrix
    states: sp.csr_matrix
    group: np.ndarray
    base_rate: np.ndarray
    targets: list[str]
    target_view: Targets


def aspect_problem(
    columns: Columns,
    positives: Mapping[str, Mapping[str, set[str]]],
    negatives: Mapping[str, set[str]],
    md5_of: Mapping[str, str],
    units: tuple[Mapping[str, np.ndarray], int],
    who: Who | tuple[list[str], list[str]],
) -> AspectProblem:
    """The aspect's donors (those whose state holds a column) and the targets, as matrices.

    ``who`` is the donors' and the targets' accessions and the exclusion (a pair
    of lists excludes by sequence); ``units`` is (md5 -> themes, the number of units).
    """
    who = Who(*who)
    if who.exclusion not in EXCLUSIONS:
        raise ValueError(f"exclusion {who.exclusion!r} is not one of {EXCLUSIONS}")
    index = columns.index()
    held = {p: positives.get(p, {}).get(columns.aspect, set()) for p in who.donors}
    states_all = state_matrix(who.donors, held, index)
    keep = np.flatnonzero(np.diff(states_all.indptr) > 0)
    donors = [who.donors[i] for i in keep]
    states = states_all[keep].tocsr()
    donor_md5 = [md5_of[d] for d in donors]
    by_sequence = who.exclusion == "sequence"
    donor_key = donor_md5 if by_sequence else donors
    _, group = np.unique(np.asarray(donor_key, dtype=object), return_inverse=True)
    carriers = unit_matrix(donor_md5, units[0], units[1])
    base_rate = np.asarray(states.sum(0)).ravel() / max(len(donors), 1)
    targets = who.targets
    target_md5 = [md5_of.get(t, "") for t in targets]  # no sequence: no units, nothing excluded
    target_key = target_md5 if by_sequence else targets
    return AspectProblem(
        donors,
        carriers,
        states,
        np.asarray(group, dtype=np.int64),
        base_rate,
        targets,
        _target_view(
            (target_md5, target_key), (donor_key, states), units, (negatives, index), targets
        ),
    )


def _target_view(
    target_side: tuple[list[str], list[str]],
    donor_side: tuple[list[str], sp.csr_matrix],
    units: tuple[Mapping[str, np.ndarray], int],
    denied_side: tuple[Mapping[str, set[str]], Mapping[str, int]],
    targets: list[str],
) -> Targets:
    """The targets' units, what their own exclusion takes out, and their denied terms.

    ``target_side`` is (each target's md5, the key its exclusion matches) and
    ``donor_side`` (each donor's key, the donors' states): the key is the md5 when
    the exclusion is by sequence and the accession when it is by accession.
    """
    target_md5, target_key = target_side
    donor_key, states = donor_side
    by_key: dict[str, list[int]] = {}
    for i, k in enumerate(donor_key):
        by_key.setdefault(k, []).append(i)
    pick_r, pick_c = [], []
    for i, k in enumerate(target_key):
        for d in by_key.get(k, ()):
            pick_r.append(i)
            pick_c.append(d)
    pick = sp.csr_matrix(
        (np.ones(len(pick_r)), (np.asarray(pick_r, np.int64), np.asarray(pick_c, np.int64))),
        shape=(len(targets), len(donor_key)),
    )
    excluded = (pick @ states).tocsr()
    excluded_n = np.asarray(pick.sum(1)).ravel()
    negatives, index = denied_side
    denied = state_matrix(targets, negatives, index)
    return Targets(unit_matrix(target_md5, units[0], units[1]), excluded, excluded_n, denied)


__all__ = [
    "ASPECTS",
    "EXCLUSIONS",
    "ROOTS",
    "AspectProblem",
    "Columns",
    "Who",
    "aspect_problem",
    "columns_of",
    "member_md5s",
    "state_matrix",
    "theme_units",
    "unit_matrix",
]
