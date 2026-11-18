"""What a count map reads at one cut, and how it becomes matrices (ADR-D65, decisions 1 and 4).

The donors are the ``truth`` members of the photo whose annotation set starts
the window, each with its state at the cut (``protea.core._cut_state``). The
targets are the window's own: the proteins its truth scores. Both are named by
their sequence's md5 in the photo, which is how the lexicon bank names its
units. Per aspect, the columns are the pivot's terms of that aspect without its
root, and a donor whose state holds none of them is not a donor of the aspect.
"""

from __future__ import annotations

import io
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

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
    who: tuple[list[str], list[str]],
) -> AspectProblem:
    """The aspect's donors (those whose state holds a column) and the targets, as matrices.

    ``who`` is (the donors' accessions, the targets' accessions); ``units`` is
    (md5 -> themes, the number of units).
    """
    index = columns.index()
    held = {p: positives.get(p, {}).get(columns.aspect, set()) for p in who[0]}
    states_all = state_matrix(who[0], held, index)
    keep = np.flatnonzero(np.diff(states_all.indptr) > 0)
    donors = [who[0][i] for i in keep]
    states = states_all[keep].tocsr()
    donor_md5 = [md5_of[d] for d in donors]
    _, group = np.unique(np.asarray(donor_md5, dtype=object), return_inverse=True)
    carriers = unit_matrix(donor_md5, units[0], units[1])
    base_rate = np.asarray(states.sum(0)).ravel() / max(len(donors), 1)
    targets = who[1]
    return AspectProblem(
        donors,
        carriers,
        states,
        np.asarray(group, dtype=np.int64),
        base_rate,
        targets,
        _target_view(targets, md5_of, (donor_md5, states), units, (negatives, index)),
    )


def _target_view(
    targets: list[str],
    md5_of: Mapping[str, str],
    donor_side: tuple[list[str], sp.csr_matrix],
    units: tuple[Mapping[str, np.ndarray], int],
    denied_side: tuple[Mapping[str, set[str]], Mapping[str, int]],
) -> Targets:
    """The targets' units, what the donors sharing their sequence take out, and their denied terms."""
    target_md5 = [md5_of.get(t, "") for t in targets]  # no sequence: no units, nothing excluded
    donor_md5, states = donor_side
    by_md5: dict[str, list[int]] = {}
    for i, m in enumerate(donor_md5):
        by_md5.setdefault(m, []).append(i)
    pick_r, pick_c = [], []
    for i, m in enumerate(target_md5):
        for d in by_md5.get(m, ()):
            pick_r.append(i)
            pick_c.append(d)
    pick = sp.csr_matrix(
        (np.ones(len(pick_r)), (np.asarray(pick_r, np.int64), np.asarray(pick_c, np.int64))),
        shape=(len(targets), len(donor_md5)),
    )
    excluded = (pick @ states).tocsr()
    excluded_n = np.asarray(pick.sum(1)).ravel()
    negatives, index = denied_side
    denied = state_matrix(targets, negatives, index)
    return Targets(unit_matrix(target_md5, units[0], units[1]), excluded, excluded_n, denied)


__all__ = [
    "ASPECTS",
    "ROOTS",
    "AspectProblem",
    "Columns",
    "aspect_problem",
    "columns_of",
    "member_md5s",
    "state_matrix",
    "theme_units",
    "unit_matrix",
]
