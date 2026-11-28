"""Hierarchy restoration: each predicted term's ancestors take the maximum of themselves and it.

A calibrated reading, a fitted set and every out-of-fold score that declares a
threshold or feeds a later level are made hierarchy-consistent before they are
rendered and evaluated: per protein, every ancestor ``a`` of a predicted term
``t`` takes ``max(s(a), s(t))``, and an ancestor without a score is added. The
evaluator propagates a prediction file under ``fill`` (a term without a score
takes the maximum of its children; one with a score keeps it), so on a restored
set the evaluator's ``fill`` and a ``max`` propagation read the same matrix and
give byte-identical per-protein grids. A per-cell monotone map does not keep a
set consistent once its strata separate a term from its ancestors, which is why
the order is the fitted map, then this, then the rendering, then the grid
(:func:`protea.core.operating_point.grid.evaluate_to_grid`).

**The ancestors are the evaluator's.** :func:`ancestry_from_obo` reads the OBO
file the evaluator reads for the window (its pivot) with cafaeval-protea's own
parser, ``obo_parser`` with ``is_a`` and ``part_of``, and takes each term's
ancestors from the same closure cafaeval's propagation walks (``_ancestors_csr``
over the parsed graph, one graph per namespace, so an ancestor never crosses an
aspect). An alternative id is read as the parser reads it, as its primary
terms; a term the parser does not hold (obsolete, or of another ontology) is
left as it is, since the evaluator drops it.

**What is not restored.** A raw literal set keeps the evaluator's ``fill``
semantics, as LAFA scores it. The raw sets of sources whose donor states are
closed are already consistent, and :func:`restoration_counts` says so: nothing
raised, nothing added.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

if TYPE_CHECKING:  # pragma: no cover - typing only
    from sqlalchemy.orm import Session

    from protea.core.contracts.operation import EmitFn

#: The relations the evaluator propagates through, in the order its parser takes them.
PROPAGATED_RELATIONS: tuple[str, str] = ("is_a", "part_of")


@dataclass(frozen=True)
class Ancestry:
    """Each term's ancestors under one ontology file, as the evaluator's parser reads them.

    ``terms`` is the vocabulary (every live term of every namespace) and
    ``ancestors[i]`` the indices of term ``i``'s ancestors, itself included.
    ``primaries`` maps an alternative id to the primary terms it stands for.
    """

    terms: tuple[str, ...]
    ancestors: tuple[np.ndarray, ...]
    primaries: Mapping[str, tuple[str, ...]]

    def index(self) -> dict[str, int]:
        """Each term's position in :attr:`terms`."""
        return {term: i for i, term in enumerate(self.terms)}

    def ancestors_of(self, go_id: str) -> frozenset[str]:
        """``go_id``'s ancestors, itself included, through its primaries for an alternative id."""
        where = self.index()
        found: set[str] = set()
        for primary in self.primaries_of(go_id, where):
            found.update(self.terms[int(j)] for j in self.ancestors[where[primary]])
        return frozenset(found)

    def primaries_of(self, go_id: str, where: Mapping[str, int]) -> tuple[str, ...]:
        """The terms ``go_id`` is read as: itself, its primaries for an alternative id, or none."""
        if go_id in where:
            return (go_id,)
        return tuple(p for p in self.primaries.get(go_id, ()) if p in where)


def ancestry_from_obo(obo_path: str | Path) -> Ancestry:
    """The ancestry of an OBO file, read with cafaeval-protea's parser and its closure."""
    from cafaeval.graph import _ancestors_csr
    from cafaeval.parser import obo_parser

    ontologies = obo_parser(str(obo_path), PROPAGATED_RELATIONS, None, True)
    terms: list[str] = []
    ancestors: list[np.ndarray] = []
    primaries: dict[str, set[str]] = {}
    for namespace in sorted(ontologies):
        graph = ontologies[namespace]
        offset = len(terms)
        indptr, indices = _ancestors_csr(graph)
        for position, term in enumerate(graph.terms_list):
            terms.append(str(term["id"]))
            span = indices[indptr[position] : indptr[position + 1]]
            ancestors.append(np.sort(np.asarray(span, dtype=np.int64) + offset))
        for alternative, canonical in graph.terms_dict_alt.items():
            primaries.setdefault(str(alternative), set()).update(str(c) for c in canonical)
    return Ancestry(
        terms=tuple(terms),
        ancestors=tuple(ancestors),
        primaries={alt: tuple(sorted(found)) for alt, found in sorted(primaries.items())},
    )


def window_ancestry(session: Session, evaluation_set: Any, workdir: str, emit: EmitFn) -> Ancestry:
    """The ancestry of the OBO file the evaluator reads for ``evaluation_set``: its pivot's.

    The pivot is the one the evaluator resolves (the window's recorded pivot,
    else its t1 corpus's snapshot), and the file is fetched the way the
    evaluator fetches it (the archived copy, verified, before the upstream URL).
    """
    import uuid

    from protea.core.operations._evaluation_artifacts import resolve_obo
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot

    recorded = (evaluation_set.stats or {}).get("pivot_ontology_snapshot_id")
    if recorded:
        pivot_id = uuid.UUID(str(recorded))
    else:
        corpus = session.get(AnnotationSet, evaluation_set.new_annotation_set_id)
        if corpus is None:
            raise ValueError(f"window {evaluation_set.id} names a t1 corpus that is gone")
        pivot_id = corpus.ontology_snapshot_id
    snapshot = session.get(OntologySnapshot, pivot_id)
    if snapshot is None:
        raise ValueError(f"window {evaluation_set.id}: pivot snapshot {pivot_id} not found")
    return ancestry_from_obo(resolve_obo(workdir, snapshot, emit))


@dataclass(frozen=True)
class _TermPlan:
    """Per distinct term of a table: the ancestor indices its rows raise, flattened."""

    known: np.ndarray
    indptr: np.ndarray
    indices: np.ndarray


def _plan(unique_terms: list[str], ancestry: Ancestry) -> _TermPlan:
    """For each distinct term, the union of its primaries' ancestors; unknown terms raise none."""
    where = ancestry.index()
    spans: list[np.ndarray] = []
    known = np.zeros(len(unique_terms), dtype=bool)
    for position, term in enumerate(unique_terms):
        primaries = ancestry.primaries_of(term, where)
        if not primaries:
            spans.append(np.zeros(0, dtype=np.int64))
            continue
        known[position] = True
        joined = np.concatenate([ancestry.ancestors[where[p]] for p in primaries])
        spans.append(np.unique(joined))
    lengths = np.fromiter((s.size for s in spans), dtype=np.int64, count=len(spans))
    indptr = np.zeros(len(spans) + 1, dtype=np.int64)
    np.cumsum(lengths, out=indptr[1:])
    indices = np.concatenate(spans) if spans else np.zeros(0, dtype=np.int64)
    return _TermPlan(known, indptr, indices)


def _expanded(
    rows: np.ndarray, codes: np.ndarray, plan: _TermPlan
) -> tuple[np.ndarray, np.ndarray]:
    """``(row, ancestor)`` for every ancestor each row raises, in row order."""
    starts = plan.indptr[codes[rows]]
    lengths = plan.indptr[codes[rows] + 1] - starts
    total = int(lengths.sum())
    owner = np.repeat(rows, lengths)
    first = np.repeat(starts - np.concatenate(([0], np.cumsum(lengths)[:-1])), lengths)
    return owner, plan.indices[first + np.arange(total, dtype=np.int64)]


@dataclass(frozen=True)
class RestorationCounts:
    """What a restoration did: rows read, rows written, scores raised and ancestors added."""

    rows_in: int
    rows_out: int
    raised: int
    added: int

    @property
    def changed(self) -> bool:
        return bool(self.raised or self.added or self.rows_in != self.rows_out)


def restore(scores: pa.Table, ancestry: Ancestry) -> pa.Table:
    """``scores`` made hierarchy-consistent under ``ancestry``; see :func:`restore_with_counts`."""
    return restore_with_counts(scores, ancestry)[0]


def restore_with_counts(scores: pa.Table, ancestry: Ancestry) -> tuple[pa.Table, RestorationCounts]:
    """Restore ``scores`` (``protein_accession``, ``go_id``, float ``score``) and count what moved.

    Per protein, every ancestor of a predicted term (its primaries' for an
    alternative id) takes the maximum of its own score and the term's, and an
    ancestor without a score is added under its primary id; an alternative id's
    row becomes its primaries' rows. A term the ancestry does not hold is kept
    as it is. The result is ordered by protein and term.
    """
    proteins = pc.dictionary_encode(scores["protein_accession"]).combine_chunks()
    terms = pc.dictionary_encode(scores["go_id"]).combine_chunks()
    values = np.asarray(scores["score"].to_numpy(zero_copy_only=False), dtype=np.float64)
    protein_codes = np.asarray(proteins.indices.to_numpy(zero_copy_only=False), dtype=np.int64)
    term_codes = np.asarray(terms.indices.to_numpy(zero_copy_only=False), dtype=np.int64)
    unique_terms = [str(t) for t in terms.dictionary.to_pylist()]
    plan = _plan(unique_terms, ancestry)
    known_rows = np.flatnonzero(plan.known[term_codes])
    kept_rows = np.flatnonzero(~plan.known[term_codes])
    owner, ancestor = _expanded(known_rows, term_codes, plan)
    width = np.int64(max(len(ancestry.terms), 1))
    keys = protein_codes[owner] * width + ancestor
    order = np.argsort(keys, kind="stable")
    keys, raised_values = keys[order], values[owner][order]
    starts = np.flatnonzero(np.r_[True, keys[1:] != keys[:-1]]) if keys.size else np.zeros(0, int)
    best = np.maximum.reduceat(raised_values, starts) if keys.size else np.zeros(0)
    out_proteins = proteins.dictionary.take(pa.array(keys[starts] // width))
    out_terms = [ancestry.terms[int(t)] for t in keys[starts] % width]
    restored = pa.table(
        {
            "protein_accession": pa.concat_arrays(
                [out_proteins, scores["protein_accession"].combine_chunks().take(kept_rows)]
            ),
            "go_id": pa.concat_arrays(
                [pa.array(out_terms, pa.string()), scores["go_id"].combine_chunks().take(kept_rows)]
            ),
            "score": pa.array(np.concatenate([best, values[kept_rows]]), pa.float64()),
        }
    )
    restored = restored.sort_by([("protein_accession", "ascending"), ("go_id", "ascending")])
    return restored, _counts(scores, restored)


def _counts(before: pa.Table, after: pa.Table) -> RestorationCounts:
    """Rows in and out, and how many (protein, term) scores rose or appeared."""
    keys = ["protein_accession", "go_id"]
    old = before.to_pandas().groupby(keys, sort=False)["score"].max().rename("before")
    new = after.to_pandas().groupby(keys, sort=False)["score"].max().rename("after")
    joined = new.to_frame().join(old, how="left")
    added = int(joined["before"].isna().sum())
    raised = int((joined["after"] > joined["before"]).sum())
    return RestorationCounts(before.num_rows, after.num_rows, raised, added)


def restoration_counts(scores: pa.Table, ancestry: Ancestry) -> RestorationCounts:
    """What restoring ``scores`` would change; nothing for a set that is already consistent."""
    return restore_with_counts(scores, ancestry)[1]


__all__ = [
    "PROPAGATED_RELATIONS",
    "Ancestry",
    "RestorationCounts",
    "ancestry_from_obo",
    "restoration_counts",
    "restore",
    "restore_with_counts",
    "window_ancestry",
]
