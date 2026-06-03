"""Helpers for the two long methods of ``annotations_service``.

Splits ``get_go_subgraph_data`` (DAG BFS) and
``iter_delta_proteins_fasta`` (FASTA emission) so neither orchestrator
nor any helper exceeds the §3 method-LOC ceiling.

Both functions are re-exported from ``protea.services.annotations_service``
for backwards compatibility with router and CLI callers.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from protea.core.evaluation import load_evaluation_data_for_set
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot

# ---------------------------------------------------------------------------
# get_go_subgraph_data helpers
# ---------------------------------------------------------------------------


def _resolve_seed_terms(
    session: Session, snapshot_id: uuid.UUID, go_ids: str
) -> list[GOTerm]:
    """Find the GOTerm rows whose ``go_id`` matches the comma-separated query."""
    query_go_ids = {g.strip() for g in go_ids.split(",") if g.strip()}
    return (
        session.query(GOTerm)
        .filter(
            GOTerm.ontology_snapshot_id == snapshot_id,
            GOTerm.go_id.in_(query_go_ids),
        )
        .all()
    )


def _bfs_ancestors(
    session: Session,
    snapshot_id: uuid.UUID,
    seed_terms: list[GOTerm],
    depth: int,
) -> tuple[dict[int, GOTerm], list[dict[str, Any]]]:
    """Walk the GO DAG upward up to ``depth`` levels and collect terms+edges.

    Returns ``(all_terms_by_db_id, edge_list)``. ``edge_list`` entries
    are dicts ready for JSON serialisation: ``source`` (child id),
    ``target`` (parent id), ``relation_type``.

    Takes the seed ``GOTerm`` objects directly (not just their ids)
    so we do not re-query the DB for them; preserves the exact
    ``session.query`` call sequence the original implementation
    expected.
    """
    from protea.infrastructure.orm.models.annotation.go_term_relationship import (
        GOTermRelationship,
    )

    visited_ids: set[int] = {t.id for t in seed_terms}
    frontier: set[int] = visited_ids.copy()
    all_terms: dict[int, GOTerm] = {t.id: t for t in seed_terms}
    all_edges: list[dict[str, Any]] = []

    for _ in range(depth):
        if not frontier:
            break
        rels = (
            session.query(GOTermRelationship)
            .filter(
                GOTermRelationship.ontology_snapshot_id == snapshot_id,
                GOTermRelationship.child_go_term_id.in_(frontier),
            )
            .all()
        )
        parent_ids = {r.parent_go_term_id for r in rels} - visited_ids
        for r in rels:
            all_edges.append(
                {
                    "source": r.child_go_term_id,
                    "target": r.parent_go_term_id,
                    "relation_type": r.relation_type,
                }
            )
        if not parent_ids:
            break
        for p in session.query(GOTerm).filter(GOTerm.id.in_(parent_ids)).all():
            all_terms[p.id] = p
        visited_ids |= parent_ids
        frontier = parent_ids

    return all_terms, all_edges


def _format_subgraph_response(
    seed_term_ids: set[int],
    all_terms: dict[int, GOTerm],
    all_edges: list[dict[str, Any]],
) -> dict[str, Any]:
    """Shape the subgraph payload for the API: ``{"nodes", "edges"}``."""
    return {
        "nodes": [
            {
                "id": t.id,
                "go_id": t.go_id,
                "name": t.name,
                "aspect": t.aspect,
                "is_query": t.id in seed_term_ids,
            }
            for t in all_terms.values()
        ],
        "edges": all_edges,
    }


def get_go_subgraph_data(
    session: Session,
    snapshot_id: uuid.UUID,
    go_ids: str,
    depth: int,
) -> dict[str, Any]:
    """BFS the GO DAG upward from the requested seed terms.

    Returns ``{"nodes": [...], "edges": [...]}`` ready for the API.
    Each node has ``id`` (DB id), ``go_id``, ``name``, ``aspect``,
    ``is_query`` (True for the seed terms). Each edge has
    ``source`` (child id), ``target`` (parent id), ``relation_type``.

    Raises :class:`EntityNotFoundError` when the snapshot does not resolve.
    Imports it lazily to avoid the circular dependency with the
    re-exporting ``annotations_service`` module.
    """
    from protea.services.annotations_service import EntityNotFoundError

    if session.get(OntologySnapshot, snapshot_id) is None:
        raise EntityNotFoundError("OntologySnapshot", snapshot_id)

    seed_terms = _resolve_seed_terms(session, snapshot_id, go_ids)
    if not seed_terms:
        return {"nodes": [], "edges": []}

    seed_ids = {t.id for t in seed_terms}
    all_terms, all_edges = _bfs_ancestors(session, snapshot_id, seed_terms, depth)
    return _format_subgraph_response(seed_ids, all_terms, all_edges)


# ---------------------------------------------------------------------------
# iter_delta_proteins_fasta helpers
# ---------------------------------------------------------------------------


def _select_delta_accessions(data: Any, category: str) -> dict[str, str]:
    """Map accession → label (NK / LK / PK) per requested category.

    ``"all"`` is the union NK ∪ LK ∪ PK with first-match wins
    (NK > LK > PK precedence in the existing implementation).
    """
    accession_label: dict[str, str] = {}
    if category in ("nk", "all"):
        for acc in data.nk:
            accession_label[acc] = "NK"
    if category in ("lk", "all"):
        for acc in data.lk:
            accession_label[acc] = "LK"
    if category in ("pk", "all"):
        for acc in data.pk:
            accession_label.setdefault(acc, "PK")
    return accession_label


def _render_fasta_lines(
    rows: list[Any], accession_label: dict[str, str]
) -> list[str]:
    """Render ``(Protein, Sequence)`` rows to FASTA lines, 60-char wrap.

    Header format:
    ``>ACCESSION entry_name OS=organism OX=taxon_id (NK|LK|PK)``.
    """
    lines: list[str] = []
    for protein, seq in rows:
        label = accession_label.get(protein.accession, "")
        parts = [protein.accession]
        if protein.entry_name:
            parts.append(protein.entry_name)
        if protein.organism:
            parts.append(f"OS={protein.organism}")
        if protein.taxonomy_id:
            parts.append(f"OX={protein.taxonomy_id}")
        parts.append(f"({label})")
        lines.append(f">{' '.join(parts)}\n")
        s = seq.sequence
        for i in range(0, len(s), 60):
            lines.append(s[i : i + 60] + "\n")
    return lines


def iter_delta_proteins_fasta(
    session: Session,
    eval_id: uuid.UUID,
    category: str,
) -> list[str]:
    """Return FASTA lines for delta proteins (``nk`` / ``lk`` / ``pk`` / ``all``).

    Only proteins whose sequence is in the DB are emitted. Header is
    ``>ACCESSION entry_name OS=organism OX=taxon (NK|LK|PK)``; the
    sequence is wrapped at 60 chars per line.

    Empty result returns an empty list. Raises
    :class:`EntityNotFoundError` if the EvaluationSet does not
    resolve. Imports it lazily to avoid the circular dependency with
    the re-exporting ``annotations_service`` module.
    """
    from protea.infrastructure.orm.models.protein.protein import Protein
    from protea.infrastructure.orm.models.sequence.sequence import Sequence
    from protea.services.annotations_service import EntityNotFoundError

    e = session.get(EvaluationSet, eval_id)
    if e is None:
        raise EntityNotFoundError("EvaluationSet", eval_id)
    data, _ = load_evaluation_data_for_set(session, e)

    accession_label = _select_delta_accessions(data, category)
    if not accession_label:
        return []

    rows = (
        session.query(Protein, Sequence)
        .join(Sequence, Protein.sequence_id == Sequence.id)
        .filter(Protein.accession.in_(accession_label.keys()))
        .order_by(Protein.accession)
        .all()
    )
    return _render_fasta_lines(rows, accession_label)
