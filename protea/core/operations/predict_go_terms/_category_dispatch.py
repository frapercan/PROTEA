"""Per-category (NK / LK / PK) candidate partitioning for INT-5.

The first-place LAFA system trains three LightGBM boosters, one per CAFA
partition (No-Knowledge, Limited-Knowledge, Partial-Knowledge). To apply
them natively the predict path must assign every candidate
``(query protein, candidate aspect)`` to its CAFA category, derived from the
protein's pre-cutoff EXPERIMENTAL annotations (the t0 "known" terms).

Partition definition (matches the verified CAFA / LAFA partition where NK
has 0% t0-known terms and LK / PK have 100%, split by aspect):

- **NK**: the protein has NO t0 experimental GO annotation in ANY aspect.
- **LK**: the protein HAS a t0 experimental annotation in some aspect but
  NOT in the aspect of this candidate term.
- **PK**: the protein HAS a t0 experimental annotation in the aspect of
  this candidate term.

The known-terms loader (``_load_annotations_for`` + ``_own_exp_terms``) and
the per-term aspect lookup (``_load_known_aspects``) are reused verbatim from
INT-2 / INT-3 so the category here is leakage-clean by construction: only the
same pre-cutoff ``annotation_set_id`` the KNN reference pool uses is read,
NOT-qualified rows are dropped, and non-experimental evidence is filtered out.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from protea.core.operations.predict_go_terms._post_knn_pipeline import (
    _load_known_aspects,
    _own_exp_terms,
)

if TYPE_CHECKING:
    from protea.core.operations.predict_go_terms._batch_op import (
        PredictGOTermsBatchOperation,
    )

#: Canonical CAFA category codes, also the per-category booster keys.
CATEGORY_NK = "NK"
CATEGORY_LK = "LK"
CATEGORY_PK = "PK"
CATEGORIES: tuple[str, str, str] = (CATEGORY_NK, CATEGORY_LK, CATEGORY_PK)


def attach_query_category(
    op: PredictGOTermsBatchOperation,
    session: Session,
    annotation_set_id: uuid.UUID,
    prediction_dicts: list[dict[str, Any]],
) -> dict[str, int]:
    """Write a CAFA ``category`` (NK / LK / PK) onto every prediction dict.

    The category is a property of ``(protein, candidate aspect)``: it depends
    on whether the protein has any pre-cutoff experimental annotation at all
    (NK vs. K) and, for K proteins, whether that knowledge is in the same
    aspect as the candidate term (PK) or a different one (LK).

    Returns a ``{category: count}`` histogram for the audit event. Candidates
    whose ``go_term_id`` is missing are left untouched (no category written).
    """
    accessions = {
        rec.get("protein_accession", "")
        for rec in prediction_dicts
        if rec.get("protein_accession")
    }
    own_exp = _own_exp_for(op, session, annotation_set_id, accessions)
    known_aspects_by_protein = _known_aspects_by_protein(session, own_exp)
    candidate_aspects = _candidate_aspect_map(session, prediction_dicts)

    hist = {CATEGORY_NK: 0, CATEGORY_LK: 0, CATEGORY_PK: 0}
    for rec in prediction_dicts:
        gtid = rec.get("go_term_id")
        if gtid is None:
            continue
        acc = rec.get("protein_accession", "")
        cand_aspect = candidate_aspects.get(int(gtid), "")
        category = _classify(known_aspects_by_protein.get(acc, frozenset()), cand_aspect)
        rec["category"] = category
        hist[category] += 1
    return hist


def _classify(known_aspects: frozenset[str], candidate_aspect: str) -> str:
    """Return NK / LK / PK for one ``(protein, candidate aspect)`` pair."""
    if not known_aspects:
        return CATEGORY_NK
    if candidate_aspect and candidate_aspect in known_aspects:
        return CATEGORY_PK
    return CATEGORY_LK


def _own_exp_for(
    op: PredictGOTermsBatchOperation,
    session: Session,
    annotation_set_id: uuid.UUID,
    accessions: set[str],
) -> dict[str, set[int]]:
    """Pre-cutoff experimental known terms per accession (leakage-clean)."""
    accessions = {acc for acc in accessions if acc}
    if not accessions:
        return {}
    annotations = op._load_annotations_for(session, annotation_set_id, accessions)
    return _own_exp_terms(annotations)


def _known_aspects_by_protein(
    session: Session,
    own_exp: dict[str, set[int]],
) -> dict[str, frozenset[str]]:
    """Map each protein to the set of aspects its known terms cover."""
    if not own_exp:
        return {}
    all_known: set[int] = set()
    for terms in own_exp.values():
        all_known |= terms
    aspect_by_id = _load_known_aspects(session, all_known)
    result: dict[str, frozenset[str]] = {}
    for acc, terms in own_exp.items():
        aspects = {aspect_by_id.get(t, "") for t in terms}
        aspects.discard("")
        if aspects:
            result[acc] = frozenset(aspects)
    return result


def _candidate_aspect_map(
    session: Session,
    prediction_dicts: list[dict[str, Any]],
) -> dict[int, str]:
    """Aspect per candidate ``go_term_id``, reusing already-attached values.

    Prefers the ``aspect`` already stamped on the dicts (the reranker's
    categorical feature); only the residual unstamped ids hit the DB.
    """
    aspect_by_id: dict[int, str] = {}
    missing: set[int] = set()
    for rec in prediction_dicts:
        gtid = rec.get("go_term_id")
        if gtid is None:
            continue
        gid = int(gtid)
        stamped = rec.get("aspect")
        if stamped:
            aspect_by_id[gid] = stamped
        elif gid not in aspect_by_id:
            missing.add(gid)
    missing -= set(aspect_by_id)
    if missing:
        aspect_by_id.update(_load_known_aspects(session, missing))
    return aspect_by_id


__all__ = (
    "CATEGORIES",
    "CATEGORY_LK",
    "CATEGORY_NK",
    "CATEGORY_PK",
    "attach_query_category",
)
