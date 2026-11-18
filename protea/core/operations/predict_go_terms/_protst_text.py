"""ProtST text-to-GO transfer producer (protst_text family, ADR-D45 safe).

The ProtST text-aligned PLM (``mila-intel/ProtST-esm1b``, supervised on
Swiss-Prot function descriptions) dents the BP wall that sequence-only KNN
cannot: offline it beats the champion at kNN GO-transfer and adds on every BP
cell, including the leakage-free NK anchor (see
``storage/text_scorer/WRITEUP.md`` and
``project_text_evidence_scorer_2026_07_08``). This module turns that validated
signal into ONE precomputed per-candidate feature the meta-reranker (ADR-D43)
reads through :class:`ProtstTextScorer`.

Stamp-only, exactly like :func:`apply_self_prior` / :func:`apply_association`:
it never adds or removes candidate rows, it annotates the EXISTING ankh-KNN (+
ancestor / classifier) candidates. The one function :func:`apply_protst_text`
covers BOTH the live predict path (gated by ``compute_protst``) and the offline
export (imported verbatim by ``_export_features``), so the training and eval
pools carry byte-identical values.

The signal, per (query protein, candidate GO term): take the query's 512-d
ProtST ``protein_feature``, retrieve the top-``K`` cosine-nearest ProtST
REFERENCE proteins, cosine-weight-vote their pre-cutoff t0 GO terms, normalise
by the per-query max vote -> ``protst_text_score``. ``protst_vote_fraction`` is
the fraction of the K neighbours carrying the term; ``protst_present`` is 1.0
when at least one neighbour voted it. Both banks are L2-normalised at read (the
config was created ``normalize=false``) so the score is a pure cosine vote.

ADR-D45 (declared-absent, never a silent zero):

* a query with NO ProtST embedding in the bank keeps the leaf builder's ``NaN``
  default on all three columns (native-missing, honest coverage gap);
* a covered query stamps a real measured ``protst_vote_fraction`` /
  ``protst_present`` (0.0 when the term drew no vote) and only stamps
  ``protst_text_score`` for terms that actually drew a vote (an unvoted term
  keeps ``NaN``: a measurement absent, not a measured zero).

Leakage guard: the query itself (same accession) is excluded from its own
neighbour set, and the reference GO terms come only from the pre-cutoff t0
annotation set the KNN reference pool uses.
"""

from __future__ import annotations

import os
import uuid
from collections import defaultdict
from typing import TYPE_CHECKING, Any

import numpy as np

from protea.core.operations.predict_go_terms._post_knn_pipeline import _load_go_id_and_aspect
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.protein.protein import Protein

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

#: Canonical ProtST API EmbeddingConfig (model_backend=protst, 512-d
#: ``protein_feature``, ``normalize=false``, ADR-D35). Overridable via the
#: ``PROTEA_PROTST_CONFIG_ID`` env var for a re-materialised bank.
DEFAULT_PROTST_CONFIG_ID = "bd3cd470-e384-4f6a-90cf-574704419373"

#: Top-K cosine neighbours voted per query, the validated recipe's ``K``
#: (``storage/text_scorer/knn_confirm_text.py``).
K_NEIGHBORS = 30

#: Column names of the protst_text family (mirrors
#: ``protea_contracts.FEATURE_FAMILIES["protst_text"]``).
_SCORE_COL = "protst_text_score"
_FRACTION_COL = "protst_vote_fraction"
_PRESENT_COL = "protst_present"

# One-entry process cache of the reference bank, keyed by
# (config_id_str, annotation_set_id_str). The bank is ~527k x 512 f32 (~1 GB);
# the export re-invokes the producer once per query chunk over the same t0 set,
# so reloading it every chunk would dominate runtime. Cleared by tests.
_BANK_CACHE: dict[tuple[str, str], tuple[list[str], np.ndarray, dict[str, set[str]]]] = {}


def _noop_emit(*_args: Any, **_kwargs: Any) -> None:
    """Swallow producer audit events; export/predict have their own logging."""
    return None


def _resolve_protst_config_id(override: str | uuid.UUID | None) -> uuid.UUID:
    """Resolve the ProtST EmbeddingConfig id (override / env / canonical default)."""
    raw = override or os.environ.get("PROTEA_PROTST_CONFIG_ID") or DEFAULT_PROTST_CONFIG_ID
    return raw if isinstance(raw, uuid.UUID) else uuid.UUID(str(raw))


def _l2_normalize(vec: np.ndarray) -> np.ndarray | None:
    """Return the unit vector, or ``None`` for a zero / non-finite vector."""
    v = np.asarray(vec, dtype=np.float32)
    norm = float(np.linalg.norm(v))
    if not np.isfinite(norm) or norm == 0.0:
        return None
    return v / norm


def _load_query_embeddings(
    session: Session, config_id: uuid.UUID, accessions: list[str]
) -> dict[str, np.ndarray]:
    """L2-normalised ProtST embedding per query accession that has one."""
    from protea.core.utils import chunks

    out: dict[str, np.ndarray] = {}
    for batch in chunks(accessions, 5000):
        rows = (
            session.query(Protein.accession, SequenceEmbedding.embedding)
            .join(
                SequenceEmbedding,
                (SequenceEmbedding.sequence_id == Protein.sequence_id)
                & (SequenceEmbedding.embedding_config_id == config_id)
                & (SequenceEmbedding.chunk_index_s == 0),
            )
            .filter(Protein.accession.in_(batch))
            .all()
        )
        for acc, emb in rows:
            unit = _l2_normalize(emb.to_numpy())
            if unit is not None:
                out[acc] = unit
    return out


def _load_reference_go_terms(
    session: Session, annotation_set_id: uuid.UUID, accessions: list[str]
) -> dict[str, set[str]]:
    """Pre-cutoff t0 GO ids (snapshot-invariant strings) per reference accession."""
    from protea.core.utils import chunks

    out: dict[str, set[str]] = defaultdict(set)
    for batch in chunks(accessions, 5000):
        rows = (
            session.query(ProteinGOAnnotation.protein_accession, GOTerm.go_id)
            .join(GOTerm, GOTerm.id == ProteinGOAnnotation.go_term_id)
            .filter(
                ProteinGOAnnotation.annotation_set_id == annotation_set_id,
                ProteinGOAnnotation.protein_accession.in_(batch),
            )
            .all()
        )
        for acc, go_id in rows:
            out[acc].add(go_id)
    return dict(out)


def _load_reference_bank(
    session: Session, config_id: uuid.UUID, annotation_set_id: uuid.UUID
) -> tuple[list[str], np.ndarray, dict[str, set[str]]]:
    """Build the ProtST reference bank: proteins with a ProtST embedding AND t0 terms.

    Returns ``(accessions, unit_embeddings (N, D), {accession: {go_id, ...}})``.
    Both the join and the annotation lookup are scoped to the SAME pre-cutoff t0
    ``annotation_set_id`` the KNN reference pool uses (leakage discipline).
    """
    annotated_sq = (
        session.query(ProteinGOAnnotation.protein_accession)
        .filter(ProteinGOAnnotation.annotation_set_id == annotation_set_id)
        .distinct()
        .subquery()
    )
    rows = (
        session.query(Protein.accession, SequenceEmbedding.embedding)
        .join(
            SequenceEmbedding,
            (SequenceEmbedding.sequence_id == Protein.sequence_id)
            & (SequenceEmbedding.embedding_config_id == config_id)
            & (SequenceEmbedding.chunk_index_s == 0),
        )
        .join(annotated_sq, Protein.accession == annotated_sq.c.protein_accession)
        .all()
    )
    accessions: list[str] = []
    vecs: list[np.ndarray] = []
    for acc, emb in rows:
        unit = _l2_normalize(emb.to_numpy())
        if unit is None:
            continue
        accessions.append(acc)
        vecs.append(unit)
    matrix = np.vstack(vecs) if vecs else np.empty((0, 0), dtype=np.float32)
    ref_go = _load_reference_go_terms(session, annotation_set_id, accessions)
    return accessions, matrix, ref_go


def _get_reference_bank(
    session: Session, config_id: uuid.UUID, annotation_set_id: uuid.UUID
) -> tuple[list[str], np.ndarray, dict[str, set[str]]]:
    """Cached :func:`_load_reference_bank` (1-entry, keyed by config + t0 set)."""
    key = (str(config_id), str(annotation_set_id))
    cached = _BANK_CACHE.get(key)
    if cached is not None:
        return cached
    _BANK_CACHE.clear()
    bank = _load_reference_bank(session, config_id, annotation_set_id)
    _BANK_CACHE[key] = bank
    return bank


def _knn_vote(
    query_vec: np.ndarray,
    bank: tuple[list[str], np.ndarray, dict[str, set[str]]],
    k: int,
    exclude: set[str],
) -> tuple[dict[str, float], dict[str, float]]:
    """Top-``k`` cosine-weighted GO vote for one query (numpy, never pgvector).

    Returns ``(score_by_go, fraction_by_go)``: ``score`` is the per-query-max
    normalised cosine-weighted vote; ``fraction`` is (# of the k neighbours
    carrying the term) / k. Non-positive cosines are dropped from the vote (the
    validated recipe); the query itself is excluded from its own neighbours.
    """
    accessions, matrix, ref_go = bank
    n = matrix.shape[0]
    if n == 0:
        return {}, {}
    sims = matrix @ query_vec
    if exclude:
        for i, acc in enumerate(accessions):
            if acc in exclude:
                sims[i] = -np.inf
    # Eligible = finite-similarity refs (excluded / non-finite are dropped). The
    # fraction denominator is min(k, eligible), so a self-hit never inflates the
    # neighbour count.
    n_elig = int(np.isfinite(sims).sum())
    if n_elig == 0:
        return {}, {}
    k_eff = min(k, n_elig)
    top = np.argpartition(-sims, k_eff - 1)[:k_eff] if k_eff < n else np.arange(n)
    votes: dict[str, float] = {}
    counts: dict[str, int] = {}
    for i in top:
        cos = float(sims[i])
        if cos <= 0.0 or not np.isfinite(cos):
            continue
        for go in ref_go.get(accessions[i], ()):
            votes[go] = votes.get(go, 0.0) + cos
            counts[go] = counts.get(go, 0) + 1
    if not votes:
        return {}, {}
    mx = max(votes.values())
    score = {go: v / mx for go, v in votes.items()} if mx > 0.0 else {}
    fraction = {go: c / k_eff for go, c in counts.items()}
    return score, fraction


def _candidate_go_id(rec: dict[str, Any], go_id_by_int: dict[int, str]) -> str | None:
    """Snapshot-invariant go_id for a candidate rec (stamped, else int-resolved)."""
    go = rec.get("go_id")
    if go:
        return go
    gtid = rec.get("go_term_id")
    if gtid is None:
        return None
    return go_id_by_int.get(int(gtid))


def _stamp_query(
    recs: list[dict[str, Any]],
    score: dict[str, float],
    fraction: dict[str, float],
    go_id_by_int: dict[int, str],
) -> int:
    """Stamp the three protst columns onto one covered query's candidates.

    ``protst_vote_fraction`` / ``protst_present`` get their measured value (0.0
    when the term drew no vote); ``protst_text_score`` is stamped only for a term
    that drew a vote (an unvoted term keeps the NaN declared-absent default).
    Returns the number of ``protst_text_score`` stamps.
    """
    stamped = 0
    for rec in recs:
        go = _candidate_go_id(rec, go_id_by_int)
        if go is None:
            continue
        frac = fraction.get(go, 0.0)
        rec[_FRACTION_COL] = frac
        rec[_PRESENT_COL] = 1.0 if frac > 0.0 else 0.0
        if go in score:
            rec[_SCORE_COL] = score[go]
            stamped += 1
    return stamped


def apply_protst_text(
    session: Session,
    predictions: list[dict[str, Any]],
    t0_annotation_set_id: uuid.UUID,
    *,
    protst_config_id: str | uuid.UUID | None = None,
    k: int = K_NEIGHBORS,
    emit: Any = _noop_emit,
) -> None:
    """Stamp the ProtST text-to-GO transfer signal onto existing candidates.

    Stamp-only (never adds/removes candidates); mutates ``predictions`` in place.
    Queries absent from the ProtST bank keep the leaf builder's NaN default on
    all three columns (ADR-D45 declared-absent), so a coverage gap is honest and
    the export degeneracy check treats an all-null protst family as absent, not a
    silent zero.
    """
    if not predictions:
        return
    config_id = _resolve_protst_config_id(protst_config_id)
    accessions = sorted({r.get("protein_accession", "") for r in predictions if r.get("protein_accession")})
    if not accessions:
        return
    query_emb = _load_query_embeddings(session, config_id, accessions)
    if not query_emb:
        emit("predict_go_terms_batch.protst_text_done", None, {"queries_covered": 0, "scores": 0}, "info")
        return
    bank = _get_reference_bank(session, config_id, t0_annotation_set_id)
    if not bank[0]:
        emit("predict_go_terms_batch.protst_text_done", None, {"queries_covered": 0, "scores": 0}, "info")
        return
    go_id_by_int, _aspect = _load_go_id_and_aspect(
        session,
        {int(r["go_term_id"]) for r in predictions if r.get("go_term_id") is not None},
    )
    recs_by_protein: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for rec in predictions:
        recs_by_protein[rec.get("protein_accession", "")].append(rec)
    covered = 0
    stamped = 0
    for acc, recs in recs_by_protein.items():
        qvec = query_emb.get(acc)
        if qvec is None:
            continue
        covered += 1
        score, fraction = _knn_vote(qvec, bank, k, exclude={acc})
        stamped += _stamp_query(recs, score, fraction, go_id_by_int)
    emit(
        "predict_go_terms_batch.protst_text_done",
        None,
        {"queries_covered": covered, "scores": stamped},
        "info",
    )


__all__ = ("apply_protst_text",)
