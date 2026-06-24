"""GOA self-prior: each query target's OWN t0 non-experimental annotation.

Root cause this fixes: the KNN scoring excludes the neighbour self-hit by
accession, so it discards each target's OWN t0 non-experimental annotation.
The LAFA GOA Non-exp baseline is exactly that own annotation, which is why it
edged PROTEA-KNN. The self-prior re-injects the own annotation as an explicit,
auditable candidate source (NOT by relaxing the neighbour self-hit hygiene),
so the prediction becomes a superset of the GOA Non-exp signal plus the
embedding-KNN signal.

Validated lever (LAFA Sep_2025_Dec_2025, IA-weighted f_micro_w): PROTEA-KNN
plus self-prior overall 0.2974 vs GOA Non-exp 0.2958 (old PROTEA-KNN 0.2871);
wins all three NK / LK / PK categories.

Leakage guardrails (load-bearing):
  - The self-prior is read ONLY from the t0-frozen reference annotation store
    (the same ``reference_annotations.parquet`` that backs the frozen reference
    embeddings, manifest ``cutoff_date`` 2025-09-04). NEVER from t1 / post-cutoff
    annotations.
  - Only the non-experimental evidence codes the GOA Non-exp baseline itself
    uses are kept (Computational ISS/ISO/ISA/ISM/IGC/RCA, Phylogenetical
    IBA/IBD/IKR/IRD, Electronic IEA, ND, NAS). Experimental codes
    (EXP/IDA/IPI/IMP/IGI/IEP/Hxx), IC, and TAS are dropped so no experimental
    self annotation can leak in.
  - NOT-qualified rows are dropped.

Combine: scale neighbour-transfer scores by ``neighbour_scale`` (so the
self-prior dominates the threshold band and neighbours fill gaps only), then
max-combine with the self-prior per (query, go_term) candidate. Targets with no
own non-exp annotation degrade gracefully to pure neighbour transfer.

The module is pure (no torch / transformers / DB) so it stays importable from
tests, container linting, and the universal reranker, which can later consume
the self-prior as a feature.
"""

from __future__ import annotations

from typing import Any

# Exact GOA Non-exp evidence set (non-experimental only). Mirrors the LAFA GOA
# Non-exp predictor's ``--selected_go 'Computational,Phylogenetical,Electronic,
# ND,NAS'`` filter.
NONEXP_CODES: frozenset[str] = frozenset(
    {
        # Computational
        "ISS", "ISO", "ISA", "ISM", "IGC", "RCA",
        # Phylogenetical
        "IBA", "IBD", "IKR", "IRD",
        # Electronic
        "IEA",
        # No-data / non-traceable author statement
        "ND", "NAS",
    },
)

# Evidence codes that must NEVER enter the self-prior (experimental + curator +
# traceable author). Kept explicit so the leakage guardrail test can assert that
# none of these can pass the filter.
FORBIDDEN_CODES: frozenset[str] = frozenset(
    {
        # Experimental
        "EXP", "IDA", "IPI", "IMP", "IGI", "IEP",
        # High-throughput experimental
        "HTP", "HDA", "HMP", "HGI", "HEP",
        # Curator / traceable author
        "IC", "TAS",
    },
)


def _is_not_qualified(qualifier: Any) -> bool:
    """True iff the annotation carries a NOT qualifier (must be dropped)."""
    if qualifier is None:
        return False
    return "NOT" in str(qualifier).upper()


def build_self_prior_rows(
    annotations: dict[str, list[dict[str, Any]]],
    query_accessions: list[str],
    *,
    self_score: float = 1.0,
) -> list[dict[str, Any]]:
    """Build self-prior candidate rows for the query targets.

    The self-prior is each target's OWN t0 non-exp annotation, i.e. a self-hit
    at identity 1.0, so it is scored as a confident candidate (``self_score``,
    default 1.0). Rows are deduplicated per (query, go_term_id).

    Parameters
    ----------
    annotations:
        Reference annotation store (``accession -> [{go_term_id, qualifier,
        evidence_code, ...}]``) loaded from the t0-frozen bundle. The query
        target's OWN rows are looked up here by accession.
    query_accessions:
        Query accessions to build the self-prior for (after embedding).
    self_score:
        Score assigned to own-annotation candidates (1.0 = confident self-hit).

    Returns prediction-shaped rows ``{protein_accession, go_term_id,
    self_prior_score}`` restricted to ``NONEXP_CODES`` with NOT rows dropped.
    """
    rows: list[dict[str, Any]] = []
    for acc in query_accessions:
        seen: set[int] = set()
        for ann in annotations.get(acc, ()):
            code = str(ann.get("evidence_code") or "").strip().upper()
            if code not in NONEXP_CODES:
                continue
            if _is_not_qualified(ann.get("qualifier")):
                continue
            gtid = int(ann["go_term_id"])
            if gtid in seen:
                continue
            seen.add(gtid)
            rows.append(
                {
                    "protein_accession": acc,
                    "go_term_id": gtid,
                    "self_prior_score": float(self_score),
                },
            )
    return rows


def combine_with_self_prior(
    predictions: list[dict[str, Any]],
    self_rows: list[dict[str, Any]],
    *,
    neighbour_scale: float = 0.95,
    score_of: Any,
) -> list[dict[str, Any]]:
    """Max-combine neighbour predictions with the self-prior per (query, term).

    Neighbour scores are scaled by ``neighbour_scale`` first so the self-prior
    (scored 1.0 by default) dominates the threshold band and neighbours fill
    gaps only. The combined score is written onto each row under
    ``reranker_score`` so the existing output writer (which prefers
    ``reranker_score``) emits it without further wiring.

    Parameters
    ----------
    predictions:
        Neighbour-transfer prediction rows from ``protea_method.pipeline.predict``.
    self_rows:
        Output of :func:`build_self_prior_rows`.
    neighbour_scale:
        Multiplier applied to neighbour-transfer scores before combining.
    score_of:
        Callable mapping a neighbour prediction row to its output score in
        [0, 1] (e.g. the runtime's ``_score_for_output``).

    Returns a fresh list of rows keyed by (protein_accession, go_term_id), each
    carrying the max-combined ``reranker_score``. Neighbour rows keep their
    original feature columns; self-only rows are minimal candidate rows.
    """
    combined: dict[tuple[str, int], dict[str, Any]] = {}

    for pred in predictions:
        acc = str(pred["protein_accession"])
        gtid = int(pred["go_term_id"])
        score = float(score_of(pred)) * neighbour_scale
        key = (acc, gtid)
        existing = combined.get(key)
        if existing is None or score > float(existing["reranker_score"]):
            row = dict(pred)
            row["reranker_score"] = score
            combined[key] = row
        # else: keep the higher-scoring neighbour row already stored.

    for sp in self_rows:
        acc = str(sp["protein_accession"])
        gtid = int(sp["go_term_id"])
        score = float(sp["self_prior_score"])
        key = (acc, gtid)
        existing = combined.get(key)
        if existing is None:
            combined[key] = {
                "protein_accession": acc,
                "go_term_id": gtid,
                "reranker_score": score,
                "self_prior": True,
            }
        elif score > float(existing["reranker_score"]):
            existing["reranker_score"] = score
            existing["self_prior"] = True

    return list(combined.values())


__all__ = [
    "FORBIDDEN_CODES",
    "NONEXP_CODES",
    "build_self_prior_rows",
    "combine_with_self_prior",
]
