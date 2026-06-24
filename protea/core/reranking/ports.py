"""The two meta-reranker port contracts: ``EvidenceScorer`` and ``Combiner``.

These are the whole abstraction (ARCHITECTURE.md "The two port contracts").
Everything plugs in through :class:`EvidenceScorer`; the per-category
:class:`Combiner` consumes the VECTOR of scorer outputs, never the raw feature
matrix. Both are :class:`typing.Protocol` ports, mirroring
:class:`protea.core.contracts.operation.Operation`, so any object that
structurally satisfies the contract is a valid scorer / combiner without
inheritance.

The candidate / query types are intentionally thin. A :class:`Candidate` is
just the per-candidate prediction dict the existing producers already build
(it carries ``go_id`` plus whatever signal keys a producer stamped on it), so
the MR-1 adapters read the SAME values the live producers wrote without any
reshaping. A :class:`QueryContext` groups the candidates of one query protein.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from protea.core.domain.category import Category

#: One candidate prediction for one query protein. Structurally the prediction
#: dict the producers build in ``_post_knn_pipeline``: it MUST carry a ``go_id``
#: string (snapshot-agnostic key, the bug fixed in #644) and MAY carry any
#: producer-stamped signal (``distance``, ``classifier_score``,
#: ``self_prior_score``, ``association_total`` ...). Kept as a plain mapping so
#: an adapter reads exactly what the live path persisted, with no conversion.
Candidate = dict[str, Any]


class QueryContext:
    """The candidates of one query protein, plus its accession.

    A scorer receives one ``QueryContext`` and returns one score per candidate.
    Grouping by protein lets a scorer that needs query-level state (the query's
    own known terms, its embedding) compute it once per protein rather than per
    candidate. ``extra`` is a free-form bag for such query-level inputs an
    adapter may need; the MR-1 adapters do not require it (they read everything
    off the candidate dicts the producers already enriched).
    """

    __slots__ = ("accession", "candidates", "extra")

    def __init__(
        self,
        accession: str,
        candidates: list[Candidate],
        extra: dict[str, Any] | None = None,
    ) -> None:
        self.accession = accession
        self.candidates = candidates
        self.extra: dict[str, Any] = extra or {}


@runtime_checkable
class EvidenceScorer(Protocol):
    """A port: one independent source of per-candidate evidence.

    A scorer is pure, independently sealable / measurable, and emits ONE
    calibrated score per candidate keyed on the snapshot-agnostic ``go_id``
    STRING. Internally it MAY be trivial (a stored feature), a trained model
    (the M2 classifier), an N-hop graph propagation, or one stage of a
    telescoping cascade; the contract does not change, only what is inside the
    port. Calibration is the scorer's own responsibility (or declared so the
    combiner knows).

    ``applies_to`` restricts the scorer to the CAFA categories where its
    evidence exists (e.g. priors do not apply to NK proteins). A caller that
    asks an inapplicable scorer for a category gets an empty dict.
    """

    name: str
    applies_to: set[Category]

    def score(self, query_ctx: QueryContext, candidates: list[Candidate]) -> dict[str, float]:
        """Return ``{go_id: score}`` for ``candidates`` of ``query_ctx``.

        Keyed on the ``go_id`` STRING. A candidate the scorer has no evidence
        for is simply absent from the returned dict (the combiner zero-fills),
        so the mapping is sparse by design.
        """
        ...


@runtime_checkable
class Combiner(Protocol):
    """A port: the shallow per-category meta-learner over the score VECTOR.

    The combiner consumes a low-dimensional vector of scorer outputs (a handful
    of components), NOT the raw feature matrix; low dimensionality is what keeps
    it low-variance and calibratable (the lever that fixes the single-level PK
    collapse). One combiner per CAFA category (NK / LK / PK have different
    available evidence). Trained on OUT-OF-FOLD scorer outputs (stacking
    discipline, no leakage) and sealed once.

    MR-0 only defines this interface plus the trivial reference implementations
    in :mod:`protea.core.reranking.combiners`. The trained per-category combiner
    is MR-2.
    """

    def fit(
        self,
        score_matrix_oof: list[list[float]],
        labels: list[int],
        category: Category,
    ) -> None:
        """Fit the combiner for ``category`` on out-of-fold score vectors.

        ``score_matrix_oof`` is row-per-candidate, column-per-scorer (a stable
        scorer order the caller fixes); ``labels`` is the matching 0/1 truth.
        """
        ...

    def combine(self, score_vector: list[float], category: Category) -> float:
        """Combine one candidate's scorer outputs into the final reranked score."""
        ...


__all__ = [
    "Candidate",
    "Combiner",
    "EvidenceScorer",
    "QueryContext",
]
