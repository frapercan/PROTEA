"""ORM model for ScoringConfig — reproducible scoring formulas for GOPrediction rows.

A ScoringConfig stores two complementary layers of configuration:

1. **Signal weights** (``weights`` field): a dict mapping each composite signal
   (e.g. ``embedding_similarity``, ``identity_nw``) to a relative weight [0, 1].
   Missing signals — because the corresponding feature-engineering flag was not
   enabled at prediction time — are automatically excluded from the denominator,
   so the remaining active signals always produce a normalised [0, 1] score.

2. **Evidence-code weights** (``evidence_weights`` field, optional JSONB):
   a per-GO-evidence-code quality multiplier, also in [0, 1].  When ``None``,
   :data:`DEFAULT_EVIDENCE_WEIGHTS` is used as the fallback.  Supplying a
   partial dict overrides only the codes present; codes absent from the dict
   fall back to the system default, making partial overrides safe.

This two-layer design separates *how much a signal matters* (signal weights)
from *how trustworthy the underlying annotation is* (evidence weights), which
are independent research decisions.

Formulas
--------
linear
    score = Σ(w_i · s_i) / Σ(w_i)  for all active (w_i > 0, s_i available) signals.
evidence_weighted
    Same as linear but the resolved evidence weight is always applied as a
    final multiplier on top of the weighted sum — even when its signal weight is
    0.  This allows down-ranking IEA-sourced predictions regardless of how
    strong the embedding or alignment signals are.

    **Recommended usage**: set ``evidence_weight = 0`` in ``weights`` when
    using this formula.  The multiplier is always applied, so including
    ``evidence_weight`` in the linear sum compounds the evidence signal
    twice — usually not intentional.  If you want a preset that only
    *vetoes* low-evidence predictions without bending the rest of the
    ranking, use ``formula=evidence_weighted`` with ``evidence_weight=0``
    in ``weights``.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from protea.infrastructure.orm.base import Base

# ---------------------------------------------------------------------------
# Formula identifiers
# ---------------------------------------------------------------------------

FORMULA_LINEAR = "linear"
FORMULA_EVIDENCE_WEIGHTED = "evidence_weighted"
VALID_FORMULAS = (FORMULA_LINEAR, FORMULA_EVIDENCE_WEIGHTED)

# ---------------------------------------------------------------------------
# Signal weight defaults
# ---------------------------------------------------------------------------
# Pure embedding similarity by default; all other signals must be opted in.
#
# The block below ``neighbor_vote_fraction`` are the A-SCORE "rich" axes
# (coverage A3, ref-density F, anc2vec G). They default to 0.0 so any config
# that does not mention them scores exactly as it did before they existed
# (a 0-weight signal is skipped in both numerator and denominator).

DEFAULT_WEIGHTS: dict[str, float] = {
    "embedding_similarity": 1.0,
    "identity_nw": 0.0,
    "identity_sw": 0.0,
    "evidence_weight": 0.0,
    "taxonomic_proximity": 0.0,
    "neighbor_vote_fraction": 0.0,
    # --- A-SCORE rich signals (opt-in) ---
    "coverage": 0.0,
    "ref_annotation_density": 0.0,
    "anc2vec_neighbor_cos": 0.0,
    "anc2vec_neighbor_maxcos": 0.0,
}

# ---------------------------------------------------------------------------
# Advanced scoring knobs (the ``params`` JSONB column)
# ---------------------------------------------------------------------------
# Everything below lives in the optional ``ScoringConfig.params`` JSONB. A
# ``None`` / empty params dict reproduces the pre-A-SCORE behaviour byte for
# byte. Each block is independently gated by an ``enabled`` flag (default
# off). See :mod:`protea.core.scoring` for the exact formulas.

IA_PRIOR_SOURCE_FREQUENCY = "frequency"
IA_PRIOR_SOURCE_IA = "ia"
VALID_IA_PRIOR_SOURCES = (IA_PRIOR_SOURCE_FREQUENCY, IA_PRIOR_SOURCE_IA)

#: Allowed top-level keys inside ``ScoringConfig.params``.
VALID_PARAM_KEYS = ("ia_prior", "soft_vote", "calibration")

#: Canonical default for every advanced knob (all off). Used for
#: documentation and as the merge base when reading a partial params dict.
DEFAULT_PARAMS: dict[str, Any] = {
    # IA / term-specificity prior (axis E). Multiplies the composite score by
    # ``prior_t ** gamma`` where ``prior_t in (0, 1]`` is a specificity weight
    # derived either from the term's corpus frequency or from a normalised IA.
    "ia_prior": {
        "enabled": False,
        "gamma": 1.0,
        "source": IA_PRIOR_SOURCE_FREQUENCY,
    },
    # Soft-vote temperature (axis T). RESERVED: a faithful ``exp(sim/T)``
    # neighbour aggregation needs the per-neighbour similarities, which are
    # NOT persisted on ``GOPrediction`` (only the precomputed flat
    # ``neighbor_vote_fraction`` survives). The knob is stored and validated
    # here so A-SCORE.2 can drive it, but it is applied at PREDICT time, not
    # at score time; the score-time scorer treats it as a no-op.
    "soft_vote": {
        "enabled": False,
        "temperature": 1.0,
    },
    # Post-score calibration hook (no-op stub). A-SCORE.2 fills the
    # per-(aspect, IA-bin) isotonic tables; the interface is fixed now so
    # callers never change.
    "calibration": {
        "enabled": False,
    },
}

# ---------------------------------------------------------------------------
# Evidence-code quality weights (default table)
# ---------------------------------------------------------------------------
# These defaults reflect the GO Annotation quality hierarchy.  A ScoringConfig
# may store a full or partial override in its ``evidence_weights`` column.
#
# Sources:
#   GO evidence code definitions — https://geneontology.org/docs/guide-go-evidence-codes/
#   CAFA community consensus: experimental codes provide the highest confidence.
#
# Default tier mapping:
#   Experimental (EXP, IDA, IPI, IMP, IGI, IEP, HTP, HDA, HMP, HGI, HEP,
#                 IC, TAS)                                          → 1.0
#   Electronic annotation (IEA)                                    → 0.8
#   Computational / Phylogenetic (ISS, ISO, ISA, ISM, IGC, IBA,
#                                 IBD, IKR, IRD, RCA)              → 0.7
#   Non-traceable author statement (NAS)                           → 0.5
#   No biological data (ND)                                        → 0.1
#
# IEA is placed above the computational tier: GOA history shows that IEA
# annotations are promoted to an experimental code at a higher rate than
# ISS/IBA/NAS, so their prior quality is underestimated by the classic
# GO-docs hierarchy that ranked IEA below every human-supplied code.

DEFAULT_EVIDENCE_WEIGHTS: dict[str, float] = {
    # Experimental — direct biological evidence
    "EXP": 1.0,  # Inferred from Experiment
    "IDA": 1.0,  # Inferred from Direct Assay
    "IPI": 1.0,  # Inferred from Physical Interaction
    "IMP": 1.0,  # Inferred from Mutant Phenotype
    "IGI": 1.0,  # Inferred from Genetic Interaction
    "IEP": 1.0,  # Inferred from Expression Pattern
    "HTP": 1.0,  # High-Throughput experiment (umbrella)
    "HDA": 1.0,  # High-Throughput Direct Assay
    "HMP": 1.0,  # High-Throughput Mutant Phenotype
    "HGI": 1.0,  # High-Throughput Genetic Interaction
    "HEP": 1.0,  # High-Throughput Expression Pattern
    "IC": 1.0,  # Inferred by Curator
    "TAS": 1.0,  # Traceable Author Statement
    # Computational / Phylogenetic — derived from sequence or phylogeny
    "ISS": 0.7,  # Inferred from Sequence or Structural Similarity
    "ISO": 0.7,  # Inferred from Sequence Orthology
    "ISA": 0.7,  # Inferred from Sequence Alignment
    "ISM": 0.7,  # Inferred from Sequence Model
    "IGC": 0.7,  # Inferred from Genomic Context
    "IBA": 0.7,  # Inferred from Biological aspect of Ancestor
    "IBD": 0.7,  # Inferred from Biological aspect of Descendant
    "IKR": 0.7,  # Inferred from Key Residues
    "IRD": 0.7,  # Inferred from Rapid Divergence
    "RCA": 0.7,  # Inferred from Reviewed Computational Analysis
    # Electronic / author statement — lowest-effort annotation
    "NAS": 0.5,  # Non-traceable Author Statement
    "IEA": 0.8,  # Inferred from Electronic Annotation (automated, bulk)
    # No biological data — used only as a placeholder
    "ND": 0.1,  # No biological Data available
}

#: Ordered grouping of evidence codes used for UI rendering and documentation.
#: Preserves the biological meaning of each tier.
EVIDENCE_CODE_GROUPS: dict[str, list[str]] = {
    "Experimental": [
        "EXP",
        "IDA",
        "IPI",
        "IMP",
        "IGI",
        "IEP",
        "HTP",
        "HDA",
        "HMP",
        "HGI",
        "HEP",
        "IC",
        "TAS",
    ],
    "Computational / Phylogenetic": [
        "ISS",
        "ISO",
        "ISA",
        "ISM",
        "IGC",
        "IBA",
        "IBD",
        "IKR",
        "IRD",
        "RCA",
    ],
    "Electronic": ["NAS", "IEA"],
    "No data": ["ND"],
}

#: Fallback weight applied when a code is not found in any lookup table.
DEFAULT_EVIDENCE_WEIGHT_FALLBACK: float = 0.5


# ---------------------------------------------------------------------------
# ORM model
# ---------------------------------------------------------------------------


class ScoringConfig(Base):
    """Persistent scoring formula definition.

    Instances are stored in the ``scoring_config`` table and referenced by
    evaluation endpoints and the UI scoring selector. Every field that
    influences score computation is serialised, making any result fully
    reproducible by re-applying the same ``ScoringConfig`` to the raw
    ``GOPrediction`` rows.

    The ``formula`` column must be one of :data:`VALID_FORMULAS`. The
    ``weights`` JSONB maps signal keys (see :data:`DEFAULT_WEIGHTS`) to
    their relative weights — a weight of 0 deactivates the signal. The
    optional ``evidence_weights`` JSONB maps GO evidence codes to per-code
    quality multipliers in [0, 1] and falls back to
    :data:`DEFAULT_EVIDENCE_WEIGHTS` for absent codes.
    """

    __tablename__ = "scoring_config"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    formula: Mapped[str] = mapped_column(String(50), nullable=False, default=FORMULA_LINEAR)
    weights: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    evidence_weights: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    #: Optional advanced scoring knobs (IA prior, soft-vote temperature,
    #: calibration). ``None`` reproduces the pre-A-SCORE behaviour exactly.
    #: See :data:`DEFAULT_PARAMS` for the schema and :mod:`protea.core.scoring`
    #: for the formulas.
    params: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return (
            f"<ScoringConfig id={self.id} name={self.name!r}"
            f" formula={self.formula!r}"
            f" evidence_weights={'custom' if self.evidence_weights else 'default'}"
            f" params={'custom' if self.params else 'default'}>"
        )
