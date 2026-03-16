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

DEFAULT_WEIGHTS: dict[str, float] = {
    "embedding_similarity": 1.0,
    "identity_nw": 0.0,
    "identity_sw": 0.0,
    "evidence_weight": 0.0,
    "taxonomic_proximity": 0.0,
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
#   Computational / Phylogenetic (ISS, ISO, ISA, ISM, IGC, IBA,
#                                 IBD, IKR, IRD, RCA)              → 0.7
#   Non-traceable author statement (NAS)                           → 0.5
#   Electronic annotation (IEA)                                    → 0.3
#   No biological data (ND)                                        → 0.1

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
    "IEA": 0.3,  # Inferred from Electronic Annotation (automated, bulk)
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
    evaluation endpoints and the UI scoring selector.  Every field that
    influences score computation is serialised, making any result fully
    reproducible by re-applying the same ``ScoringConfig`` to the raw
    ``GOPrediction`` rows.

    Attributes
    ----------
    id:
        UUID primary key.
    name:
        Human-readable label shown in the UI dropdown.
    formula:
        One of :data:`VALID_FORMULAS` — controls how the weighted average is
        combined with the evidence multiplier.
    weights:
        JSONB dict mapping signal keys to their relative weights.  Valid keys
        are the ones in :data:`DEFAULT_WEIGHTS`.  Weights of 0 deactivate a
        signal; absent keys are treated as 0.
    evidence_weights:
        Optional JSONB dict mapping GO evidence codes (e.g. ``"IEA"``) to
        per-code quality multipliers in [0, 1].  When ``None`` the system falls
        back to :data:`DEFAULT_EVIDENCE_WEIGHTS`.  Partial dicts are allowed:
        codes absent from the override still resolve via the default table.
    description:
        Free-text description shown as a tooltip in the UI.
    created_at:
        UTC timestamp set by the database at insert time.
    """

    __tablename__ = "scoring_config"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    formula: Mapped[str] = mapped_column(String(50), nullable=False, default=FORMULA_LINEAR)
    weights: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    evidence_weights: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return (
            f"<ScoringConfig id={self.id} name={self.name!r}"
            f" formula={self.formula!r}"
            f" evidence_weights={'custom' if self.evidence_weights else 'default'}>"
        )
