"""Evidence regimes for Information Accretion corpora.

IA is not a property of an ontology. It is estimated from term frequencies over
an annotation corpus, so an IA table is identified by THREE axes:

    (ontology_snapshot, annotation_set, evidence_regime)

The third axis is the one that is easiest to lose and the most expensive to get
wrong. Measured on the v226 pivot (``releases/2025-03-16``) against the GOA
v226 corpus, which is 89.8 percent IEA:

    IA (all evidence)   max 18.956   mean 3.261
    IA (lafa regime)    max 15.943   mean 2.681
    IA_cafa6.tsv        max 15.880   mean 2.647

    lafa regime  vs IA_cafa6   mean abs diff 0.0918   pearson r 0.9924
    all evidence vs IA_cafa6   mean abs diff 0.8081   pearson r 0.9495

Restricting to the scored evidence regime moves the table 8.8x closer to the
reference. An IA computed over all evidence mostly measures how ubiquitous
electronic annotation is, which is not the quantity CAFA weights by.

A regime name is stored on the artifact alongside the RESOLVED code list, so a
table produced today keeps its meaning even if a regime definition is later
revised.
"""

from __future__ import annotations

from types import MappingProxyType

# The evidence set LAFA scores on: experimental, high-throughput experimental,
# plus IC and TAS. This is the regime to use for board-comparable IA.
LAFA_EVIDENCE: tuple[str, ...] = (
    "EXP", "IDA", "IPI", "IMP", "IGI", "IEP",
    "HTP", "HDA", "HMP", "HGI", "HEP",
    "IC", "TAS",
)

# The strict experimental six, without the high-throughput variants and
# without the curator-inference codes.
EXPERIMENTAL_EVIDENCE: tuple[str, ...] = (
    "EXP", "IDA", "IPI", "IMP", "IGI", "IEP",
)

# ``None`` means no evidence predicate at all: every annotation in the set.
EVIDENCE_REGIMES: MappingProxyType[str, tuple[str, ...] | None] = MappingProxyType(
    {
        "lafa": LAFA_EVIDENCE,
        "experimental": EXPERIMENTAL_EVIDENCE,
        "all": None,
    }
)

#: The default is deliberately the board-comparable one. An IA table computed
#: over all evidence is a valid object but answers a different question, so it
#: has to be asked for by name.
DEFAULT_REGIME = "lafa"


def resolve_regime(name: str) -> tuple[str, ...] | None:
    """Resolve a regime name to its evidence code tuple.

    Returns ``None`` for the unrestricted regime. Raises ``ValueError`` for an
    unknown name rather than falling back to a default: silently widening the
    corpus is exactly the failure this module exists to prevent.
    """
    if name not in EVIDENCE_REGIMES:
        raise ValueError(
            f"unknown evidence regime {name!r}; "
            f"known regimes: {sorted(EVIDENCE_REGIMES)}"
        )
    return EVIDENCE_REGIMES[name]
