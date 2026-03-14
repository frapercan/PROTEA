"""GO evidence code utilities.

Provides normalisation from ECO IDs to canonical GO evidence codes and
classification of codes as experimental vs. non-experimental.

Mapping source (canonical ECO → GO code):
  https://github.com/evidenceontology/evidenceontology/blob/master/gaf-eco-mapping.txt

Additional ECO IDs used by UniProt GAF (not in the canonical mapping file)
are resolved here using the EBI QuickGO ECO term definitions; all are
"used in automatic assertion" and therefore map to IEA.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# ECO ID → GO root evidence code
# Canonical entries from gaf-eco-mapping.txt (reversed) +
# common UniProt-specific subterms.
# ---------------------------------------------------------------------------
ECO_TO_CODE: dict[str, str] = {
    # Canonical mappings (gaf-eco-mapping.txt)
    "ECO:0000269": "EXP",
    "ECO:0007005": "HDA",
    "ECO:0007007": "HEP",
    "ECO:0007003": "HGI",
    "ECO:0007001": "HMP",
    "ECO:0006056": "HTP",
    "ECO:0000318": "IBA",
    "ECO:0000319": "IBD",
    "ECO:0000305": "IC",
    "ECO:0000314": "IDA",
    "ECO:0000501": "IEA",
    "ECO:0000270": "IEP",
    "ECO:0000317": "IGC",
    "ECO:0000316": "IGI",
    "ECO:0000320": "IKR",
    "ECO:0000315": "IMP",
    "ECO:0000353": "IPI",
    "ECO:0000321": "IRD",
    "ECO:0000247": "ISA",
    "ECO:0000255": "ISM",
    "ECO:0000266": "ISO",
    "ECO:0000250": "ISS",
    "ECO:0000303": "NAS",
    "ECO:0000307": "ND",
    "ECO:0000245": "RCA",
    "ECO:0000304": "TAS",
    # UniProt-specific subterms (all "used in automatic assertion" → IEA)
    "ECO:0000256": "IEA",  # match to sequence model evidence, automatic assertion
    "ECO:0000366": "IEA",  # logical inference from automatic annotation
    "ECO:0007322": "IEA",  # curator inference, automatic assertion
    "ECO:0007826": "IEA",  # phylogenetic evidence, automatic assertion
}

# ---------------------------------------------------------------------------
# Experimental evidence codes (used as ground truth in CAFA evaluation).
# Only annotations with these codes are considered when building ground truth.
# ---------------------------------------------------------------------------
EXPERIMENTAL: frozenset[str] = frozenset({
    "EXP",  # Inferred from Experiment
    "IDA",  # Inferred from Direct Assay
    "IPI",  # Inferred from Physical Interaction
    "IMP",  # Inferred from Mutant Phenotype
    "IGI",  # Inferred from Genetic Interaction
    "IEP",  # Inferred from Expression Pattern
    "HTP",  # High Throughput
    "HDA",  # High Throughput Direct Assay
    "HMP",  # High Throughput Mutant Phenotype
    "HGI",  # High Throughput Genetic Interaction
    "HEP",  # High Throughput Expression Pattern
    "IC",   # Inferred by Curator
    "TAS",  # Traceable Author Statement
})


def normalize(code: str) -> str:
    """Return the canonical GO evidence code for *code*.

    If *code* is already a GO root evidence code it is returned as-is.
    ECO IDs are translated via :data:`ECO_TO_CODE`.
    Unknown codes are returned unchanged so that no information is silently
    discarded.

    Examples::

        normalize("IDA")           # → "IDA"
        normalize("ECO:0000314")   # → "IDA"
        normalize("ECO:0000256")   # → "IEA"
        normalize("UNKNOWN")       # → "UNKNOWN"
    """
    if code in ECO_TO_CODE:
        return ECO_TO_CODE[code]
    return code


def is_experimental(code: str) -> bool:
    """Return True if *code* (GO or ECO) represents experimental evidence.

    Examples::

        is_experimental("IDA")          # True
        is_experimental("ECO:0000314")  # True  (IDA)
        is_experimental("IEA")          # False
        is_experimental("ECO:0000501")  # False (IEA)
    """
    return normalize(code) in EXPERIMENTAL
