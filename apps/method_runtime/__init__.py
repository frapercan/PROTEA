"""protea-method-runtime container entry package.

Holds the thin CLI wrapper and the ProtT5 query encoder used by the
Tier 3 distribution channel (ADR-D15). Compute logic lives in the
``protea_method`` library; this package owns only argparse, FASTA
parsing, and TSV emission.
"""
