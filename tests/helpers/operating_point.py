"""A tiny window the evaluator can score for real: an ontology, its truth, its weights.

Two namespaces, so cells exist in more than one aspect; a ``part_of`` edge and an
alternative id, so the ancestry is the evaluator's and not a reading of ``is_a``
alone; ground truth in all three settings, with known terms for PK; and
information accretion that is zero at the roots, so the weighted population is
not the unweighted one. Scores carry more than four decimals and include
children scored above their parents, so the rendering and the restoration both
have something to do.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from protea.core.operating_point.grid import EvaluatorInputs, score_table

OBO = """format-version: 1.2

[Term]
id: GO:0008150
name: biological_process
namespace: biological_process

[Term]
id: GO:0000002
name: bp_mid
namespace: biological_process
is_a: GO:0008150 ! biological_process
alt_id: GO:0000092

[Term]
id: GO:0000003
name: bp_leaf_a
namespace: biological_process
is_a: GO:0000002 ! bp_mid

[Term]
id: GO:0000004
name: bp_leaf_b
namespace: biological_process
is_a: GO:0000002 ! bp_mid

[Term]
id: GO:0000005
name: bp_part
namespace: biological_process
relationship: part_of GO:0000003 ! bp_leaf_a

[Term]
id: GO:0000006
name: bp_deep
namespace: biological_process
is_a: GO:0000004 ! bp_leaf_b

[Term]
id: GO:0003674
name: molecular_function
namespace: molecular_function

[Term]
id: GO:0000011
name: mf_mid
namespace: molecular_function
is_a: GO:0003674 ! molecular_function

[Term]
id: GO:0000012
name: mf_leaf_a
namespace: molecular_function
is_a: GO:0000011 ! mf_mid

[Term]
id: GO:0000013
name: mf_leaf_b
namespace: molecular_function
is_a: GO:0000011 ! mf_mid

[Term]
id: GO:0000099
name: retired
namespace: biological_process
is_obsolete: true
"""

IA = (
    "GO:0008150\t0.0\nGO:0000002\t0.7\nGO:0000003\t1.6\nGO:0000004\t1.1\n"
    "GO:0000005\t2.3\nGO:0000006\t2.9\nGO:0003674\t0.0\nGO:0000011\t0.4\n"
    "GO:0000012\t1.9\nGO:0000013\t2.4\n"
)

TERMS = (
    "GO:0008150",
    "GO:0000002",
    "GO:0000003",
    "GO:0000004",
    "GO:0000005",
    "GO:0000006",
    "GO:0003674",
    "GO:0000011",
    "GO:0000012",
    "GO:0000013",
)

TRUTH = {
    "NK": "P1\tGO:0000005\nP1\tGO:0000012\nP2\tGO:0000006\nP3\tGO:0000013\nP4\tGO:0000003\n"
    "P4\tGO:0000012\n",
    "LK": "P5\tGO:0000004\nP6\tGO:0000012\nP9\tGO:0000006\n",
    "PK": "P7\tGO:0000005\nP7\tGO:0000003\nP8\tGO:0000013\nP8\tGO:0000011\nP10\tGO:0000006\n",
}

KNOWN = "P7\tGO:0000003\nP8\tGO:0000011\n"

#: ``(protein, term, score)``: off the four-decimal grid, with children above parents.
SCORES: tuple[tuple[str, str, float], ...] = (
    ("P1", "GO:0000005", 0.912345),
    ("P1", "GO:0000003", 0.41234),
    ("P1", "GO:0000012", 0.55555),
    ("P1", "GO:0000013", 0.3333333),
    ("P2", "GO:0000006", 0.734567),
    ("P2", "GO:0000002", 0.2),
    ("P2", "GO:0000092", 0.25),
    ("P3", "GO:0000012", 0.61111),
    ("P3", "GO:0000011", 0.123456),
    ("P4", "GO:0000003", 0.87654),
    ("P4", "GO:0000004", 0.45678),
    ("P4", "GO:0000012", 0.29999),
    ("P5", "GO:0000004", 0.70707),
    ("P5", "GO:0000006", 0.85858),
    ("P6", "GO:0000013", 0.66666),
    ("P6", "GO:0000099", 0.95),
    ("P7", "GO:0000005", 0.77777),
    ("P7", "GO:0000004", 0.35353),
    ("P8", "GO:0000013", 0.51515),
    ("P8", "GO:0000012", 0.48484),
    ("P9", "GO:0000006", 0.0312),
    ("P10", "GO:0000006", 0.64646),
    ("P10", "GO:0000003", 0.71717),
)


def raw_scores() -> pa.Table:
    """The fixture's scores as a table of :data:`protea.core.operating_point.grid.SCORE_COLUMNS`."""
    return score_table(*zip(*SCORES, strict=True))


def write_frame(root: Path) -> EvaluatorInputs:
    """The ontology, the weights, the truth of each setting and the known terms, on disk."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "go.obo").write_text(OBO)
    (root / "ia.tsv").write_text(IA)
    (root / "terms_of_interest.txt").write_text("".join(f"{t}\n" for t in TERMS))
    truth = {}
    for setting, body in TRUTH.items():
        path = root / f"{setting.lower()}.tsv"
        path.write_text(body)
        truth[setting] = str(path)
    (root / "pk_known.tsv").write_text(KNOWN)
    return EvaluatorInputs(
        obo_path=str(root / "go.obo"),
        ground_truth=truth,
        pk_known_path=str(root / "pk_known.tsv"),
        toi_path=str(root / "terms_of_interest.txt"),
        ia_path=str(root / "ia.tsv"),
    )
