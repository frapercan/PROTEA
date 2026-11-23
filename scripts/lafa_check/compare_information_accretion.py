"""PROTEA's information accretion under LAFA's cohort against LAFA's own table, and its record.

Computes, in process and read only, the table ``compute_information_accretion``
would write for the start cut's corpus and ontology with the evidence regime
``lafa`` and the cohort ``swissprot``, and compares it term by term with the
``lafa/t0/IA.tsv`` that ``run_lafa.sh`` wrote. Writes ``<record>``. Run it with
the store's environment::

    python scripts/lafa_check/compare_information_accretion.py <out_dir> <cut> <record.json> <protea_commit>
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from protea.core._window_population import swissprot_of_corpus
from protea.core.ia import build_ancestors, ia_over_components
from protea.core.ia_regimes import resolve_regime
from protea.core.lafa_frame import LAFA_PACKAGE
from protea.core.operations.compute_information_accretion import (
    ComputeInformationAccretionOperation,
    corpus_rows,
)


def _protea_table(cut: int) -> tuple[dict[str, float], dict[str, object]]:
    operation = ComputeInformationAccretionOperation()
    engine = create_engine(os.environ["PROTEA_DB_URL"], future=True)
    with Session(engine) as session:
        session.execute(text("SET TRANSACTION READ ONLY"))
        corpus, snapshot, version, release = session.execute(
            text(
                "SELECT p.annotation_set_id, p.ontology_snapshot_id, o.obo_version, r.release "
                "FROM cut_photo p JOIN ontology_snapshot o ON o.id = p.ontology_snapshot_id "
                "JOIN swissprot_release r ON r.id = p.swissprot_release_id WHERE p.cut_release = :cut"
            ),
            {"cut": cut},
        ).one()
        terms, _aspects, parents = operation._load_dag(session, str(snapshot))
        ancestors = build_ancestors(parents)
        for term in terms:
            ancestors.setdefault(term, frozenset({term}))
        cohort = swissprot_of_corpus(session, corpus)
        per_term, raw, dropped, proteins, outside = operation._propagate(
            corpus_rows(session, str(corpus), resolve_regime("lafa")),
            ancestors,
            lambda *a, **k: None,
            cohort,
        )
    counts = {
        "cut": cut,
        "ontology": version,
        "swissprot": release,
        "rows_counted": raw,
        "rows_dropped_not_live": dropped,
        "rows_outside_cohort": outside,
        "proteins": len(proteins),
        "cohort": len(cohort),
    }
    return ia_over_components(terms, parents, per_term), counts


def main(out: Path, cut: int, record_path: Path, protea_commit: str) -> None:
    theirs_path = out / "lafa" / "t0" / "IA.tsv"
    theirs = {}
    for line in theirs_path.read_text().splitlines():
        fields = line.split("\t")
        if len(fields) >= 2 and fields[0].startswith("GO:"):
            theirs[fields[0]] = float(fields[1])
    ours, counts = _protea_table(cut)
    common = ours.keys() & theirs.keys()
    differences = sorted((abs(ours[term] - theirs[term]), term) for term in common)
    record = {
        **counts,
        "protea_commit": protea_commit,
        "lafa_package": LAFA_PACKAGE,
        "lafa_table_sha256": hashlib.sha256(theirs_path.read_bytes()).hexdigest(),
        "terms_protea": len(ours),
        "terms_lafa": len(theirs),
        "terms_in_both": len(common),
        "max_abs_difference": differences[-1][0] if differences else None,
        "terms_equal_within_1e-9": sum(1 for gap, _ in differences if gap <= 1e-9),
        "examples_only_protea": sorted(ours.keys() - theirs.keys())[:5],
        "examples_only_lafa": sorted(theirs.keys() - ours.keys())[:5],
    }
    record_path.write_text(json.dumps(record, indent=1) + "\n")


if __name__ == "__main__":
    main(Path(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3]), sys.argv[4])
