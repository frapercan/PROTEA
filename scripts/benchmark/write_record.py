"""The record of a store's benchmark: its standing LAFA windows, what they replaced, and their IA tables.

Reads the store, read only, and writes one JSON document: for every window of
LAFA's frame that nothing supersedes, its two corpora and ontologies, its role,
rules, population, code revision and the counts its build recorded; the windows
it supersedes, with the note of the mark; and the information accretion table
LAFA scores it with, the one over its start's corpus and ontology with the regime
``lafa`` and the cohort ``swissprot``. Run it with the store's environment::

    python scripts/benchmark/write_record.py <record.json>
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

#: The counts a window's build records that a reader of the benchmark needs.
_WINDOW_COUNTS = (
    "delta_proteins",
    "nk_proteins",
    "lk_proteins",
    "pk_proteins",
    "nk_annotations",
    "lk_annotations",
    "pk_annotations",
    "population_proteins",
    "population_in_both",
    "population_sequence_changed",
    "population_swissprot_releases",
    "terms_of_interest",
    "lafa_pk_proteins_made_by_the_ontology",
    "lafa_pk_pairs_made_by_the_ontology",
    "lafa_workflow",
    "lafa_package",
)

_WINDOWS = text(
    """
    SELECT e.id, e.window_role, e.target_population, e.truth_rules, e.code_revision, e.job_id,
           e.stats, e.old_annotation_set_id, e.old_native_snapshot_id,
           o.cut_release AS start_cut, n.cut_release AS end_cut,
           so.obo_version AS start_ontology, sn.obo_version AS end_ontology
    FROM evaluation_set e
    JOIN cut_photo o ON o.annotation_set_id = e.old_annotation_set_id
    JOIN cut_photo n ON n.annotation_set_id = e.new_annotation_set_id
    JOIN ontology_snapshot so ON so.id = e.old_native_snapshot_id
    JOIN ontology_snapshot sn ON sn.id = e.new_native_snapshot_id
    WHERE e.truth_profile = 'own_graph' AND e.superseded_by_id IS NULL
    ORDER BY o.cut_release
    """
)


def _superseded(session: Session, window_id: object) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            "SELECT id, target_population, truth_rules ->> 'profile', supersession_note "
            "FROM evaluation_set WHERE superseded_by_id = :window ORDER BY target_population"
        ),
        {"window": window_id},
    )
    return [
        {"id": str(row[0]), "target_population": row[1], "profile": row[2], "note": row[3]}
        for row in rows
    ]


def _ia_table(session: Session, corpus: object, ontology: object) -> dict[str, Any] | None:
    row = session.execute(
        text(
            "SELECT id, term_count, protein_count, annotation_count, ia_max, ia_mean, "
            "content_sha256, code_revision, stats ->> 'rows_outside_cohort' "
            "FROM information_accretion_set WHERE annotation_set_id = :corpus "
            "AND ontology_snapshot_id = :ontology AND evidence_regime = 'lafa' "
            "AND cohort = 'swissprot'"
        ),
        {"corpus": corpus, "ontology": ontology},
    ).one_or_none()
    if row is None:
        return None
    return {
        "id": str(row[0]),
        "evidence_regime": "lafa",
        "cohort": "swissprot",
        "terms": row[1],
        "proteins": row[2],
        "rows_counted": row[3],
        "rows_outside_cohort": int(row[8]) if row[8] is not None else None,
        "ia_max": round(row[4], 6),
        "ia_mean": round(row[5], 6),
        "content_sha256": row[6],
        "code_revision": row[7],
    }


def main(record_path: Path) -> None:
    engine = create_engine(os.environ["PROTEA_DB_URL"], future=True)
    windows = []
    with Session(engine) as session:
        session.execute(text("SET TRANSACTION READ ONLY"))
        for row in session.execute(_WINDOWS).mappings():
            stats = row["stats"] or {}
            windows.append(
                {
                    "window": f"{row['start_cut']} -> {row['end_cut']}",
                    "id": str(row["id"]),
                    "role": row["window_role"],
                    "ontologies": [row["start_ontology"], row["end_ontology"]],
                    "target_population": row["target_population"],
                    "truth_rules": row["truth_rules"],
                    "code_revision": row["code_revision"],
                    "job_id": str(row["job_id"]) if row["job_id"] else None,
                    "counts": {key: stats.get(key) for key in _WINDOW_COUNTS},
                    "information_accretion": _ia_table(
                        session, row["old_annotation_set_id"], row["old_native_snapshot_id"]
                    ),
                    "supersedes": _superseded(session, row["id"]),
                }
            )
    record_path.write_text(json.dumps({"windows": windows}, indent=1, default=str) + "\n")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
