"""A window's targets and their own rows at t0, for democafa's goa_nonexp oracle.

Writes, under ``<out_dir>``:

* ``targets.txt``: the window's targets, one accession per line, read as every
  native writer reads them (its declared target set, or its truth's targets);
* ``inputs/t0/goa_targets.gaf.gz``: every row of those targets in the window's t0
  annotation set, every code, NOT rows included, as GAF 2.2 sorted by every column,
  so the file and its digest are a function of the store alone (the taxon column is
  the protein's when the store has it and the root otherwise; democafa never reads it);
* ``inputs/t0/go-basic.obo``: the window's t0 ontology, from the artifact store, its
  recorded sha256 verified.

Read only: the transaction is declared so. Run it with the store's environment::

    python scripts/lafa_check/baselines/export_targets.py <out_dir> <evaluation_set_id>
"""

from __future__ import annotations

import gzip
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from protea.core.method_write import window_targets
from protea.core.operations._evaluation_artifacts import resolve_obo
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot

_ROWS = text(
    """
    SELECT pga.protein_accession, COALESCE(pga.qualifier, ''), gt.go_id,
           COALESCE(pga.db_reference, ''), pga.evidence_code, COALESCE(pga.with_from, ''),
           gt.aspect, p.taxonomy_id, pga.annotation_date, COALESCE(pga.assigned_by, '')
    FROM protein_go_annotation pga
    JOIN go_term gt ON gt.id = pga.go_term_id
    LEFT JOIN protein p ON p.accession = pga.protein_accession
    WHERE pga.annotation_set_id = :set_id AND pga.protein_accession = ANY(:targets)
    ORDER BY 1, 2, 3, 4, 5, 6
    """
)


def write_gaf(rows: object, path: Path) -> int:
    """GAF 2.2 lines, one per row, in the columns democafa's reader takes."""
    written = 0
    with gzip.open(path, "wt") as gaf:
        gaf.write("!gaf-version: 2.2\n")
        for accession, qualifier, go_id, ref, code, with_from, aspect, taxon, day, by in rows:  # type: ignore[attr-defined]
            date = (
                day.strftime("%Y%m%d")
                if hasattr(day, "strftime")
                else str(day or "").replace("-", "")
            )
            fields = [
                "UniProtKB",
                accession,
                accession,
                qualifier,
                go_id,
                ref,
                code,
                with_from,
                aspect,
                "",
                "",
                "protein",
                f"taxon:{taxon or 1}",
                date or "20000101",
                by,
                "",
                "",
            ]
            gaf.write("\t".join(fields) + "\n")
            written += 1
    return written


def main(out: Path, evaluation_set_id: str) -> None:
    engine = create_engine(os.environ["PROTEA_DB_URL"], future=True)
    folder = out / "inputs" / "t0"
    folder.mkdir(parents=True, exist_ok=True)
    with Session(engine) as session:
        session.execute(text("SET TRANSACTION READ ONLY"))
        eset = session.get(EvaluationSet, uuid.UUID(evaluation_set_id))
        if eset is None:
            raise SystemExit(f"no window {evaluation_set_id}")
        targets, declared = window_targets(session, eset)
        (out / "targets.txt").write_text("".join(f"{t}\n" for t in targets))
        rows = session.execute(_ROWS, {"set_id": eset.old_annotation_set_id, "targets": targets})
        written = write_gaf(rows, folder / "goa_targets.gaf.gz")
        snapshot = session.get(OntologySnapshot, eset.old_native_snapshot_id)
        with tempfile.TemporaryDirectory() as tmp:
            obo = Path(resolve_obo(tmp, snapshot, lambda *a, **k: None))
            (folder / "go-basic.obo").write_bytes(obo.read_bytes())
    record = {
        "evaluation_set_id": evaluation_set_id,
        "targets": len(targets),
        "declared_target_set": str(declared.query_set_id) if declared else None,
        "rows": written,
        "ontology": snapshot.obo_version if snapshot else None,
    }
    (out / "export_targets.json").write_text(json.dumps(record, indent=1))


if __name__ == "__main__":
    main(Path(sys.argv[1]), sys.argv[2])
