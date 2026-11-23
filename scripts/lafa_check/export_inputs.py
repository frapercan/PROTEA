"""LAFA's inputs for one window, from the store, and the frame's truth in LAFA's files.

Writes, under ``<out_dir>``:

* ``inputs/{t0,t1}/go-basic.obo``: the ontology each end's corpus was loaded
  under, from the artifact store, its recorded sha256 verified;
* ``inputs/{t0,t1}/goa_uniprot_all.gaf.gz``: each end's rows with the thirteen
  experimental codes, NOT rows included, as GAF 2.2 sorted by every column, so
  the file and its digest are a function of the store alone. The taxon column
  is the protein's when the store has it and the root otherwise; LAFA's steps
  never read it;
* ``inputs/{t0,t1}/uniprot_sprot.fasta.gz``: copied from the Swiss-Prot release
  archive the end's cut photo names, which ``load_swissprot_release`` cached;
* ``frame/{as_published,with_the_not_rule}/groundtruth_*.tsv``: the frame's NK,
  LK, PK, PK known and targets on LAFA's targets, without and with the author's
  NOT rule, and ``frame/groundtruth_terms_of_interest.txt``.

Read only: the transaction is declared so. Run it with the store's environment::

    python scripts/lafa_check/export_inputs.py <out_dir> <start_cut> <end_cut> <swissprot_cache>
"""

from __future__ import annotations

import gzip
import json
import os
import sys
import tarfile
import tempfile
from collections import defaultdict
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from protea.core._lafa_frame_data import load_end_ontology, load_end_rows
from protea.core._window_population import restricted_to, same_sequence_at_both_ends
from protea.core.evidence_codes import EXPERIMENTAL
from protea.core.lafa_frame import LafaInputs, lafa_truth, terms_of_interest
from protea.core.operations._evaluation_artifacts import resolve_obo
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot

_ROWS = text(
    """
    SELECT pga.protein_accession, COALESCE(pga.qualifier, ''), gt.go_id,
           COALESCE(pga.db_reference, ''), pga.evidence_code, COALESCE(pga.with_from, ''),
           gt.aspect, p.taxonomy_id, pga.annotation_date, COALESCE(pga.assigned_by, '')
    FROM protein_go_annotation pga
    JOIN go_term gt ON gt.id = pga.go_term_id
    LEFT JOIN protein p ON p.accession = pga.protein_accession
    WHERE pga.annotation_set_id = :set_id AND pga.evidence_code = ANY(:codes)
    ORDER BY 1, 2, 3, 4, 5, 6
    """
)


def _gaf(session: Session, corpus: object, path: Path) -> int:
    written = 0
    with gzip.open(path, "wt") as gaf:
        gaf.write("!gaf-version: 2.2\n")
        rows = session.execute(_ROWS, {"set_id": corpus, "codes": sorted(EXPERIMENTAL)})
        for accession, qualifier, go_id, ref, code, with_from, aspect, taxon, day, by in rows:
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


def _fasta(cache: Path, release: str, path: Path) -> None:
    archive = cache / release / f"uniprot_sprot-only{release}.tar.gz"
    with tarfile.open(archive) as tar, open(path, "wb") as out:
        member = tar.extractfile("uniprot_sprot.fasta.gz")
        assert member is not None, f"{archive} holds no uniprot_sprot.fasta.gz"
        out.write(member.read())


def _write_truth(folder: Path, data: object, aspect_of: dict[str, str]) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    buckets = {"NK": data.nk, "LK": data.lk, "PK": data.pk, "PK_known": data.pk_known}
    for name, bucket in buckets.items():
        with open(folder / f"groundtruth_{name}.tsv", "w") as out:
            out.write("EntryID\tterm\taspect\n")
            for protein in sorted(bucket):
                for term in sorted(bucket[protein]):
                    out.write(f"{protein}\t{term}\t{aspect_of[term]}\n")
    targets = sorted(set(data.nk) | set(data.lk) | set(data.pk))
    (folder / "groundtruth_targets.tsv").write_text("".join(f"{p}\n" for p in targets))


def main(out: Path, start_cut: int, end_cut: int, cache: Path) -> None:
    engine = create_engine(os.environ["PROTEA_DB_URL"], future=True)
    record: dict[str, object] = {"window": f"{start_cut} -> {end_cut}"}
    with Session(engine) as session:
        session.execute(text("SET TRANSACTION READ ONLY"))
        photos = {
            cut: (corpus, snapshot, release)
            for cut, corpus, snapshot, release in session.execute(
                text(
                    "SELECT p.cut_release, p.annotation_set_id, p.ontology_snapshot_id, r.release "
                    "FROM cut_photo p JOIN swissprot_release r ON r.id = p.swissprot_release_id"
                )
            )
        }
        for end, cut in (("t0", start_cut), ("t1", end_cut)):
            corpus, snapshot_id, release = photos[cut]
            folder = out / "inputs" / end
            folder.mkdir(parents=True, exist_ok=True)
            snapshot = session.get(OntologySnapshot, snapshot_id)
            with tempfile.TemporaryDirectory() as tmp:
                (folder / "go-basic.obo").write_bytes(
                    Path(resolve_obo(tmp, snapshot, lambda *a, **k: None)).read_bytes()
                )
            record[f"{end}_rows"] = _gaf(session, corpus, folder / "goa_uniprot_all.gaf.gz")
            _fasta(cache, release, folder / "uniprot_sprot.fasta.gz")
            record[f"{end}_sources"] = {
                "cut": cut,
                "ontology": snapshot.obo_version,
                "swissprot": release,
            }
        (old_set, t0_id, _), (new_set, t1_id, _) = photos[start_cut], photos[end_cut]
        t0, t1 = load_end_ontology(session, t0_id), load_end_ontology(session, t1_id)
        t0_rows, t0_not = load_end_rows(session, old_set, True)
        t1_rows, t1_not = load_end_rows(session, new_set, True)
        targets = same_sequence_at_both_ends(session, old_set, new_set)
    negations: dict[str, set[str]] = defaultdict(set)
    for side in (t0_not, t1_not):
        for protein, ids in side.items():
            negations[protein] |= ids
    aspect_of = {**t0.live, **t1.live}
    for variant, denied in (("as_published", {}), ("with_the_not_rule", dict(negations))):
        data, _ = lafa_truth(LafaInputs(t0, t1, t0_rows, t1_rows, denied))
        kept, _ = restricted_to(data, targets.accessions)
        _write_truth(out / "frame" / variant, kept, aspect_of)
    toi = sorted(terms_of_interest(t0, t1))
    (out / "frame" / "groundtruth_terms_of_interest.txt").write_text("\n".join(toi))
    (out / "export.json").write_text(json.dumps(record, indent=1, default=str))


if __name__ == "__main__":
    main(Path(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), Path(sys.argv[4]))
