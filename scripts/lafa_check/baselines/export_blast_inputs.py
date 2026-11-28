"""The hits and donor terms behind a top-hit homology transfer set, as democafa's scorer reads them.

Writes, under ``<out_dir>/blast``:

* ``blast.tsv``: BLAST tabular without a header (``qseqid sseqid evalue length pident
  nident``): one line per target accession and admitted donor accession with a state of
  each hit the set read, ``pident = 100 x fident`` and ``nident = round(fident x
  alnlen)``, and one per twin at pident 100 and e-value 0;
* ``donor_terms.tsv``: ``EntryID term aspect`` of every donor's state at t0 as the set
  read it (closed, NOT terms out, roots out);
* ``targets.txt``: the targets with a sequence.

It reads the inputs exactly as ``run_homology_transfer`` reads them, with the set's own
recipe and donor policy (``resolve`` and ``gather``), so the oracle scores the same hits
and the same donors. Read only: the transaction is declared so. Run it with the store's
environment::

    python scripts/lafa_check/baselines/export_blast_inputs.py <out_dir> <prediction_set_id>
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from protea.core.method_write import window_targets
from protea.core.operations import run_homology_transfer as op
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.method.method import Method


def payload_of(pset: PredictionSet, method: Method) -> op.RunHomologyTransferPayload:
    """The payload that wrote the set, from its method's recipe and its meta."""
    recipe = method.recipe or {}
    if recipe.get("rule") != "top_hit_identity":
        raise SystemExit(
            f"set {pset.id} is a {recipe.get('rule')} set: the BLAST oracle scores the top hit"
        )
    return op.RunHomologyTransferPayload.model_validate(
        {
            "evaluation_set_id": pset.meta["evaluation_set_id"],
            "rule": recipe["rule"],
            "identity": recipe.get("identity"),
            "hit_cap": recipe.get("hit_cap"),
            "donor_policy": pset.meta["donor_policy"],
            "self_exclusion": pset.meta["self_exclusion"],
        }
    )


def main(out: Path, prediction_set_id: str) -> None:
    engine = create_engine(os.environ["PROTEA_DB_URL"], future=True)
    folder = out / "blast"
    folder.mkdir(parents=True, exist_ok=True)
    with Session(engine) as session:
        session.execute(text("SET TRANSACTION READ ONLY"))
        pset = session.get(PredictionSet, uuid.UUID(prediction_set_id))
        method = session.get(Method, pset.method_id) if pset and pset.method_id else None
        if pset is None or method is None:
            raise SystemExit(f"{prediction_set_id} is not a set a method wrote")
        p = payload_of(pset, method)
        src = op.resolve(session, p)
        targets, _ = window_targets(session, src.eset)
        data = op.gather(session, src, p, targets)
    lines = 0
    with open(folder / "blast.tsv", "w") as blast:
        for accession in data.targets:
            if accession not in data.md5_of:
                continue
            donors = op.donors_for(data, accession, p.self_exclusion)
            for hit in op.target_hits(data, data.md5_of[accession], p.rule_of()):
                for donor in donors.get(hit.md5, []):
                    if donor not in data.states.terms:
                        continue
                    evalue = 0.0 if hit.twin else hit.evalue
                    nident = round(hit.fident * hit.alnlen)
                    blast.write(
                        f"{accession}\t{donor}\t{evalue}\t{hit.alnlen}\t{100 * hit.fident}\t{nident}\n"
                    )
                    lines += 1
    with open(folder / "donor_terms.tsv", "w") as terms:
        terms.write("EntryID\tterm\taspect\n")
        go_ids, aspects = data.states.go_ids, data.states.aspects
        for donor in sorted(data.states.terms):
            for index in sorted(data.states.terms[donor]):
                terms.write(f"{donor}\t{go_ids[index]}\t{aspects[index]}\n")
    (folder / "targets.txt").write_text("".join(f"{t}\n" for t in data.targets if t in data.md5_of))
    record = {
        "prediction_set_id": prediction_set_id,
        "lines": lines,
        "donors": len(data.states.terms),
    }
    (folder / "export_blast_inputs.json").write_text(json.dumps(record, indent=1))


if __name__ == "__main__":
    main(Path(sys.argv[1]), sys.argv[2])
