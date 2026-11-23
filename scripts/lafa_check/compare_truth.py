"""LAFA's own truth against the frame's, category by category, and the record of the check.

Reads what ``export_inputs.py`` and ``run_lafa.sh`` wrote under ``<out_dir>``
and writes ``<record>``: the digest of every input, the commits on both sides,
and per category the pairs LAFA and the frame hold, the pairs only one holds and
their Jaccard index, for the frame as LAFA publishes it and with the author's
NOT rule. Differences come with a few examples, so a failed check says where.

    python scripts/lafa_check/compare_truth.py <out_dir> <record.json> <protea_commit>
"""

from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path

from protea.core.lafa_frame import LAFA_PACKAGE, LAFA_WORKFLOW

_CATEGORIES = ("NK", "LK", "PK", "PK_known")
_INPUTS = ("go-basic.obo", "goa_uniprot_all.gaf.gz", "uniprot_sprot.fasta.gz")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _pairs(path: Path) -> set[tuple[str, str]]:
    with open(path) as handle:
        return {(row["EntryID"], row["term"]) for row in csv.DictReader(handle, delimiter="\t")}


def _ids(path: Path) -> set[str]:
    return {line.strip() for line in path.read_text().splitlines() if line.strip()}


def _compared(theirs: set, ours: set) -> dict[str, object]:
    union = theirs | ours
    cell: dict[str, object] = {
        "lafa": len(theirs),
        "frame": len(ours),
        "only_lafa": len(theirs - ours),
        "only_frame": len(ours - theirs),
        "jaccard": round(len(theirs & ours) / len(union), 9) if union else 1.0,
    }
    if theirs != ours:
        cell["examples_only_lafa"] = [
            list(x) if isinstance(x, tuple) else x for x in sorted(theirs - ours)[:5]
        ]
        cell["examples_only_frame"] = [
            list(x) if isinstance(x, tuple) else x for x in sorted(ours - theirs)[:5]
        ]
    return cell


def main(out: Path, record_path: Path, protea_commit: str) -> None:
    lafa, frame = out / "lafa", out / "frame"
    record: dict[str, object] = {
        **json.loads((out / "export.json").read_text()),
        "protea_commit": protea_commit,
        "lafa_workflow": LAFA_WORKFLOW,
        "lafa_package": LAFA_PACKAGE,
        "inputs_sha256": {
            f"{end}/{name}": _sha256(out / "inputs" / end / name)
            for end in ("t0", "t1")
            for name in _INPUTS
        },
    }
    for variant in ("as_published", "with_the_not_rule"):
        section: dict[str, object] = {
            name: _compared(
                _pairs(lafa / f"groundtruth_{name}.tsv"),
                _pairs(frame / variant / f"groundtruth_{name}.tsv"),
            )
            for name in _CATEGORIES
        }
        section["targets"] = _compared(
            _ids(lafa / "groundtruth_targets.tsv"),
            _ids(frame / variant / "groundtruth_targets.tsv"),
        )
        record[variant] = section
    record["terms_of_interest"] = _compared(
        _ids(lafa / "groundtruth_terms_of_interest.txt"),
        _ids(frame / "groundtruth_terms_of_interest.txt"),
    )
    record_path.write_text(json.dumps(record, indent=1) + "\n")


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3])
