"""The segment families' file stage (ADR-D65, decision 2): prepare on a small photo and lexicon bank,
and the MMseqs2 passes end to end on a synthetic problem when a binary is named.

``PROTEA_TEST_MMSEQS`` names an MMseqs2 binary (the pinned 18-8cc5c); without it the
end-to-end test skips, as CI has none.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from protea.core.file_stage import file_sha256, write_record
from protea.core.marks import MARK_RELEASE
from protea.core.segment_families import build
from protea.core.segment_families.build import FamiliesRefused, prepare_pass

AMINO = "ACDEFGHIKLMNPQRSTVWY"


def _md5(s: str) -> str:
    return hashlib.md5(s.encode(), usedforsecurity=False).hexdigest()


def _photo(tmp: Path, members: list[tuple[str, bool, bool, str]]) -> Path:
    """A complete full photo: (accession, superset, truth, sequence) per member."""
    photo = tmp / "photo"
    photo.mkdir()
    pq.write_table(
        pa.table(
            {
                "accession": [m[0] for m in members],
                "superset": [m[1] for m in members],
                "truth": [m[2] for m in members],
            }
        ),
        photo / "members.parquet",
    )
    rows = {
        "accession": [m[0] for m in members],
        "md5": [_md5(m[3]) for m in members],
        "sequence": [m[3] for m in members],
    }
    pq.write_table(pa.table(rows), photo / "sequences.parquet")
    pq.write_table(
        pa.table({k: v[:0] for k, v in rows.items()}), photo / "unreviewed_sequences.parquet"
    )
    outputs = {
        n: file_sha256(photo / n)
        for n in ("members.parquet", "sequences.parquet", "unreviewed_sequences.parquet")
    }
    write_record(
        photo / "photo.json",
        {"cut_release": MARK_RELEASE, "status": "complete", "kind": "full", "outputs": outputs},
    )
    return photo


def _bank(tmp: Path, sequences: list[str], segments: dict[str, list[tuple[int, int, int]]]) -> Path:
    """A one-shard population and a complete lexicon bank whose cut names the segments."""
    bank = tmp / "bank"
    cut_dir = bank / "lexicon" / "lex" / "cut"
    cut_dir.mkdir(parents=True)
    md5s = [_md5(s) for s in sequences]
    pq.write_table(pa.table({"md5": md5s, "sequence": sequences}), bank / "sequences.parquet")
    segs = [segments.get(m, []) for m in md5s]
    cut = pa.table(
        {
            "md5": md5s,
            "seg_start": [[s[0] for s in g] for g in segs],
            "seg_end": [[s[1] for s in g] for g in segs],
            "seg_theme": [[s[2] for s in g] for g in segs],
            "columns": [list(range(len(s))) for s in sequences],  # every residue a match
        }
    )
    pq.write_table(cut, cut_dir / "shard-00000.parquet")
    manifest = {
        "status": "complete",
        "name": "lex",
        "lexicon_sha256": "a" * 64,
        "passes": {
            "cut": {
                "shards": [
                    {
                        "shard": 0,
                        "rows": len(md5s),
                        "sha256": file_sha256(cut_dir / "shard-00000.parquet"),
                    }
                ]
            }
        },
    }
    write_record(bank / "lexicon" / "lex" / "lexicon-bank.json", manifest)
    return bank


def _random(rng: random.Random, n: int) -> str:
    return "".join(rng.choice(AMINO) for _ in range(n))


def test_prepare_writes_the_donors_and_the_targets_cores(tmp_path: Path) -> None:
    rng = random.Random(1)
    seqs = [_random(rng, 60) for _ in range(4)]
    members = [
        ("D1", True, True, seqs[0]),
        ("D2", False, True, seqs[1]),
        ("T1", True, False, seqs[2]),
        ("N1", False, False, seqs[3]),
    ]
    photo = _photo(tmp_path, members)
    segments = {_md5(s): [(0, 30, 7), (30, 60, 9)] for s in seqs}
    bank = _bank(tmp_path, seqs, segments)
    record = prepare_pass(photo, bank, "lex", tmp_path / "out")
    assert (record["donor_cores"], record["target_cores"]) == (4, 2)  # N1 is neither
    table = pq.read_table(tmp_path / "out" / "segments.parquet").to_pydict()
    assert table["donor"].count(True) == 4 and set(table["theme"]) == {7, 9}
    fasta = (tmp_path / "out" / "targets.fasta").read_text().split()
    assert fasta == [">0", seqs[2][:30], ">1", seqs[2][30:]]


def test_prepare_refuses_an_incomplete_lexicon_bank_and_a_changed_shard(tmp_path: Path) -> None:
    rng = random.Random(2)
    seqs = [_random(rng, 40)]
    photo = _photo(tmp_path, [("D1", True, True, seqs[0])])
    bank = _bank(tmp_path, seqs, {_md5(seqs[0]): [(0, 40, 1)]})
    path = bank / "lexicon" / "lex" / "lexicon-bank.json"
    manifest = json.loads(path.read_text())
    path.write_text(json.dumps({**manifest, "status": "refused"}))
    with pytest.raises(FamiliesRefused, match="not complete"):
        prepare_pass(photo, bank, "lex", tmp_path / "out")
    path.write_text(json.dumps(manifest))
    shard = bank / "lexicon" / "lex" / "cut" / "shard-00000.parquet"
    pq.write_table(pq.read_table(shard).slice(0, 0), shard)
    with pytest.raises(FamiliesRefused, match="does not hash"):
        prepare_pass(photo, bank, "lex", tmp_path / "out")


def _mutant(rng: random.Random, base: str, rate: float) -> str:
    return "".join(rng.choice(AMINO) if rng.random() < rate else a for a in base)


@pytest.mark.skipif(not os.environ.get("PROTEA_TEST_MMSEQS"), reason="no MMseqs2 binary named")
def test_three_families_come_out_of_mutated_copies(tmp_path: Path) -> None:
    from protea.core.segment_families import searches

    rng = random.Random(3)
    bases = [_random(rng, 120) for _ in range(3)]
    donors = [_mutant(rng, b, 0.15) for b in bases for _ in range(6)] + [
        _random(rng, 120) for _ in range(8)
    ]
    targets = [_mutant(rng, b, 0.15) for b in bases for _ in range(2)] + [
        _random(rng, 120) for _ in range(3)
    ]
    out = tmp_path / "out"
    out.mkdir()
    rows: dict[str, list] = {f.name: [] for f in build.SEGMENT_SCHEMA}
    for donor, cores in ((True, donors), (False, targets)):
        with open(out / ("donors.fasta" if donor else "targets.fasta"), "w") as fh:
            for i, core in enumerate(cores):
                fh.write(f">{i}\n{core}\n")
                for name, value in zip(
                    build.SEGMENT_SCHEMA.names, (_md5(core), 0, 120, 0, 120, donor, i), strict=True
                ):
                    rows[name].append(value)
    pq.write_table(pa.table(rows, schema=build.SEGMENT_SCHEMA), out / "segments.parquet")
    write_record(
        out / "prepare.json",
        {
            "donor_cores": len(donors),
            "target_cores": len(targets),
            "photo": {"cut_release": 211},
            "lexicon": {},
        },
    )
    binary = Path(os.environ["PROTEA_TEST_MMSEQS"])
    tool = searches.Tool(binary, file_sha256(binary), 2, tmp_path / "tmp")
    searches.search_pass(out, tool)
    for scope in build.SCOPES:
        record = searches.families_pass(out, tool, scope)
        table = pq.read_table(out / f"families-{scope}.parquet").to_pydict()
        family = np.array(table["family"])
        donor = np.array(table["donor"])
        fam_d = family[donor]
        for b in range(3):
            assert len(set(fam_d[b * 6 : b * 6 + 6])) == 1 and fam_d[b * 6] >= 0, (scope, b)
        assert len({fam_d[0], fam_d[6], fam_d[12]}) == 3
        fam_t = family[~donor]
        assert [int(x) for x in fam_t[:6]] == [int(fam_d[0])] * 2 + [int(fam_d[6])] * 2 + [
            int(fam_d[12])
        ] * 2
        assert (fam_t[6:] == -1).all()
        assert record["families_of_two_or_more"] == 3
    assert build.manifest_pass(out, "r")["status"] == "complete"
