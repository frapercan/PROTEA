"""The self scores' file stage: what it reads, what it writes, what it counts and refuses.

Two small photos are written on disk the way the photo builder writes one, and
MMseqs2 is replaced by a script that answers each call the stage makes (``version``,
``createdb``, ``tsv2db``, ``align``, ``convertalis`` and ``easy-search``) with what the
real binary writes, so every pass runs here offline. The real binary was run on
sequences of this shape by hand: the forced self alignment gives each sequence the
bit score ``easy-search`` gives its own hit, and a run of ``X`` an absurd one.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from protea.core.homology import build, self_scores
from protea.core.homology.build import HomologyRefused

_FAKE = '''#!{python}
"""A stand-in for MMseqs2: each call writes what the stage reads from the real one."""
import os, sys
from pathlib import Path

args = sys.argv[1:]
def fasta(path):
    out, name = {{}}, None
    for line in Path(path).read_text().splitlines():
        if line.startswith(">"):
            name = line[1:]
        else:
            out[name] = line
    return out
def bits(sequence):
    if set(sequence) == {{"X"}}:
        return 1695306980  # what MMseqs2 answers for a run of X aligned with itself
    return 2 * len(sequence) + int(os.environ.get("FAKE_BITS_SHIFT", "0"))
def rows(sequences):
    return "".join(
        f"{{m}}\\t{{m}}\\t1.000\\t{{len(s)}}\\t1.000\\t1.000\\t1.0E-50\\t{{bits(s)}}\\n"
        for m, s in sequences.items() if "B" not in s
    )
if args[0] == "version":
    print("8cc5ce367b5638c4306c2d7cfc652dd099a4643f")
elif args[0] == "createdb":
    keys = "".join(f"{{i}}\\t{{m}}\\t0\\n" for i, m in enumerate(fasta(args[1])))
    Path(args[2] + ".lookup").write_text(keys)
elif args[0] in ("tsv2db", "align"):
    pass
elif args[0] == "convertalis":
    union = Path(args[1]).parent / "union.fasta"
    Path(args[4]).write_text(rows(fasta(union)))
elif args[0] == "easy-search":
    Path(args[3]).write_text(rows(fasta(args[1])))
'''


def _md5(sequence: str) -> str:
    return hashlib.md5(sequence.encode(), usedforsecurity=False).hexdigest()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _photo(root: Path, cut: int, swissprot: dict[str, str], trembl: dict[str, str]) -> Path:
    photo = root / f"photo-{cut}"
    photo.mkdir()
    accessions = [*swissprot, *trembl]
    flags = [(a in swissprot, True, False, False) for a in accessions]
    sets = list(zip(*flags, strict=True))
    pq.write_table(
        pa.table(
            {
                "accession": accessions,
                "superset": list(sets[0]),
                "truth": list(sets[1]),
                "curated": list(sets[2]),
                "neighbour": list(sets[3]),
                "cluster": ["UniRef50_A"] * len(accessions),
                "md5": [None] * len(accessions),
            }
        ),
        photo / "members.parquet",
    )
    for name, table in (("sequences", swissprot), ("unreviewed_sequences", trembl)):
        pq.write_table(
            pa.table(
                {
                    "accession": list(table),
                    "md5": [_md5(s) for s in table.values()],
                    "sequence": list(table.values()),
                }
            ),
            photo / f"{name}.parquet",
        )
    manifest = {
        "cut_release": cut,
        "kind": "full",
        "status": "complete",
        "revision": "photo-revision",
        "outputs": {name: _sha256(photo / name) for name in build.PHOTO_INPUTS},
    }
    (photo / "photo.json").write_text(json.dumps(manifest))
    return photo


#: P1 and P2 share a sequence; PX is a run of X; PB is a sequence the alignment skips.
SP_211 = {"P1": "MKKLLA", "P2": "MKKLLA", "P3": "MAAGGW", "PX": "XXXXXX", "PB": "MBBQ"}
SP_194 = {"P1": "MKKLLA", "P4": "MWWPPT"}
TREMBL = {"Q9": "MQQLKE"}


@pytest.fixture()
def mmseqs(tmp_path: Path) -> Path:
    path = tmp_path / "mmseqs"
    path.write_text(_FAKE.format(python=sys.executable))
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


@pytest.fixture()
def photos(tmp_path: Path) -> list[Path]:
    root = tmp_path / "photos"
    root.mkdir()
    return [_photo(root, 211, SP_211, TREMBL), _photo(root, 194, SP_194, TREMBL)]


def test_the_queries_are_the_tables_own_by_the_sha256_of_their_bytes(
    tmp_path: Path, photos: list[Path]
) -> None:
    record = self_scores.prepare_pass(photos, tmp_path / "out")
    table = build.prepare_pass(photos[0], tmp_path / "table")
    assert record["cuts"][0]["queries_fasta_sha256"] == table["queries_fasta_sha256"]
    assert record["cuts"][0]["queries"] == table["queries"] == 4
    # The union holds each sequence once, the shared one included.
    union = (tmp_path / "out" / "union.fasta").read_text().count(">")
    assert union == len({*SP_211.values(), *SP_194.values()})


def _run(tmp_path: Path, photos: list[Path], mmseqs: Path, sample: int | None = None) -> Path:
    out = tmp_path / ("sample" if sample else "out")
    self_scores.prepare_pass(photos, out, sample)
    self_scores.align_pass(out, mmseqs, _sha256(mmseqs), threads=2)
    return out


def test_each_cut_gets_its_queries_scores_and_counts_the_ones_it_cannot_score(
    tmp_path: Path, photos: list[Path], mmseqs: Path
) -> None:
    out = _run(tmp_path, photos, mmseqs)
    align = json.loads((out / "align.json").read_text())
    assert align["unscored"] == 2 and sorted(align["unscored_examples"]) == sorted(
        [_md5(SP_211["PX"]), _md5(SP_211["PB"])]
    )
    manifests = {m["cut_release"]: m for m in self_scores.manifest_pass(out, "code-revision")}
    assert {m["status"] for m in manifests.values()} == {"complete"}
    scores = pq.read_table(out / "211" / "self-scores.parquet")
    assert set(scores["query_md5"].to_pylist()) == {_md5(SP_211["P1"]), _md5(SP_211["P3"])}
    assert manifests[211]["queries"] == 4 and manifests[211]["queries_without_self_score"] == 2
    assert manifests[194]["queries_without_self_score"] == 0
    assert manifests[211]["outputs"]["self-scores.parquet"] == _sha256(
        out / "211" / "self-scores.parquet"
    )
    assert manifests[211]["mmseqs"]["binary_sha256"] == _sha256(mmseqs)
    assert manifests[211]["command"][2][:2] == ["mmseqs", "align"]


def test_a_sampled_run_is_a_measurement_checked_against_the_search(
    tmp_path: Path, photos: list[Path], mmseqs: Path
) -> None:
    out = _run(tmp_path, photos, mmseqs, sample=3)
    tmp = tmp_path / "tmp"
    tmp.mkdir()
    check = self_scores.check_pass(out, mmseqs, tmp, threads=2)
    assert check["differ"] == 0 and check["equal"] == check["compared"] > 0
    manifests = self_scores.manifest_pass(out, "code-revision")
    assert [m["status"] for m in manifests] == ["measurement", "measurement"]
    assert (out / "bank.fasta").read_text().count(">") == len({*SP_211.values(), *TREMBL.values()})


def test_the_check_fails_when_a_bit_score_differs(
    tmp_path: Path, photos: list[Path], mmseqs: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_BITS_SHIFT", "1")
    out = _run(tmp_path, photos, mmseqs, sample=3)
    monkeypatch.delenv("FAKE_BITS_SHIFT")
    tmp = tmp_path / "tmp"
    tmp.mkdir()
    code = self_scores.main(
        ["check", "--out", str(out), "--mmseqs", str(mmseqs), "--threads", "2", "--tmp", str(tmp)]
    )
    assert code == 4
    assert json.loads((out / "check.json").read_text())["differ"] > 0


def test_a_binary_that_is_not_the_pinned_one_is_refused(
    tmp_path: Path, photos: list[Path], mmseqs: Path
) -> None:
    out = tmp_path / "out"
    self_scores.prepare_pass(photos, out)
    with pytest.raises(HomologyRefused, match="hashes to"):
        self_scores.align_pass(out, mmseqs, "0" * 64, threads=2)


def test_a_run_without_its_alignment_is_refused_in_its_manifest(
    tmp_path: Path, photos: list[Path]
) -> None:
    out = tmp_path / "out"
    self_scores.prepare_pass(photos, out)
    assert self_scores.main(["manifest", "--out", str(out), "--revision", "r"]) == 3
    assert json.loads((out / "211" / "self-scores.json").read_text())["status"] == "refused"


def test_a_score_is_usable_only_within_what_the_alignment_can_hold() -> None:
    assert self_scores.usable(583.0, 250)
    assert not self_scores.usable(1695306980.0, 25)
    assert not self_scores.usable(0.0, 10) and not self_scores.usable(float("nan"), 10)
    assert self_scores.usable(4.0, 60)


def test_a_photo_whose_superset_lacks_a_sequence_is_refused(tmp_path: Path) -> None:
    root = tmp_path / "photos"
    root.mkdir()
    photo = _photo(root, 211, {"P1": "MKKL"}, {})
    members = pq.read_table(photo / "members.parquet").to_pydict()
    for key in members:
        members[key].append(
            {"accession": "P9", "superset": True, "cluster": "U", "md5": None}.get(key, False)
        )
    pq.write_table(pa.table(members), photo / "members.parquet")
    manifest = json.loads((photo / "photo.json").read_text())
    manifest["outputs"]["members.parquet"] = _sha256(photo / "members.parquet")
    (photo / "photo.json").write_text(json.dumps(manifest))
    with pytest.raises(HomologyRefused, match="superset members have no sequence"):
        self_scores.prepare_pass([photo], tmp_path / "out")
    assert not os.path.exists(tmp_path / "out" / "prepare.json")
