"""The homology table's file stage (ADR-D61): what it reads, what it writes, what it refuses.

A small photo is written on disk the way the photo builder writes one (members, the
superset's sequences, the TrEMBL sequences of the other members and a manifest that
hashes them), and MMseqs2 is replaced by a script that answers ``version`` and writes a
fixed tab file, so every pass runs here offline.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from protea.core.homology import build
from protea.core.homology.build import (
    HomologyRefused,
    checked_photo,
    hits_table,
    manifest_pass,
    mmseqs_command,
    prepare_pass,
    search_pass,
)


def _md5(sequence: str) -> str:
    return hashlib.md5(sequence.encode(), usedforsecurity=False).hexdigest()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


#: P1 and P2 are in the superset and share a sequence; P3 is in it alone. Q9 is a truth
#: member from TrEMBL, N1 a neighbour, N2 a neighbour whose sequence the photo lacks.
SWISSPROT = {"P1": "MKKLL", "P2": "MKKLL", "P3": "MAAGG"}
TREMBL = {"Q9": "MQQLK", "N1": "MNNPP"}
MEMBERS = {
    "P1": (True, False, False, False),
    "P2": (True, True, False, False),
    "P3": (True, False, True, False),
    "Q9": (False, True, False, False),
    "N1": (False, False, False, True),
    "N2": (False, False, False, True),
}


def _photo(tmp_path: Path, **over: object) -> Path:
    photo = tmp_path / "photo"
    photo.mkdir()
    accessions = list(MEMBERS)
    sets = list(zip(*MEMBERS.values(), strict=True))
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
    for name, table in (("sequences", SWISSPROT), ("unreviewed_sequences", TREMBL)):
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
        "cut_release": 211,
        "kind": "full",
        "status": "complete",
        "revision": "photo-revision",
        "outputs": {name: _sha256(photo / name) for name in build.PHOTO_INPUTS},
    }
    manifest.update(over)
    (photo / "photo.json").write_text(json.dumps(manifest), encoding="utf-8")
    return photo


def _fasta(path: Path) -> dict[str, str]:
    lines = path.read_text(encoding="ascii").splitlines()
    return {lines[i][1:]: lines[i + 1] for i in range(0, len(lines), 2)}


class TestPrepare:
    def test_the_queries_are_the_superset_and_the_bank_every_member(self, tmp_path: Path) -> None:
        out = tmp_path / "out"
        record = prepare_pass(_photo(tmp_path), out)
        queries, bank = _fasta(out / "queries.fasta"), _fasta(out / "bank.fasta")
        # P1 and P2 share a sequence: one query, keyed by its md5.
        assert queries == {_md5("MKKLL"): "MKKLL", _md5("MAAGG"): "MAAGG"}
        assert set(bank) == {_md5(s) for s in ("MKKLL", "MAAGG", "MQQLK", "MNNPP")}
        assert list(queries) == sorted(queries) and list(bank) == sorted(bank)
        assert (record["queries"], record["bank"]) == (2, 4)

    def test_a_member_without_a_sequence_is_counted_and_named(self, tmp_path: Path) -> None:
        record = prepare_pass(_photo(tmp_path), tmp_path / "out")
        assert record["members_without_sequence"] == 1
        assert record["members_without_sequence_examples"] == ["N2"]

    def test_the_record_names_the_photo_it_read(self, tmp_path: Path) -> None:
        photo = _photo(tmp_path)
        record = prepare_pass(photo, tmp_path / "out")
        assert record["photo"] == {
            "cut_release": 211,
            "manifest_sha256": _sha256(photo / "photo.json"),
            "revision": "photo-revision",
        }

    def test_a_sample_keeps_the_first_query_md5s_in_order(self, tmp_path: Path) -> None:
        out = tmp_path / "out"
        record = prepare_pass(_photo(tmp_path), out, sample=1)
        assert list(_fasta(out / "queries.fasta")) == [min(_md5("MKKLL"), _md5("MAAGG"))]
        assert record["sample"] == 1 and record["bank"] == 4


class TestPrepareRefuses:
    def test_a_cut_after_the_mark(self, tmp_path: Path) -> None:
        with pytest.raises(HomologyRefused, match="after the mark"):
            checked_photo(_photo(tmp_path, cut_release=228))

    def test_a_refused_photo(self, tmp_path: Path) -> None:
        with pytest.raises(HomologyRefused, match="'refused'"):
            checked_photo(_photo(tmp_path, status="refused"))

    def test_a_truth_only_photo(self, tmp_path: Path) -> None:
        with pytest.raises(HomologyRefused, match="only closes a window"):
            checked_photo(_photo(tmp_path, kind="truth_only"))

    def test_an_input_changed_after_the_photo_was_built(self, tmp_path: Path) -> None:
        photo = _photo(tmp_path)
        pq.write_table(
            pa.table({"accession": ["P1"], "superset": [True]}), photo / "members.parquet"
        )
        with pytest.raises(HomologyRefused, match="members.parquet no longer hashes"):
            checked_photo(photo)

    def test_a_sequence_its_md5_does_not_describe(self, tmp_path: Path) -> None:
        photo = _photo(tmp_path)
        table = pa.table({"accession": ["Q9"], "md5": ["0" * 32], "sequence": ["MQQLK"]})
        pq.write_table(table, photo / "unreviewed_sequences.parquet")
        manifest = json.loads((photo / "photo.json").read_text())
        manifest["outputs"]["unreviewed_sequences.parquet"] = _sha256(
            photo / "unreviewed_sequences.parquet"
        )
        (photo / "photo.json").write_text(json.dumps(manifest))
        with pytest.raises(HomologyRefused, match="does not hash to"):
            prepare_pass(photo, tmp_path / "out")


def _fake_mmseqs(tmp_path: Path, rows: list[str]) -> Path:
    """A stand-in for MMseqs2: ``version`` prints a tag, ``easy-search`` writes ``rows``."""
    script = tmp_path / "mmseqs"
    body = "\\n".join(rows)
    script.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = version ]; then echo 18-8cc5c; exit 0; fi\n'
        f'printf "{body}\\n" > "$4"\n',
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return script


A, B, C = "a" * 32, "b" * 32, "c" * 32
M8 = [
    f"{A}\t{A}\t1.000\t5\t1.000\t1.000\t1e-30\t50",  # A with itself: dropped
    f"{A}\t{C}\t0.400\t5\t1.000\t1.000\t1e-03\t12",
    f"{A}\t{B}\t0.800\t5\t1.000\t0.900\t1e-10\t30",
    f"{B}\t{A}\t0.800\t5\t0.900\t1.000\t1e-10\t30",
]


class TestSearch:
    def test_the_command_sets_resources_and_columns_only(self, tmp_path: Path) -> None:
        command = mmseqs_command(Path("/x/mmseqs"), tmp_path, tmp_path / "t", 32, "67200M")
        assert command[1] == "easy-search"
        flags = [part for part in command if part.startswith("--")]
        assert flags == ["--threads", "--format-output", "--split-memory-limit"]
        assert command[command.index("--format-output") + 1] == build.MMSEQS_FORMAT

    def test_self_hits_are_dropped_and_the_rest_sorted(self, tmp_path: Path) -> None:
        m8 = tmp_path / "hits.m8"
        m8.write_text("\n".join(M8) + "\n", encoding="utf-8")
        table, dropped = hits_table(m8)
        assert dropped == 1
        assert table.column_names == list(build.HIT_COLUMNS)
        # Ninety million hits do not fit the offsets of a ``string`` array.
        assert table.schema.field("query_md5").type == pa.large_string()
        assert table.schema.field("target_md5").type == pa.large_string()
        pairs = list(
            zip(table["query_md5"].to_pylist(), table["target_md5"].to_pylist(), strict=True)
        )
        assert pairs == [(A, B), (A, C), (B, A)]

    def test_a_run_writes_the_table_and_its_record(self, tmp_path: Path) -> None:
        out = tmp_path / "out"
        prepare_pass(_photo(tmp_path), out)
        mmseqs = _fake_mmseqs(tmp_path, M8)
        run = {"threads": 4, "split_memory_limit": None, "tmp": tmp_path / "tmp"}
        record = search_pass(out, mmseqs, _sha256(mmseqs), run)
        assert record["mmseqs"] == {"version": "18-8cc5c", "binary_sha256": _sha256(mmseqs)}
        assert (record["hits"], record["self_hits_dropped"]) == (3, 1)
        assert record["queries_with_hits"] == 2 and record["queries_without_hits"] == 0
        assert pq.read_table(out / "hits.parquet").num_rows == 3
        assert not (out / "hits.m8").exists()
        assert all("/" not in part for part in record["command"])

    def test_a_binary_other_than_the_pinned_one_is_refused(self, tmp_path: Path) -> None:
        out = tmp_path / "out"
        prepare_pass(_photo(tmp_path), out)
        mmseqs = _fake_mmseqs(tmp_path, M8)
        run = {"threads": 4, "tmp": tmp_path / "tmp"}
        with pytest.raises(HomologyRefused, match="hashes to"):
            search_pass(out, mmseqs, "0" * 64, run)
        assert not (out / "hits.parquet").exists()


class TestManifest:
    def _built(self, tmp_path: Path, sample: int | None = None) -> Path:
        out = tmp_path / "out"
        prepare_pass(_photo(tmp_path), out, sample=sample)
        mmseqs = _fake_mmseqs(tmp_path, M8)
        search_pass(out, mmseqs, _sha256(mmseqs), {"threads": 4, "tmp": tmp_path / "tmp"})
        return out

    def test_a_full_run_is_complete_and_hashes_its_output(self, tmp_path: Path) -> None:
        out = self._built(tmp_path)
        manifest = manifest_pass(out, revision="abc")
        assert manifest["status"] == "complete"
        assert manifest["outputs"] == {"hits.parquet": _sha256(out / "hits.parquet")}
        assert manifest["cut_release"] == 211 and manifest["revision"] == "abc"
        assert manifest["mmseqs"]["version"] == "18-8cc5c"

    def test_a_sampled_run_is_a_measurement(self, tmp_path: Path) -> None:
        manifest = manifest_pass(self._built(tmp_path, sample=1), revision="abc")
        assert manifest["status"] == "measurement"

    def test_a_run_that_never_searched_is_refused(self, tmp_path: Path) -> None:
        out = tmp_path / "out"
        prepare_pass(_photo(tmp_path), out)
        manifest = manifest_pass(out, revision="abc")
        assert manifest["status"] == "refused"
        assert manifest["refusals"]["missing_outputs"] == ["hits.parquet"]


class TestCommandLine:
    def test_a_refusal_exits_2_and_says_why(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        photo = _photo(tmp_path, cut_release=228)
        assert build.main(["prepare", "--photo", str(photo), "--out", str(tmp_path / "o")]) == 2
        assert "REFUSED: cut 228 is after the mark 227" in capsys.readouterr().err

    def test_the_job_script_needs_the_photo_manifest(self, tmp_path: Path) -> None:
        import subprocess

        script = Path(__file__).resolve().parents[1] / "deploy/cluster/homology_build.sh"
        args = ["python", str(tmp_path), str(tmp_path / "nophoto"), str(tmp_path / "o")]
        args += ["rev", "/bin/true", "0" * 64]
        done = subprocess.run(
            ["bash", str(script), *args],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
            env={**os.environ, "TMPDIR": str(tmp_path)},
        )
        assert done.returncode == 1
        assert "MISSING" in done.stdout and "photo.json" in done.stdout
