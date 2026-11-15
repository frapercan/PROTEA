"""Step 2 of ADR-D59: the photo builder's passes, on a cut small enough to read."""

from __future__ import annotations

import gzip
import io
import json
import tarfile
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from protea.core.cut_photo.build import (
    OUTPUTS,
    main,
    manifest_pass,
    members_pass,
    rows_pass,
    sequences_pass,
)
from protea.core.cut_photo.measure import PassSource

CUT = 211
CUT_DATE = date(2022, 9, 16)


def _source(out: Path, release: str | int = CUT, published: date = CUT_DATE) -> PassSource:
    return PassSource(
        release=release, published=published, cut_release=CUT, cut_date=CUT_DATE, out=out
    )


def _gaf(accession: str, go: str, code: str, reference: str, qualifier: str = "enables") -> str:
    cols = [""] * 17
    cols[0], cols[1], cols[3], cols[4], cols[5], cols[6] = (
        "UniProtKB",
        accession,
        qualifier,
        go,
        reference,
        code,
    )
    cols[8], cols[12], cols[13], cols[14] = "F", "taxon:9606", "20220101", "UniProt"
    return "\t".join(cols) + "\n"


GAF = [
    "!gaf-version: 2.2\n",
    _gaf("P1", "GO:0000001", "IDA", "PMID:1"),
    _gaf("P1", "GO:0000002", "IEA", "GO_REF:0000043"),
    _gaf("Q9", "GO:0000003", "ISS", "GO_REF:0000024"),
    _gaf("Q9", "GO:0000004", "IEA", "GO_REF:0000043"),
    _gaf("N1", "GO:0000005", "IBA", "GO_REF:0000033"),
    _gaf("N1", "GO:0000005", "IBA", "GO_REF:0000033"),
    _gaf("N2", "GO:0000006", "IEA", "GO_REF:0000107"),
    _gaf("N2", "GO:0000007", "IEA", "GO_REF:0000002"),
    _gaf("X1", "GO:0000009", "IEA", "GO_REF:0000002"),
]
UNKNOWN_ROW = _gaf("N3", "GO:0000008", "IEA", "Reactome:R-1")


def _setup(tmp_path: Path) -> tuple[Path, Path]:
    superset = tmp_path / "superset.tsv"
    superset.write_text(f"P1\t{'a' * 32}\nP2\t{'b' * 32}\n")
    neighbours = tmp_path / "neighbours.parquet"
    pq.write_table(
        pa.table(
            {
                "cluster": ["C1", "C1", "C1", "C2", "C2"],
                "accession": ["P1", "N1", "N2", "P2", "N3"],
                "in_superset": [True, False, False, True, False],
            }
        ),
        neighbours,
    )
    return superset, neighbours


def _build(tmp_path: Path, gaf: list[str]) -> Path:
    superset, neighbours = _setup(tmp_path)
    out = tmp_path / "photo"
    members_pass(gaf, superset_table=superset, source=_source(out))
    rows_pass(
        gaf, members_path=out / "members.parquet", neighbours_path=neighbours, source=_source(out)
    )
    return out


class TestMembers:
    def test_the_base_is_the_superset_and_what_goa_admits(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        table = pq.read_table(out / "members.parquet").to_pylist()
        by = {row["accession"]: row for row in table}
        assert sorted(by) == ["P1", "P2", "Q9"]
        assert by["P1"]["superset"] and by["P1"]["truth"] and by["P1"]["md5"] == "a" * 32
        assert by["Q9"]["curated"] and not by["Q9"]["superset"] and by["Q9"]["md5"] is None
        assert by["P2"]["superset"] and not by["P2"]["truth"]


class TestRowsAndProfiles:
    def test_member_rows_carry_their_channel(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        rows = pq.read_table(out / "rows.parquet").to_pylist()
        got = {(r["accession"], r["go_id"]): r["channel"] for r in rows}
        assert got == {
            ("P1", "GO:0000001"): "truth",
            ("P1", "GO:0000002"): "reviewed_field_electronic",
            ("Q9", "GO:0000003"): "curated",
            ("Q9", "GO:0000004"): "signature_electronic",
        }

    def test_a_profile_counts_members_once_and_only_in_profile_channels(
        self, tmp_path: Path
    ) -> None:
        out = _build(tmp_path, GAF)
        profile = {
            (r["cluster"], r["go_id"], r["channel"], r["negated"]): r["members"]
            for r in pq.read_table(out / "profiles.parquet").to_pylist()
        }
        assert profile == {
            ("C1", "GO:0000005", "phylogenetic", False): 1,
            ("C1", "GO:0000006", "orthology_electronic", False): 1,
        }

    def test_the_superset_clusters_are_written(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        clusters = pq.read_table(out / "clusters.parquet").to_pylist()
        assert clusters == [
            {"accession": "P1", "cluster": "C1"},
            {"accession": "P2", "cluster": "C2"},
        ]


class TestTheManifest:
    def _sequences(self, tmp_path: Path, out: Path) -> None:
        fasta = gzip.compress(b">sp|P1|ONE_HUMAN\nMKV\n>sp|P2|TWO_HUMAN\nMAA\nGG\n")
        archive = tmp_path / "sprot.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            info = tarfile.TarInfo("uniprot_sprot.fasta.gz")
            info.size = len(fasta)
            tar.addfile(info, io.BytesIO(fasta))
        sequences_pass(archive, _source(out, release="2022_03", published=date(2022, 8, 3)))

    def test_an_unknown_reference_refuses_the_photo(self, tmp_path: Path) -> None:
        out = _build(tmp_path, [*GAF, UNKNOWN_ROW])
        self._sequences(tmp_path, out)
        manifest = manifest_pass(out, revision="abc")
        assert manifest["status"] == "refused"
        assert manifest["refusals"]["unknown_evidence_rows"] == 1
        assert "IEA\tReactome\tUniProt\t1" in (out / "unknown_evidence.tsv").read_text()

    def test_a_clean_cut_is_complete_with_every_output_digested(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        self._sequences(tmp_path, out)
        manifest = manifest_pass(out, revision="abc")
        assert manifest["status"] == "complete"
        assert sorted(manifest["outputs"]) == sorted(OUTPUTS)
        assert manifest["cut_release"] == CUT
        assert json.loads((out / "photo.json").read_text())["status"] == "complete"
        sequences = pq.read_table(out / "sequences.parquet").to_pylist()
        assert [s["sequence"] for s in sequences] == ["MKV", "MAAGG"]

    def test_the_cli_exits_3_on_a_refused_photo(self, tmp_path: Path) -> None:
        out = _build(tmp_path, [*GAF, UNKNOWN_ROW])
        self._sequences(tmp_path, out)
        assert main(["manifest", "--out", str(out), "--revision", "abc"]) == 3
