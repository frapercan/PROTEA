"""Step 2 of ADR-D59: the photo builder's passes, on a cut small enough to read."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import tarfile
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from protea.core.cut_photo.build import (
    OUTPUTS,
    main,
    manifest_pass,
    members_pass,
    rows_pass,
    sequences_pass,
)
from protea.core.cut_photo.measure import PassSource, PhotoGuardError
from protea.core.swissprot_table import canonical_table

CUT = 211
CUT_DATE = date(2022, 9, 16)
UNIPROT = {"release": "2022_03", "published": "2022-08-03"}
SEQUENCES = {"P1": "MKV", "P2": "MAAGG"}
MD5 = {a: hashlib.md5(s.encode(), usedforsecurity=False).hexdigest() for a, s in SEQUENCES.items()}
GOA_FILE = {
    "url": "https://ftp.ebi.ac.uk/pub/databases/GO/goa/old/UNIPROT/goa_uniprot_all.gaf.211.gz",
    "sha256": "b" * 64,
}
SPROT_URL = (
    "https://ftp.uniprot.org/pub/databases/uniprot/previous_releases/release-2022_03/"
    "knowledgebase/uniprot_sprot-only2022_03.tar.gz"
)


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


def _record(path: Path, pass_name: str, **extra: object) -> None:
    head = {"pass": pass_name, **UNIPROT, "cut_release": CUT, "cut_date": CUT_DATE.isoformat()}
    path.write_text(json.dumps({**head, **extra}))


def _setup(tmp_path: Path) -> tuple[Path, Path]:
    superset = tmp_path / "superset.tsv"
    blob = canonical_table(MD5)
    superset.write_bytes(blob)
    _record(tmp_path / "superset.json", "superset", sha256=hashlib.sha256(blob).hexdigest())
    _record(tmp_path / "neighbours.json", "neighbours")
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


def _build(
    tmp_path: Path,
    gaf: list[str],
    goa_file: dict[str, str] = GOA_FILE,
    rows_file: dict[str, str] | None = None,
) -> Path:
    superset, neighbours = _setup(tmp_path)
    out = tmp_path / "photo"
    members_pass(gaf, superset_table=superset, source=_source(out), source_file=goa_file)
    rows_pass(
        gaf,
        members_path=out / "members.parquet",
        neighbours_path=neighbours,
        source=_source(out),
        source_file=goa_file if rows_file is None else rows_file,
    )
    return out


class TestMembers:
    def test_the_base_is_the_superset_and_what_goa_admits(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        table = pq.read_table(out / "members.parquet").to_pylist()
        by = {row["accession"]: row for row in table}
        assert sorted(by) == ["P1", "P2", "Q9"]
        assert by["P1"]["superset"] and by["P1"]["truth"] and by["P1"]["md5"] == MD5["P1"]
        assert by["Q9"]["curated"] and not by["Q9"]["superset"] and by["Q9"]["md5"] is None
        assert by["P2"]["superset"] and not by["P2"]["truth"]


class TestTheInputsAreNamed:
    """Each measured table is named by the record beside it, or the pass refuses."""

    def test_the_photo_names_its_superset_and_its_clusters(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        members = json.loads((out / "members.json").read_text())
        rows = json.loads((out / "rows.json").read_text())
        assert members["superset_source"]["release"] == "2022_03"
        assert (
            members["superset_source"]["sha256"] == hashlib.sha256(canonical_table(MD5)).hexdigest()
        )
        assert rows["neighbours_source"]["release"] == "2022_03"
        assert len(rows["neighbours_source"]["sha256"]) == 64

    def test_a_table_with_no_record_refuses(self, tmp_path: Path) -> None:
        superset, _ = _setup(tmp_path)
        (tmp_path / "superset.json").unlink()
        with pytest.raises(PhotoGuardError, match="missing"):
            members_pass(GAF, superset_table=superset, source=_source(tmp_path / "photo"))

    def test_a_table_measured_for_another_cut_refuses(self, tmp_path: Path) -> None:
        superset, neighbours = _setup(tmp_path)
        out = tmp_path / "photo"
        members_pass(GAF, superset_table=superset, source=_source(out))
        record = json.loads((tmp_path / "neighbours.json").read_text())
        (tmp_path / "neighbours.json").write_text(json.dumps({**record, "cut_release": 205}))
        with pytest.raises(PhotoGuardError, match="written for cut 205"):
            rows_pass(
                GAF,
                members_path=out / "members.parquet",
                neighbours_path=neighbours,
                source=_source(out),
            )

    def test_clusters_published_after_the_cut_refuse(self, tmp_path: Path) -> None:
        superset, neighbours = _setup(tmp_path)
        out = tmp_path / "photo"
        members_pass(GAF, superset_table=superset, source=_source(out))
        _record(
            tmp_path / "neighbours.json", "neighbours", release="2022_04", published="2022-10-12"
        )
        with pytest.raises(PhotoGuardError):
            rows_pass(
                GAF,
                members_path=out / "members.parquet",
                neighbours_path=neighbours,
                source=_source(out),
            )

    def test_a_superset_table_its_record_does_not_describe_refuses(self, tmp_path: Path) -> None:
        superset, _ = _setup(tmp_path)
        superset.write_bytes(canonical_table({**MD5, "P3": "c" * 32}))
        with pytest.raises(PhotoGuardError, match="its record says"):
            members_pass(GAF, superset_table=superset, source=_source(tmp_path / "photo"))

    def test_an_accession_in_two_clusters_refuses(self, tmp_path: Path) -> None:
        superset, neighbours = _setup(tmp_path)
        out = tmp_path / "photo"
        members_pass(GAF, superset_table=superset, source=_source(out))
        pq.write_table(
            pa.table(
                {
                    "cluster": ["C1", "C1", "C2"],
                    "accession": ["P1", "N1", "N1"],
                    "in_superset": [True, False, False],
                }
            ),
            neighbours,
        )
        with pytest.raises(PhotoGuardError, match="more than one cluster"):
            rows_pass(
                GAF,
                members_path=out / "members.parquet",
                neighbours_path=neighbours,
                source=_source(out),
            )

    def test_clusters_measured_against_another_superset_refuse(self, tmp_path: Path) -> None:
        superset, neighbours = _setup(tmp_path)
        out = tmp_path / "photo"
        members_pass(GAF, superset_table=superset, source=_source(out))
        pq.write_table(
            pa.table({"cluster": ["C1"], "accession": ["N1"], "in_superset": [True]}), neighbours
        )
        with pytest.raises(PhotoGuardError, match="superset does not"):
            rows_pass(
                GAF,
                members_path=out / "members.parquet",
                neighbours_path=neighbours,
                source=_source(out),
            )


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
    def _archive(self, tmp_path: Path, extra: bytes = b"") -> Path:
        fasta = gzip.compress(b">sp|P1|ONE_HUMAN\nMKV\n>sp|P2|TWO_HUMAN\nMAA\nGG\n" + extra)
        archive = tmp_path / "sprot.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            info = tarfile.TarInfo("uniprot_sprot.fasta.gz")
            info.size = len(fasta)
            tar.addfile(info, io.BytesIO(fasta))
        return archive

    def _sequences(self, tmp_path: Path, out: Path, extra: bytes = b"") -> None:
        sequences_pass(
            self._archive(tmp_path, extra),
            _source(out, release="2022_03", published=date(2022, 8, 3)),
            source_url=SPROT_URL,
        )

    def test_an_unknown_reference_refuses_the_photo(self, tmp_path: Path) -> None:
        out = _build(tmp_path, [*GAF, UNKNOWN_ROW])
        self._sequences(tmp_path, out)
        manifest = manifest_pass(out, revision="abc")
        assert manifest["status"] == "refused"
        assert manifest["refusals"]["unknown_evidence_rows"] == 1
        lines = (out / "unknown_evidence.tsv").read_text().splitlines()
        assert lines[1] == "IEA\tother\tReactome\tUniProt\t1\tReactome:R-1"

    def test_an_unknown_goref_keeps_its_id_and_its_examples(self, tmp_path: Path) -> None:
        rows = [_gaf("N3", f"GO:00000{n}", "IEA", "GO_REF:0009999") for n in (10, 11, 12)]
        out = _build(tmp_path, [*GAF, *rows])
        self._sequences(tmp_path, out)
        manifest = manifest_pass(out, revision="abc")
        assert manifest["counts"]["rows"]["unknown_evidence_keys"] == 1
        lines = (out / "unknown_evidence.tsv").read_text().splitlines()
        assert lines[1] == "IEA\tGO_REF:0009999\tGO_REF\tUniProt\t3\tGO_REF:0009999"

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

    def test_the_manifest_names_its_sources(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        self._sequences(tmp_path, out)
        sources = manifest_pass(out, revision="abc")["sources"]
        assert sources["goa"] == {
            "release": CUT,
            "published": CUT_DATE.isoformat(),
            **GOA_FILE,
        }
        assert sources["uniprot"]["release"] == "2022_03"
        assert sources["uniprot"]["url"] == SPROT_URL
        assert (
            sources["uniprot"]["superset_sha256"]
            == hashlib.sha256(canonical_table(MD5)).hexdigest()
        )
        assert sources["uniref"]["release"] == "2022_03"

    def test_an_undeclared_goa_file_refuses_the_photo(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF, goa_file={})
        self._sequences(tmp_path, out)
        manifest = manifest_pass(out, revision="abc")
        assert manifest["status"] == "refused"
        disagreements = manifest["refusals"]["source_disagreements"]
        assert any("not declared with its URL and sha256" in d for d in disagreements)

    def test_two_goa_passes_over_different_files_refuse_the_photo(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF, rows_file={**GOA_FILE, "sha256": "c" * 64})
        self._sequences(tmp_path, out)
        manifest = manifest_pass(out, revision="abc")
        assert manifest["status"] == "refused"
        assert any(d.startswith("GOA file:") for d in manifest["refusals"]["source_disagreements"])

    def test_an_archive_that_is_not_the_checked_one_is_refused(self, tmp_path: Path) -> None:
        source = _source(tmp_path / "photo", release="2022_03", published=date(2022, 8, 3))
        with pytest.raises(PhotoGuardError, match="hashes to"):
            sequences_pass(
                self._archive(tmp_path), source, source_url=SPROT_URL, expected_sha256="0" * 64
            )

    def test_the_cli_will_not_read_goa_without_its_file_declared(self, tmp_path: Path) -> None:
        argv = ["members", "--superset-table", str(tmp_path / "superset.tsv"), "--release", "211"]
        argv += ["--published", "2022-09-16", "--cut-release", "211", "--cut-date", "2022-09-16"]
        with pytest.raises(SystemExit):
            main([*argv, "--out", str(tmp_path / "photo")])

    def test_sequences_of_another_release_refuse_the_photo(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        self._sequences(tmp_path, out, extra=b">sp|P3|THREE_HUMAN\nMW\n")
        manifest = manifest_pass(out, revision="abc")
        assert manifest["status"] == "refused"
        disagreements = manifest["refusals"]["source_disagreements"]
        assert any("superset holds 2 entries, sequences 3" in d for d in disagreements)
        assert any("canonical" in d or "hashes to" in d for d in disagreements)

    def test_the_cli_exits_3_on_a_refused_photo(self, tmp_path: Path) -> None:
        out = _build(tmp_path, [*GAF, UNKNOWN_ROW])
        self._sequences(tmp_path, out)
        assert main(["manifest", "--out", str(out), "--revision", "abc"]) == 3
