"""Step 2 of ADR-D59: the photo builder's passes, on a cut small enough to read."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import sys
import tarfile
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from protea.core.cut_photo.build import (
    KNOWLEDGEBASE_STREAM_MD5,
    OUTPUTS,
    main,
    manifest_pass,
    members_pass,
    rows_pass,
    sequences_pass,
    unreviewed_pass,
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
KB_FILE = {
    "url": "https://ftp.uniprot.org/pub/databases/uniprot/previous_releases/release-2022_03/"
    "knowledgebase/knowledgebase2022_03.tar.gz",
    "md5": "3d030df00f7ec4fd5b2d43ca74e3e4f7",
}
#: The paired release's TrEMBL, as the job streams it: Q9 is the one member outside
#: the superset; X7 is a record nobody asked for.
TREMBL = [b">tr|Q9|Q9_HUMAN Some protein OS=Homo sapiens\n", b"MQQ\n", b"LK\n"]
TREMBL += [b">tr|X7|X7_HUMAN Other\n", b"MAA\n"]
TREMBL += [b">tr|N1|N1_MOUSE\n", b"MNN\n", b">tr|N2|N2_RAT\n", b"MPP\n"]


def _source(out: Path, release: str | int = CUT, published: date = CUT_DATE) -> PassSource:
    return PassSource(
        release=release, published=published, cut_release=CUT, cut_date=CUT_DATE, out=out
    )


def _gaf(
    accession: str,
    go: str,
    code: str,
    reference: str,
    qualifier: str = "enables",
    with_from: str = "",
) -> str:
    cols = [""] * 17
    cols[7] = with_from
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


GO_VERSION = "http://purl.obolibrary.org/obo/go/releases/2022-09-12/extensions/go-plus.owl"
HEADER = [
    "!gaf-version: 2.2\n",
    "!\n",
    "!This file contains all GO annotations for proteins in the UniProt KnowledgeBase,\n",
    "!date-generated: 2022-09-16 10:43 \n",
    f"!go-version: {GO_VERSION}\n",
]
GAF = [
    *HEADER,
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
UNKNOWN_ROW = _gaf("Q9", "GO:0000008", "IEA", "Reactome:R-1")


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
    members_pass(
        gaf,
        superset_table=superset,
        neighbours_path=neighbours,
        source=_source(out),
        source_file=goa_file,
    )
    rows_pass(
        gaf,
        members_path=out / "members.parquet",
        source=_source(out),
        source_file=goa_file if rows_file is None else rows_file,
    )
    return out


def _members(tmp_path: Path, gaf: list[str]) -> dict[str, dict[str, object]]:
    out = _build(tmp_path, gaf)
    return {row["accession"]: row for row in pq.read_table(out / "members.parquet").to_pylist()}


class TestMembers:
    def test_the_superset_what_goa_admits_and_the_annotated_neighbours(
        self, tmp_path: Path
    ) -> None:
        by = _members(tmp_path, GAF)
        assert sorted(by) == ["N1", "N2", "P1", "P2", "Q9"]
        assert by["P1"]["superset"] and by["P1"]["truth"] and by["P1"]["md5"] == MD5["P1"]
        assert by["Q9"]["curated"] and not by["Q9"]["superset"] and by["Q9"]["md5"] is None
        assert by["P2"]["superset"] and not by["P2"]["truth"]
        # N1 by PAINT (IBA), N2 by electronic orthology: neighbours alone, never targets.
        for neighbour in ("N1", "N2"):
            row = by[neighbour]
            assert row["neighbour"] and not (row["superset"] or row["truth"] or row["curated"])
        assert {a: by[a]["cluster"] for a in by} == {
            "N1": "C1",
            "N2": "C1",
            "P1": "C1",
            "P2": "C2",
            "Q9": None,
        }

    def test_the_record_counts_each_set(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        record = json.loads((out / "members.json").read_text())
        assert (record["members"], record["by_neighbour"], record["outside_superset"]) == (5, 2, 3)
        assert record["clusters_with_superset"] == 2
        assert record["cluster_members_outside_base"] == 3

    def test_a_neighbour_with_signatures_alone_stays_out(self, tmp_path: Path) -> None:
        by = _members(tmp_path, [*GAF, _gaf("N3", "GO:0000030", "IEA", "GO_REF:0000002")])
        assert "N3" not in by and "X1" not in by

    def test_a_signature_cited_before_an_orthology_reference_does_not_admit(
        self, tmp_path: Path
    ) -> None:
        # The reference class is the FIRST GO_REF the field cites, as in the rows pass.
        row = _gaf("N3", "GO:0000031", "IEA", "GO_REF:0000002|GO_REF:0000107")
        assert "N3" not in _members(tmp_path, [*GAF, row])

    def test_a_cluster_member_with_a_truth_row_enters_by_truth(self, tmp_path: Path) -> None:
        by = _members(tmp_path, [*GAF, _gaf("N3", "GO:0000032", "IDA", "PMID:9")])
        assert by["N3"]["truth"] and not by["N3"]["neighbour"]
        assert by["N3"]["cluster"] == "C2"


class TestTheInputsAreNamed:
    """Each measured table is named by the record beside it, or the pass refuses."""

    def _members_pass(self, tmp_path: Path, superset: Path, neighbours: Path) -> None:
        members_pass(
            GAF,
            superset_table=superset,
            neighbours_path=neighbours,
            source=_source(tmp_path / "photo"),
        )

    def test_the_photo_names_its_superset_and_its_clusters(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        members = json.loads((out / "members.json").read_text())
        assert members["superset_source"]["release"] == "2022_03"
        assert (
            members["superset_source"]["sha256"] == hashlib.sha256(canonical_table(MD5)).hexdigest()
        )
        assert members["uniref_source"]["release"] == "2022_03"
        assert len(members["uniref_source"]["sha256"]) == 64

    def test_a_table_with_no_record_refuses(self, tmp_path: Path) -> None:
        superset, neighbours = _setup(tmp_path)
        (tmp_path / "superset.json").unlink()
        with pytest.raises(PhotoGuardError, match="missing"):
            self._members_pass(tmp_path, superset, neighbours)

    def test_a_table_measured_for_another_cut_refuses(self, tmp_path: Path) -> None:
        superset, neighbours = _setup(tmp_path)
        record = json.loads((tmp_path / "neighbours.json").read_text())
        (tmp_path / "neighbours.json").write_text(json.dumps({**record, "cut_release": 205}))
        with pytest.raises(PhotoGuardError, match="written for cut 205"):
            self._members_pass(tmp_path, superset, neighbours)

    def test_clusters_published_after_the_cut_refuse(self, tmp_path: Path) -> None:
        superset, neighbours = _setup(tmp_path)
        _record(
            tmp_path / "neighbours.json", "neighbours", release="2022_04", published="2022-10-12"
        )
        with pytest.raises(PhotoGuardError):
            self._members_pass(tmp_path, superset, neighbours)

    def test_a_superset_table_its_record_does_not_describe_refuses(self, tmp_path: Path) -> None:
        superset, neighbours = _setup(tmp_path)
        superset.write_bytes(canonical_table({**MD5, "P3": "c" * 32}))
        with pytest.raises(PhotoGuardError, match="its record says"):
            self._members_pass(tmp_path, superset, neighbours)

    def test_an_accession_in_two_clusters_refuses(self, tmp_path: Path) -> None:
        superset, neighbours = _setup(tmp_path)
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
            self._members_pass(tmp_path, superset, neighbours)

    def test_clusters_measured_against_another_superset_refuse(self, tmp_path: Path) -> None:
        superset, neighbours = _setup(tmp_path)
        pq.write_table(
            pa.table({"cluster": ["C1"], "accession": ["N1"], "in_superset": [True]}), neighbours
        )
        with pytest.raises(PhotoGuardError, match="superset does not"):
            self._members_pass(tmp_path, superset, neighbours)


class TestRows:
    def test_every_member_row_carries_its_channel(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        rows = pq.read_table(out / "rows.parquet").to_pylist()
        got = {(r["accession"], r["go_id"]): r["channel"] for r in rows}
        assert got == {
            ("P1", "GO:0000001"): "truth",
            ("P1", "GO:0000002"): "reviewed_field_electronic",
            ("Q9", "GO:0000003"): "curated",
            ("Q9", "GO:0000004"): "signature_electronic",
            ("N1", "GO:0000005"): "phylogenetic",
            ("N2", "GO:0000006"): "orthology_electronic",
            ("N2", "GO:0000007"): "signature_electronic",
        }

    def test_every_row_of_a_neighbour_is_kept_as_written(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        rows = pq.read_table(out / "rows.parquet").to_pylist()
        assert sum(r["accession"] == "N1" for r in rows) == 2
        assert sum(r["accession"] == "N2" for r in rows) == 2


class TestAnEmptyGoRef:
    """GOA 211's rows that cite ``GO_REF:`` with no number, read through their with/from."""

    def test_a_subcellular_location_row_takes_the_field_channel(self, tmp_path: Path) -> None:
        empty = [
            _gaf("P1", "GO:0000020", "IEA", "GO_REF:", with_from="UniProtKB-SubCell:SL-0097"),
            _gaf("Q9", "GO:0000021", "IEA", "GO_REF:", with_from="UniProtKB-SubCell:SL-0097"),
            _gaf("N2", "GO:0000022", "IEA", "GO_REF:", with_from="UniProtKB-KW:KW-0539"),
        ]
        out = _build(tmp_path, [*GAF, *empty])
        rows = pq.read_table(out / "rows.parquet").to_pylist()
        got = {(r["accession"], r["go_id"]): r["channel"] for r in rows}
        assert got[("P1", "GO:0000020")] == "reviewed_field_electronic"
        assert got[("Q9", "GO:0000021")] == "signature_electronic"
        assert got[("N2", "GO:0000022")] == "signature_electronic"
        record = json.loads((out / "rows.json").read_text())
        assert record["unknown_evidence_rows"] == 0
        assert record["empty_go_ref_read_as_GO_REF:0000044"] == 2
        assert record["empty_go_ref_read_as_GO_REF:0000043"] == 1

    def test_an_empty_goref_from_another_vocabulary_refuses(self, tmp_path: Path) -> None:
        unread = _gaf("Q9", "GO:0000023", "IEA", "GO_REF:", with_from="InterPro:IPR000001")
        out = _build(tmp_path, [*GAF, unread])
        lines = (out / "unknown_evidence.tsv").read_text().splitlines()
        assert lines[1] == "IEA\tempty_go_ref\tGO_REF\tUniProt\t1\tGO_REF:"

    def test_the_channel_digest_covers_the_reading_of_an_empty_goref(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from protea.core.cut_photo import build

        before = build.channel_table_digest()
        monkeypatch.setitem(build.EMPTY_GO_REF_BY_WITH_FROM, "UniProtKB-KW", "GO_REF:0000037")
        assert build.channel_table_digest() != before


class TestTheManifest:
    def _archive(self, tmp_path: Path, extra: bytes = b"") -> Path:
        fasta = gzip.compress(b">sp|P1|ONE_HUMAN\nMKV\n>sp|P2|TWO_HUMAN\nMAA\nGG\n" + extra)
        archive = tmp_path / "sprot.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            info = tarfile.TarInfo("uniprot_sprot.fasta.gz")
            info.size = len(fasta)
            tar.addfile(info, io.BytesIO(fasta))
        return archive

    def _sequences(
        self,
        tmp_path: Path,
        out: Path,
        extra: bytes = b"",
        stream_md5: str | None = KB_FILE["md5"],
        kb_file: dict[str, str] = KB_FILE,
    ) -> None:
        uniprot = _source(out, release="2022_03", published=date(2022, 8, 3))
        sequences_pass(self._archive(tmp_path, extra), uniprot, source_url=SPROT_URL)
        unreviewed_pass(
            TREMBL, members_path=out / "members.parquet", source=uniprot, source_file=kb_file
        )
        if stream_md5 is not None:
            (out / KNOWLEDGEBASE_STREAM_MD5).write_text(stream_md5 + "\n")

    def test_an_unknown_reference_refuses_the_photo(self, tmp_path: Path) -> None:
        out = _build(tmp_path, [*GAF, UNKNOWN_ROW])
        self._sequences(tmp_path, out)
        manifest = manifest_pass(out, revision="abc")
        assert manifest["status"] == "refused"
        assert manifest["refusals"]["unknown_evidence_rows"] == 1
        lines = (out / "unknown_evidence.tsv").read_text().splitlines()
        assert lines[1] == "IEA\tother\tReactome\tUniProt\t1\tReactome:R-1"

    def test_an_unknown_goref_keeps_its_id_and_its_examples(self, tmp_path: Path) -> None:
        rows = [_gaf("Q9", f"GO:00000{n}", "IEA", "GO_REF:0009999") for n in (10, 11, 12)]
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
        unreviewed = pq.read_table(out / "unreviewed_sequences.parquet").to_pylist()
        assert [u["accession"] for u in unreviewed] == ["N1", "N2", "Q9"]
        md5 = hashlib.md5(b"MQQLK", usedforsecurity=False).hexdigest()
        assert unreviewed[2] == {"accession": "Q9", "md5": md5, "sequence": "MQQLK"}
        assert manifest["counts"]["unreviewed"]["missing"] == 0

    def test_the_manifest_names_its_sources(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        self._sequences(tmp_path, out)
        sources = manifest_pass(out, revision="abc")["sources"]
        assert sources["goa"] == {
            "release": CUT,
            "published": CUT_DATE.isoformat(),
            **GOA_FILE,
            "date_generated": "2022-09-16 10:43",
            "go_version": GO_VERSION,
            "declared_go_release": "2022-09-12",
        }
        assert sources["uniprot"]["release"] == "2022_03"
        assert sources["uniprot"]["url"] == SPROT_URL
        assert (
            sources["uniprot"]["superset_sha256"]
            == hashlib.sha256(canonical_table(MD5)).hexdigest()
        )
        assert sources["uniref"]["release"] == "2022_03"
        assert sources["uniprot"]["knowledgebase"] == KB_FILE

    def test_a_gaf_that_declares_no_go_release_refuses_the_photo(self, tmp_path: Path) -> None:
        out = _build(tmp_path, [line for line in GAF if not line.startswith("!go-version")])
        self._sequences(tmp_path, out)
        manifest = manifest_pass(out, revision="abc")
        assert manifest["status"] == "refused"
        disagreements = manifest["refusals"]["source_disagreements"]
        assert any("declares no GO release" in d for d in disagreements)

    def test_two_passes_over_different_gaf_headers_refuse_the_photo(self, tmp_path: Path) -> None:
        superset, neighbours = _setup(tmp_path)
        out = tmp_path / "photo"
        members_pass(
            GAF,
            superset_table=superset,
            neighbours_path=neighbours,
            source=_source(out),
            source_file=GOA_FILE,
        )
        other = [line.replace("2022-09-12", "2022-07-01") for line in GAF]
        rows_pass(
            other, members_path=out / "members.parquet", source=_source(out), source_file=GOA_FILE
        )
        self._sequences(tmp_path, out)
        manifest = manifest_pass(out, revision="abc")
        assert any(
            d.startswith("GAF header:") for d in manifest["refusals"]["source_disagreements"]
        )

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

    def _refusals(self, out: Path) -> list[str]:
        manifest = manifest_pass(out, revision="abc")
        assert manifest["status"] == "refused"
        return list(manifest["refusals"]["source_disagreements"]) + list(
            manifest["refusals"]["missing_outputs"]
        )

    def test_a_stream_that_is_not_the_published_archive_refuses(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        self._sequences(tmp_path, out, stream_md5="0" * 32)
        assert any("knowledgebase stream hashed to 000" in r for r in self._refusals(out))

    def test_a_stream_with_no_md5_recorded_refuses(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        self._sequences(tmp_path, out, stream_md5=None)
        assert any("nothing recorded" in r for r in self._refusals(out))

    def test_an_archive_of_another_release_refuses(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        other = {**KB_FILE, "url": KB_FILE["url"].replace("2022_03", "2021_03")}
        self._sequences(tmp_path, out, kb_file=other)
        assert any("is not UniProt 2022_03" in r for r in self._refusals(out))

    def test_a_photo_without_the_trembl_pass_is_refused(self, tmp_path: Path) -> None:
        out = _build(tmp_path, GAF)
        uniprot = _source(out, release="2022_03", published=date(2022, 8, 3))
        sequences_pass(self._archive(tmp_path), uniprot, source_url=SPROT_URL)
        assert "unreviewed_sequences.parquet" in self._refusals(out)


class TestTheTremblPass:
    """The members outside the superset take their sequence from the paired TrEMBL."""

    def _photo(self, tmp_path: Path) -> tuple[Path, PassSource]:
        out = _build(tmp_path, GAF)
        return out, _source(out, release="2022_03", published=date(2022, 8, 3))

    def test_a_member_the_release_does_not_hold_is_counted_and_named(self, tmp_path: Path) -> None:
        out, uniprot = self._photo(tmp_path)
        record = unreviewed_pass(
            TREMBL[3:5], members_path=out / "members.parquet", source=uniprot, source_file=KB_FILE
        )
        assert (record["members_outside_superset"], record["sequences"]) == (3, 0)
        assert (record["missing"], record["missing_examples"]) == (3, ["N1", "N2", "Q9"])

    def test_a_record_read_twice_refuses(self, tmp_path: Path) -> None:
        out, uniprot = self._photo(tmp_path)
        with pytest.raises(PhotoGuardError, match="twice"):
            unreviewed_pass(
                TREMBL + TREMBL[:3],
                members_path=out / "members.parquet",
                source=uniprot,
                source_file=KB_FILE,
            )

    def test_the_cli_reads_the_stream_as_bytes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        out, _ = self._photo(tmp_path)
        monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(b"".join(TREMBL))))
        argv = ["unreviewed", "--members", str(out / "members.parquet"), "--release", "2022_03"]
        argv += ["--published", "2022-08-03", "--cut-release", "211", "--cut-date", "2022-09-16"]
        argv += ["--out", str(out), "--source-url", KB_FILE["url"], "--source-md5", KB_FILE["md5"]]
        assert main(argv) == 0
        assert json.loads((out / "unreviewed.json").read_text())["sequences"] == 3
