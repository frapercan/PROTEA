"""Tests for step 1 of ADR-D59, the measurement of a cut.

Small, hand-built inputs whose counts can be checked by eye: a two-entry
Swiss-Prot archive, a two-cluster UniRef stream and a GAF with one row of each
kind the inventory has to tell apart.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import tarfile
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from protea.core.cut_photo.measure import (
    MARK_RELEASE,
    SWISSPROT_FASTA_MEMBER,
    PassSource,
    PhotoGuardError,
    check_dates,
    code_family,
    gaf_pass,
    main,
    neighbours_pass,
    pattern_of,
    read_neighbours,
    read_superset,
    superset_pass,
)
from protea.core.swissprot_table import canonical_table, parse_swissprot_fasta

CUT = 211
CUT_DATE = date(2022, 9, 16)
UNIPROT_DATE = date(2022, 8, 3)

FASTA = b">sp|P11111|EX_HUMAN Example\nMKV\nLA\n>sp|P22222|EY_YEAST Other\nMSTN\n"


def _archive(tmp_path: Path) -> Path:
    path = tmp_path / "uniprot_sprot-only2022_03.tar.gz"
    payload = gzip.compress(FASTA)
    with tarfile.open(path, "w:gz") as tar:
        info = tarfile.TarInfo(SWISSPROT_FASTA_MEMBER)
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    return path


# ---------------------------------------------------------------------------
# The guard
# ---------------------------------------------------------------------------


class TestGuard:
    def test_refuses_a_cut_after_the_mark(self) -> None:
        with pytest.raises(PhotoGuardError, match="after the mark"):
            check_dates(
                source="GOA", published=CUT_DATE, cut_release=MARK_RELEASE + 1, cut_date=CUT_DATE
            )

    def test_refuses_a_source_published_after_its_cut(self) -> None:
        with pytest.raises(PhotoGuardError, match="after the cut"):
            check_dates(
                source="UniRef",
                published=date(2022, 10, 12),
                cut_release=CUT,
                cut_date=CUT_DATE,
            )

    def test_accepts_the_mark_itself_and_a_same_day_source(self) -> None:
        check_dates(source="GOA", published=CUT_DATE, cut_release=MARK_RELEASE, cut_date=CUT_DATE)

    def test_every_pass_applies_it(self, tmp_path: Path) -> None:
        late = date(2022, 10, 12)
        with pytest.raises(PhotoGuardError):
            superset_pass(
                _archive(tmp_path),
                source=PassSource(
                    release="2022_04",
                    published=late,
                    cut_release=CUT,
                    cut_date=CUT_DATE,
                    out=tmp_path / "s",
                ),
            )
        with pytest.raises(PhotoGuardError):
            neighbours_pass(
                [],
                superset=frozenset(),
                source=PassSource(
                    release="2022_04",
                    published=late,
                    cut_release=CUT,
                    cut_date=CUT_DATE,
                    out=tmp_path / "n",
                ),
            )
        with pytest.raises(PhotoGuardError):
            gaf_pass(
                [],
                superset=frozenset(),
                neighbours=frozenset(),
                source=PassSource(
                    release=MARK_RELEASE + 1,
                    published=CUT_DATE,
                    cut_release=MARK_RELEASE + 1,
                    cut_date=CUT_DATE,
                    out=tmp_path / "g",
                ),
            )


# ---------------------------------------------------------------------------
# Families and patterns
# ---------------------------------------------------------------------------


class TestFamilies:
    def test_each_code_lands_in_its_adr_d49_family(self) -> None:
        assert code_family("IDA") == "truth"
        assert code_family("TAS") == "truth"
        assert code_family("ISS") == "curated"
        assert code_family("IKR") == "curated"
        assert code_family("IBA") == "propagated"
        assert code_family("IEA") == "automatic"
        assert code_family("ND") == "no_data"
        assert code_family("XYZ") == "unknown"

    def test_a_truth_row_decides_the_pattern(self) -> None:
        assert pattern_of(1 | 8) == "truth"
        assert pattern_of(2 | 8) == "curated"
        assert pattern_of(4) == "only_electronic_or_phylogenetic"
        assert pattern_of(8 | 16) == "only_electronic_or_phylogenetic"
        assert pattern_of(16) == "only_no_data"
        assert pattern_of(32) == "only_unknown"


# ---------------------------------------------------------------------------
# The three passes
# ---------------------------------------------------------------------------


class TestSupersetPass:
    def test_writes_the_table_the_loader_verifies(self, tmp_path: Path) -> None:
        archive = _archive(tmp_path)
        record = superset_pass(
            archive,
            source=PassSource(
                release="2022_03",
                published=UNIPROT_DATE,
                cut_release=CUT,
                cut_date=CUT_DATE,
                out=tmp_path / "out",
            ),
        )
        expected = canonical_table(parse_swissprot_fasta(io.BytesIO(FASTA)))
        written = (tmp_path / "out" / "superset.tsv").read_bytes()
        assert written == expected
        assert record["sha256"] == hashlib.sha256(expected).hexdigest()
        assert record["accessions"] == 2
        assert read_superset(tmp_path / "out" / "superset.tsv") == {"P11111", "P22222"}
        assert json.loads((tmp_path / "out" / "superset.json").read_text())["accessions"] == 2


UNIREF_LINES = [
    '<entry id="UniRef50_P11111" updated="2022-08-03">\n',
    '<dbReference type="UniProtKB ID" id="EX_HUMAN">\n',
    '<property type="UniProtKB accession" value="P11111"/>\n',
    '<dbReference type="UniProtKB ID" id="A0A000_MOUSE">\n',
    '<property type="UniProtKB accession" value="A0A000"/>\n',
    '<entry id="UniRef50_Q33333" updated="2022-08-03">\n',
    '<dbReference type="UniProtKB ID" id="Q33333_ECOLI">\n',
    '<property type="UniProtKB accession" value="Q33333"/>\n',
]


class TestNeighboursPass:
    def test_keeps_only_clusters_holding_a_superset_protein(self, tmp_path: Path) -> None:
        record = neighbours_pass(
            UNIREF_LINES,
            superset=frozenset({"P11111", "P22222"}),
            source=PassSource(
                release="2022_03",
                published=UNIPROT_DATE,
                cut_release=CUT,
                cut_date=CUT_DATE,
                out=tmp_path,
            ),
            batch_rows=1,
        )
        table = pq.read_table(tmp_path / "neighbours.parquet").to_pylist()
        assert table == [
            {"cluster": "UniRef50_P11111", "accession": "P11111", "in_superset": True},
            {"cluster": "UniRef50_P11111", "accession": "A0A000", "in_superset": False},
        ]
        assert record["clusters"] == 2
        assert record["clusters_with_superset"] == 1
        assert record["neighbours_not_in_superset"] == 1
        assert record["superset_in_no_cluster"] == 1
        assert read_neighbours(tmp_path / "neighbours.parquet") == {"A0A000"}


def _row(
    accession: str,
    code: str,
    ref: str = "GO_REF:0000002",
    qualifier: str = "enables",
    by: str = "InterPro",
) -> str:
    cols = [""] * 17
    cols[0] = "UniProtKB"
    cols[1] = accession
    cols[3] = qualifier
    cols[4] = "GO:0005515"
    cols[5] = ref
    cols[6] = code
    cols[14] = by
    return "\t".join(cols) + "\n"


GAF_LINES = [
    "!gaf-version: 2.2\n",
    _row("A0A000", "IBA", ref="GO_REF:0000033", by="GO_Central"),
    _row("A0A000", "IBA", ref="GO_REF:0000033", by="GO_Central", qualifier="NOT|enables"),
    _row("P11111", "IDA", ref="PMID:1", by="UniProt"),
    _row("P11111", "IEA"),
    _row("P11111", "IEA"),
    _row("P22222", "IEA"),
    _row("Z99999", "IEA"),
    _row("Z99999", "QQQ"),
]


class TestGafPass:
    def _run(self, tmp_path: Path) -> dict:
        return gaf_pass(
            GAF_LINES,
            superset=frozenset({"P11111", "P22222", "P33333"}),
            neighbours=frozenset({"A0A000", "B0B000"}),
            source=PassSource(
                release=CUT,
                published=CUT_DATE,
                cut_release=CUT,
                cut_date=CUT_DATE,
                out=tmp_path,
            ),
            batch_rows=1,
        )

    def test_counts_rows_by_set_and_proteins_by_pattern(self, tmp_path: Path) -> None:
        record = self._run(tmp_path)
        assert record["rows_by_set"] == {"superset": 4, "neighbour": 2, "outside": 2}
        assert record["superset_by_pattern"] == {
            "truth": 1,
            "only_electronic_or_phylogenetic": 1,
            "no_rows": 1,
        }
        assert record["neighbours_by_pattern"] == {"only_electronic_or_phylogenetic": 1}
        assert record["neighbours_with_no_rows"] == 1
        assert record["outside_rows_by_code"] == {"IEA": 1, "QQQ": 1}
        assert record["unknown_codes"] == {}
        assert record["order_breaks"] == 0

    def test_groups_keep_the_negation_and_the_pipeline(self, tmp_path: Path) -> None:
        self._run(tmp_path)
        rows = (tmp_path / "groups.tsv").read_text().splitlines()
        assert rows[0] == "set\tcode\treference\tassigned_by\tnegated\trows"
        body = {tuple(r.split("\t")[:5]): int(r.split("\t")[5]) for r in rows[1:]}
        assert body[("superset", "IEA", "GO_REF:0000002", "InterPro", "False")] == 3
        assert body[("neighbour", "IBA", "GO_REF:0000033", "GO_Central", "True")] == 1
        assert body[("superset", "IDA", "publication", "UniProt", "False")] == 1

    def test_protein_keys_carry_row_counts(self, tmp_path: Path) -> None:
        self._run(tmp_path)
        keys = {
            int(line.split("\t")[0]): tuple(line.split("\t")[1:])
            for line in (tmp_path / "keys.tsv").read_text().splitlines()[1:]
        }
        rows = pq.read_table(tmp_path / "protein_keys.parquet").to_pylist()
        p1 = {keys[r["key_id"]][0]: r["rows"] for r in rows if r["accession"] == "P11111"}
        assert p1 == {"IDA": 1, "IEA": 2}

    def test_an_accession_out_of_order_is_counted(self, tmp_path: Path) -> None:
        record = gaf_pass(
            [_row("P22222", "IEA"), _row("P11111", "IEA"), _row("P22222", "IEA")],
            superset=frozenset({"P11111", "P22222"}),
            neighbours=frozenset(),
            source=PassSource(
                release=CUT,
                published=CUT_DATE,
                cut_release=CUT,
                cut_date=CUT_DATE,
                out=tmp_path,
            ),
        )
        assert record["order_breaks"] == 1
        assert record["superset_by_pattern"] == {
            "only_electronic_or_phylogenetic": 2,
            "no_rows": 0,
        }

    def test_a_neighbour_split_across_runs_is_one_protein(self, tmp_path: Path) -> None:
        record = gaf_pass(
            [_row("A0A000", "IBA"), _row("B0B000", "IEA"), _row("A0A000", "IEA")],
            superset=frozenset(),
            neighbours=frozenset({"A0A000", "B0B000", "C0C000"}),
            source=PassSource(
                release=CUT,
                published=CUT_DATE,
                cut_release=CUT,
                cut_date=CUT_DATE,
                out=tmp_path,
            ),
        )
        assert record["runs_by_set"] == {"neighbour": 3}
        assert record["proteins_by_set"] == {"superset": 0, "neighbour": 2}
        assert record["neighbours_by_pattern"] == {"only_electronic_or_phylogenetic": 2}
        assert record["neighbours_with_no_rows"] == 1

    def test_an_unknown_code_of_a_member_is_reported_by_name(self, tmp_path: Path) -> None:
        record = gaf_pass(
            [_row("P11111", "QQQ")],
            superset=frozenset({"P11111"}),
            neighbours=frozenset(),
            source=PassSource(
                release=CUT,
                published=CUT_DATE,
                cut_release=CUT,
                cut_date=CUT_DATE,
                out=tmp_path,
            ),
        )
        assert record["unknown_codes"] == {"QQQ": 1}
        assert record["superset_by_pattern"] == {"only_unknown": 1, "no_rows": 0}


class TestCommandLine:
    def test_superset_runs_from_the_command_line_and_stamps_the_revision(
        self, tmp_path: Path
    ) -> None:
        archive = _archive(tmp_path)
        out = tmp_path / "cli"
        code = main(
            [
                "superset",
                "--archive",
                str(archive),
                "--release",
                "2022_03",
                "--published",
                "2022-08-03",
                "--cut-release",
                "211",
                "--cut-date",
                "2022-09-16",
                "--out",
                str(out),
                "--revision",
                "abc1234",
            ]
        )
        assert code == 0
        assert json.loads((out / "superset.run.json").read_text())["revision"] == "abc1234"
        assert (out / "superset.tsv").exists()
