"""Tests for ``load_swissprot_release``.

The operation's whole purpose is that two routes to the same release -- a
streamed download from UniProt and a table extracted earlier -- reduce to the
same bytes, so each is evidence about the other. The test that matters is
therefore the one that reproduces the published digests from the delivered
tables: if the canonical form drifted, the gate would start refusing correct
files, and the drift would be invisible until a load failed.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from protea.core.operations.load_swissprot_release import (
    LoadSwissProtReleaseOperation,
    LoadSwissProtReleasePayload,
    canonical_table,
    parse_swissprot_fasta,
    read_local_table,
)

# ---------------------------------------------------------------------------
# The canonical form
# ---------------------------------------------------------------------------


class TestCanonicalTable:
    def test_sorts_by_accession_regardless_of_insertion_order(self) -> None:
        forward = canonical_table({"P00001": "a" * 32, "P00002": "b" * 32})
        reverse = canonical_table({"P00002": "b" * 32, "P00001": "a" * 32})
        assert forward == reverse
        assert forward == b"P00001\t" + b"a" * 32 + b"\nP00002\t" + b"b" * 32 + b"\n"

    def test_is_empty_for_an_empty_release(self) -> None:
        assert canonical_table({}) == b""


# ---------------------------------------------------------------------------
# Reading a FASTA
# ---------------------------------------------------------------------------


def _fasta(*records: tuple[str, list[str]]) -> io.BytesIO:
    lines: list[str] = []
    for header, sequence in records:
        lines.append(header)
        lines.extend(sequence)
    return io.BytesIO(("\n".join(lines) + "\n").encode())


class TestParseSwissProtFasta:
    def test_accession_is_the_second_pipe_field(self) -> None:
        members = parse_swissprot_fasta(_fasta((">sp|P12345|TEST_HUMAN x", ["MKV"])))
        assert list(members) == ["P12345"]

    def test_md5_is_over_the_joined_stripped_sequence(self) -> None:
        wrapped = parse_swissprot_fasta(_fasta((">sp|P1|A_B", ["MK", "VL"])))
        flat = parse_swissprot_fasta(_fasta((">sp|P1|A_B", ["MKVL"])))
        assert wrapped == flat
        assert wrapped["P1"] == hashlib.md5(b"MKVL").hexdigest()  # noqa: S324

    def test_blank_lines_are_not_part_of_the_sequence(self) -> None:
        with_blank = parse_swissprot_fasta(
            _fasta((">sp|P1|A_B", ["MK", "", "VL"]))
        )
        assert with_blank["P1"] == hashlib.md5(b"MKVL").hexdigest()  # noqa: S324

    def test_reads_every_record_not_only_the_first(self) -> None:
        members = parse_swissprot_fasta(
            _fasta((">sp|P1|A_B", ["MK"]), (">sp|P2|C_D", ["VL"]))
        )
        assert sorted(members) == ["P1", "P2"]

    def test_a_header_without_a_sequence_is_an_error(self) -> None:
        # Silently dropping it would make the accession count wrong by one and
        # nothing downstream would notice.
        with pytest.raises(ValueError, match="no sequence"):
            parse_swissprot_fasta(_fasta((">sp|P1|A_B", [])))

    def test_a_repeated_accession_is_an_error(self) -> None:
        with pytest.raises(ValueError, match="twice"):
            parse_swissprot_fasta(
                _fasta((">sp|P1|A_B", ["MK"]), (">sp|P1|A_B", ["VL"]))
            )

    def test_an_unparseable_header_is_an_error(self) -> None:
        with pytest.raises(ValueError, match="unparseable"):
            parse_swissprot_fasta(_fasta((">P1 no pipes", ["MK"])))

    def test_reads_through_gzip_as_the_archive_member_does(self) -> None:
        raw = (b">sp|P1|A_B\nMKVL\n")
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb") as handle:
            handle.write(raw)
        buffer.seek(0)
        with gzip.GzipFile(fileobj=buffer) as handle:
            members = parse_swissprot_fasta(handle)
        assert members["P1"] == hashlib.md5(b"MKVL").hexdigest()  # noqa: S324


# ---------------------------------------------------------------------------
# Reading a pre-extracted table
# ---------------------------------------------------------------------------


class TestReadLocalTable:
    def test_reads_two_columns(self, tmp_path: Path) -> None:
        path = tmp_path / "t.tsv"
        path.write_text(f"P1\t{'a' * 32}\nP2\t{'b' * 32}\n")
        assert read_local_table(str(path)) == {"P1": "a" * 32, "P2": "b" * 32}

    def test_rejects_a_digest_of_the_wrong_length(self, tmp_path: Path) -> None:
        path = tmp_path / "t.tsv"
        path.write_text("P1\tdeadbeef\n")
        with pytest.raises(ValueError, match="accession<TAB>md5"):
            read_local_table(str(path))

    def test_rejects_a_repeated_accession(self, tmp_path: Path) -> None:
        path = tmp_path / "t.tsv"
        path.write_text(f"P1\t{'a' * 32}\nP1\t{'b' * 32}\n")
        with pytest.raises(ValueError, match="twice"):
            read_local_table(str(path))

    def test_names_the_offending_line(self, tmp_path: Path) -> None:
        path = tmp_path / "t.tsv"
        path.write_text(f"P1\t{'a' * 32}\nbroken\n")
        with pytest.raises(ValueError, match=":2:"):
            read_local_table(str(path))


# ---------------------------------------------------------------------------
# The payload refuses what it cannot pin
# ---------------------------------------------------------------------------


class TestPayload:
    def _valid(self, **over: object) -> dict[str, object]:
        base: dict[str, object] = {
            "release": "2025_03",
            "published_at": "2025-06-18",
            "expected_sha256": "0" * 64,
        }
        base.update(over)
        return base

    def test_accepts_a_minimal_payload(self) -> None:
        p = LoadSwissProtReleasePayload.model_validate(self._valid())
        assert p.release == "2025_03"
        assert p.source == "uniprot_archive"

    @pytest.mark.parametrize("bad", ["", "   ", "2025-03", "25_03", "2025_x", "latest"])
    def test_rejects_a_release_that_is_not_a_uniprot_release(self, bad: str) -> None:
        with pytest.raises(ValidationError):
            LoadSwissProtReleasePayload.model_validate(self._valid(release=bad))

    @pytest.mark.parametrize("bad", ["", "abc", "0" * 63, "0" * 65, "g" * 64])
    def test_rejects_a_digest_that_is_not_hex64(self, bad: str) -> None:
        with pytest.raises(ValidationError):
            LoadSwissProtReleasePayload.model_validate(
                self._valid(expected_sha256=bad)
            )

    def test_normalises_an_uppercase_digest(self) -> None:
        p = LoadSwissProtReleasePayload.model_validate(
            self._valid(expected_sha256="A" * 64)
        )
        assert p.expected_sha256 == "a" * 64

    def test_rejects_an_unknown_source(self) -> None:
        with pytest.raises(ValidationError):
            LoadSwissProtReleasePayload.model_validate(self._valid(source="ftp"))

    def test_round_trips_through_json_as_a_real_job_does(self) -> None:
        import json

        raw = json.loads(json.dumps(self._valid(source="local_table")))
        p = LoadSwissProtReleasePayload.model_validate(raw)
        assert p.source == "local_table"


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


class TestTheGateRefusesBeforeWriting:
    def test_local_table_without_a_path_is_refused(self) -> None:
        op = LoadSwissProtReleaseOperation()
        payload = {
            "release": "2025_03",
            "published_at": "2025-06-18",
            "expected_sha256": "0" * 64,
            "source": "local_table",
        }
        with pytest.raises(ValueError, match="requires local_path"):
            op.execute(None, payload, emit=lambda *a, **k: None)  # type: ignore[arg-type]

    def test_a_digest_mismatch_is_refused(self, tmp_path: Path) -> None:
        path = tmp_path / "t.tsv"
        path.write_text(f"P1\t{'a' * 32}\n")
        op = LoadSwissProtReleaseOperation()
        payload = {
            "release": "2025_03",
            "published_at": "2025-06-18",
            "expected_sha256": "0" * 64,
            "source": "local_table",
            "local_path": str(path),
        }
        with pytest.raises(ValueError, match="does not match the declared"):
            op.execute(None, payload, emit=lambda *a, **k: None)  # type: ignore[arg-type]

    def test_an_accession_count_mismatch_is_refused(self, tmp_path: Path) -> None:
        path = tmp_path / "t.tsv"
        path.write_text(f"P1\t{'a' * 32}\n")
        digest = hashlib.sha256(canonical_table({"P1": "a" * 32})).hexdigest()
        op = LoadSwissProtReleaseOperation()
        payload = {
            "release": "2025_03",
            "published_at": "2025-06-18",
            "expected_sha256": digest,
            "source": "local_table",
            "local_path": str(path),
            "expected_accessions": 2,
        }
        with pytest.raises(ValueError, match="provenance declares 2"):
            op.execute(None, payload, emit=lambda *a, **k: None)  # type: ignore[arg-type]

    def test_the_summary_names_the_release_and_the_route(self) -> None:
        op = LoadSwissProtReleaseOperation()
        assert op.summarize_payload(
            {"release": "2026_01", "source": "local_table"}
        ) == ("Swiss-Prot 2026_01 <- local_table")
        assert op.summarize_payload({}) == ""


# ---------------------------------------------------------------------------
# Against the delivered tables: the canonical form must be theirs
# ---------------------------------------------------------------------------

_PACKAGE = Path(
    os.environ.get(
        "PROTEA_LAFA_ACCEPTANCE_DIR", str(Path.home() / "handoff" / "sfjepa-2026-10-27")
    )
)

#: Published by the reconstruction alongside each table, with the archived
#: UniProt URL and the release date.
_PUBLISHED = {
    "2025_03": (
        "420ad1c7843839ec3d275f13c9770dea31ceca338b7d9407d3e5c686408d207b",
        573661,
    ),
    "2026_01": (
        "153964755e7f130c9f616473a5e02874e1084cfa7a948624a1217c18e0b7ba04",
        574627,
    ),
}

requires_tables = pytest.mark.skipif(
    not (_PACKAGE / "swissprot" / "swissprot-2025_03.tsv").is_file(),
    reason=(
        f"Swiss-Prot tables absent under {_PACKAGE}; set "
        "PROTEA_LAFA_ACCEPTANCE_DIR to the handoff directory"
    ),
)


@requires_tables
class TestCanonicalFormMatchesTheDeliveredTables:
    """If this drifts, the gate refuses correct files and says the file is wrong."""

    @pytest.mark.parametrize("release", sorted(_PUBLISHED))
    def test_digest_and_count_reproduce_the_published_values(
        self, release: str
    ) -> None:
        expected_digest, expected_count = _PUBLISHED[release]
        members = read_local_table(
            str(_PACKAGE / "swissprot" / f"swissprot-{release}.tsv")
        )
        assert len(members) == expected_count
        digest = hashlib.sha256(canonical_table(members)).hexdigest()
        assert digest == expected_digest

    def test_the_same_sequence_rule_gives_the_contract_population(self) -> None:
        # The number piece (b) has to reproduce from the database. Computed
        # here from the tables alone so the target is pinned independently of
        # any query plan.
        t0 = read_local_table(str(_PACKAGE / "swissprot" / "swissprot-2025_03.tsv"))
        t1 = read_local_table(str(_PACKAGE / "swissprot" / "swissprot-2026_01.tsv"))
        in_both = set(t0) & set(t1)
        same_sequence = {acc for acc in in_both if t0[acc] == t1[acc]}
        assert len(in_both) == 573640
        assert len(same_sequence) == 573460
        assert len(in_both) - len(same_sequence) == 180
