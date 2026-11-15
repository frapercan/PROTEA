"""Tests for the cut photo's wire formats (ADR-D59).

The builder restates three things the platform already defines elsewhere,
because their home modules import the database layer and the builder runs on a
compute node without it: the GAF column indices, ADR-D49's tier codes and the
Swiss-Prot archive member. The first group of tests pins each restatement to its
home, so a change there fails here instead of making two parts of the platform
read one file two ways.
"""

from __future__ import annotations

import re

from protea.core.cut_photo import formats
from protea.core.cut_photo.formats import (
    UNIREF_LINE_FILTER,
    is_negated,
    iter_gaf_columns,
    iter_uniref_clusters,
    reference_class,
)
from protea.core.cut_photo.measure import SWISSPROT_FASTA_MEMBER

# ---------------------------------------------------------------------------
# Restatements pinned to their homes
# ---------------------------------------------------------------------------


class TestPinnedToThePlatform:
    def test_gaf_columns_are_where_extract_goa_universe_reads_them(self) -> None:
        from protea.core.operations import extract_goa_universe as egu

        assert formats.GAF_ID == egu._GAF_ID
        assert formats.GAF_EVIDENCE == egu._GAF_EVIDENCE
        assert formats.GAF_TYPE == egu._GAF_TYPE

    def test_tier_codes_are_adr_d49s(self) -> None:
        from protea.core.operations import _universe_sources as us

        assert formats.TRUTH_CODES == us.TRUTH_CODES
        assert formats.CURATED_INFERENCE_CODES == us.CURATED_INFERENCE_CODES
        assert formats.PROPAGATED_CODES == us.PROPAGATED_CODES
        assert formats.KNOWN_CODES == us.ALL_KNOWN_CODES

    def test_swissprot_member_is_the_loaders(self) -> None:
        from protea.core.operations import load_swissprot_release as lsr

        assert SWISSPROT_FASTA_MEMBER == lsr._FASTA_MEMBER


# ---------------------------------------------------------------------------
# The reference field and the qualifier
# ---------------------------------------------------------------------------


class TestReferenceClass:
    def test_a_go_ref_names_itself(self) -> None:
        assert reference_class("GO_REF:0000002") == "GO_REF:0000002"

    def test_the_first_go_ref_wins_over_a_publication(self) -> None:
        assert reference_class("PMID:12345|GO_REF:0000024") == "GO_REF:0000024"

    def test_publications_collapse(self) -> None:
        assert reference_class("PMID:12345") == "publication"
        assert reference_class("DOI:10.1000/xyz") == "publication"

    def test_anything_else_is_other(self) -> None:
        assert reference_class("Reactome:R-HSA-123") == "other"
        assert reference_class("") == "other"


class TestNegation:
    def test_not_is_read_from_a_compound_qualifier(self) -> None:
        assert is_negated("NOT|enables")
        assert is_negated("NOT")

    def test_a_relation_alone_is_not_a_negation(self) -> None:
        assert not is_negated("enables")
        assert not is_negated("")


# ---------------------------------------------------------------------------
# GAF rows
# ---------------------------------------------------------------------------


def _gaf(db: str, accession: str, code: str, n_cols: int = 17) -> str:
    cols = [""] * n_cols
    cols[0] = db
    cols[1] = accession
    cols[4] = "GO:0005515"
    cols[5] = "PMID:1"
    cols[6] = code
    if n_cols > 14:
        cols[14] = "UniProt"
    return "\t".join(cols) + "\n"


class TestGafColumns:
    def test_keeps_uniprot_rows_and_skips_the_rest(self) -> None:
        lines = [
            "!gaf-version: 2.2\n",
            "\n",
            _gaf("UniProtKB", "P12345", "IDA"),
            _gaf("ComplexPortal", "CPX-1", "IDA"),
            _gaf("UniProtKB", "P99999", "IEA", n_cols=10),
        ]
        rows = list(iter_gaf_columns(lines))
        assert [r[formats.GAF_ID] for r in rows] == ["P12345"]
        assert rows[0][formats.GAF_EVIDENCE] == "IDA"


# ---------------------------------------------------------------------------
# UniRef XML membership
# ---------------------------------------------------------------------------

UNIREF_XML = """<?xml version="1.0" encoding="UTF-8"?>
<UniRef50 xmlns="http://uniprot.org/uniref" releaseDate="2022-08-03" version="2022_03">
<entry id="UniRef50_P11111" updated="2022-08-03">
<name>Cluster: Example</name>
<property type="member count" value="4"/>
<property type="GO Molecular Function" value="GO:0005515"/>
<representativeMember>
<dbReference type="UniProtKB ID" id="EX_HUMAN">
<property type="UniProtKB accession" value="P11111"/>
<property type="UniProtKB accession" value="Q00001"/>
<property type="UniParc ID" value="UPI000000AAAA"/>
</dbReference>
<sequence length="5" checksum="ABC">MKVLA</sequence>
</representativeMember>
<member>
<dbReference type="UniParc ID" id="UPI000000BBBB">
<property type="UniRef100 ID" value="UniRef100_UPI000000BBBB"/>
</dbReference>
</member>
<member>
<dbReference type="UniProtKB ID" id="A0A000_MOUSE">
<property type="UniProtKB accession" value="A0A000"/>
</dbReference>
</member>
</entry>
<entry id="UniRef50_P22222" updated="2022-08-03">
<representativeMember>
<dbReference type="UniProtKB ID" id="P22222_YEAST">
<property type="UniProtKB accession" value="P22222"/>
</dbReference>
</representativeMember>
</entry>
</UniRef50>
"""


class TestUniRefClusters:
    def test_one_primary_accession_per_member_and_no_uniparc(self) -> None:
        clusters = list(iter_uniref_clusters(UNIREF_XML.splitlines(keepends=True)))
        assert clusters == [
            ("UniRef50_P11111", ["P11111", "A0A000"]),
            ("UniRef50_P22222", ["P22222"]),
        ]

    def test_the_grep_filter_keeps_everything_the_parser_reads(self) -> None:
        lines = UNIREF_XML.splitlines(keepends=True)
        pattern = re.compile(UNIREF_LINE_FILTER)
        kept = [line for line in lines if pattern.search(line)]
        assert list(iter_uniref_clusters(kept)) == list(iter_uniref_clusters(lines))
        assert len(kept) < len(lines)

    def test_an_empty_stream_has_no_clusters(self) -> None:
        assert list(iter_uniref_clusters([])) == []
