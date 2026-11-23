"""The evidence channel partition of ADR-D59, decision 2."""

from __future__ import annotations

import pytest

from protea.core.cut_photo import channels
from protea.core.cut_photo.channels import (
    CHANNELS,
    ELECTRONIC_REFERENCES,
    NEIGHBOUR_CHANNELS,
    RECIPE_CHANNELS,
    UnknownEvidence,
    channel_of,
)
from protea.core.cut_photo.formats import AUTOMATIC_CODE, KNOWN_CODES, TRUTH_CODES

#: The references the electronic rows of the superset carry at GOA 211, measured
#: by the photo's gaf pass (2026-11-15). Every one of them must classify.
GOA_211_ELECTRONIC_REFERENCES = (
    "GO_REF:0000043",
    "GO_REF:0000002",
    "GO_REF:0000104",
    "GO_REF:0000044",
    "GO_REF:0000003",
    "GO_REF:0000108",
    "GO_REF:0000117",
    "GO_REF:0000107",
    "GO_REF:0000116",
    "GO_REF:0000041",
)


class TestThePartition:
    def test_seven_channels_and_the_recipe_and_neighbour_channels_are_subsets(self) -> None:
        assert len(CHANNELS) == len(set(CHANNELS)) == 7
        assert RECIPE_CHANNELS <= set(CHANNELS)
        # A neighbour is admitted only by recipe channels an unreviewed protein outside
        # the base can carry: truth or curated would have admitted it already.
        assert NEIGHBOUR_CHANNELS == {"phylogenetic", "orthology_electronic"}
        assert NEIGHBOUR_CHANNELS <= RECIPE_CHANNELS
        assert "signature_electronic" not in RECIPE_CHANNELS
        assert "no_data" not in RECIPE_CHANNELS

    def test_every_non_electronic_known_code_has_a_channel(self) -> None:
        for code in KNOWN_CODES - {AUTOMATIC_CODE}:
            assert channel_of(code, "publication", reviewed=True) in CHANNELS

    @pytest.mark.parametrize("code", sorted(TRUTH_CODES))
    def test_the_lafa_codes_are_truth(self, code: str) -> None:
        assert channel_of(code, "publication", reviewed=False) == "truth"

    @pytest.mark.parametrize("code", ["ISS", "ISO", "ISA", "ISM", "IGC", "RCA", "NAS"])
    def test_curated_inference_is_curated(self, code: str) -> None:
        assert channel_of(code, "GO_REF:0000024", reviewed=True) == "curated"

    @pytest.mark.parametrize("code", ["IBA", "IBD", "IKR", "IRD"])
    def test_paint_is_phylogenetic_even_where_it_admits_through_curation(self, code: str) -> None:
        assert channel_of(code, "GO_REF:0000033", reviewed=True) == "phylogenetic"

    def test_nd_is_no_data(self) -> None:
        assert channel_of("ND", "GO_REF:0000015", reviewed=True) == "no_data"


class TestElectronicRows:
    def test_orthology_is_its_own_channel(self) -> None:
        for ref in ("GO_REF:0000107", "GO_REF:0000019"):
            assert channel_of("IEA", ref, reviewed=False) == "orthology_electronic"

    @pytest.mark.parametrize(
        "ref",
        sorted(ref for ref, (kind, _) in ELECTRONIC_REFERENCES.items() if kind == "field"),
    )
    def test_a_field_translation_is_the_curators_only_on_a_reviewed_entry(self, ref: str) -> None:
        assert channel_of("IEA", ref, reviewed=True) == "reviewed_field_electronic"
        assert channel_of("IEA", ref, reviewed=False) == "signature_electronic"

    @pytest.mark.parametrize(
        "ref",
        sorted(ref for ref, (kind, _) in ELECTRONIC_REFERENCES.items() if kind == "signature"),
    )
    def test_signatures_are_the_mirror_whoever_is_annotated(self, ref: str) -> None:
        assert channel_of("IEA", ref, reviewed=True) == "signature_electronic"
        assert channel_of("IEA", ref, reviewed=False) == "signature_electronic"

    @pytest.mark.parametrize("ref", GOA_211_ELECTRONIC_REFERENCES)
    def test_every_reference_goa_211_carries_classifies(self, ref: str) -> None:
        assert channel_of("IEA", ref, reviewed=True) in CHANNELS

    def test_titles_are_present_and_obsolete_ones_say_so(self) -> None:
        for ref, (_, title) in ELECTRONIC_REFERENCES.items():
            assert title.strip(), ref
        for ref in ("GO_REF:0000019", "GO_REF:0000020", "GO_REF:0000037", "GO_REF:0000038"):
            assert ELECTRONIC_REFERENCES[ref][1].startswith("OBSOLETE"), ref


class TestNothingIsGuessed:
    def test_an_unknown_code_is_refused(self) -> None:
        with pytest.raises(UnknownEvidence, match="evidence code"):
            channel_of("XYZ", "publication", reviewed=True)

    @pytest.mark.parametrize("ref", ["publication", "other", "GO_REF:0009999"])
    def test_an_electronic_row_without_a_listed_reference_is_refused(self, ref: str) -> None:
        with pytest.raises(UnknownEvidence, match="electronic reference"):
            channel_of("IEA", ref, reviewed=True)

    def test_the_list_records_when_it_was_read(self) -> None:
        assert channels.GOREF_LIST_READ_ON == "2026-11-16"
