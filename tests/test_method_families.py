"""The family registry holds the three families of ADR-D60, each named twice and introduced by nobody yet.

A family is a registry entry with a short and a long descriptor (ADR-D58) and an
introduction whose text is the author's (ADR-D60, decision 2). The introductions
are one of the four conditions of the promotion milestone, so a placeholder in
their slot would read as his text and make the condition look met; the slot is
empty until he writes it, and the test below says so.
"""

from __future__ import annotations

import re
from dataclasses import replace

import pytest

from protea.core.contracts.method import Family
from protea.core.families import (
    FAMILIES,
    FamilyEntry,
    families_without_introduction,
    family_entry,
)

_SLUG = re.compile(r"[a-z0-9][a-z0-9-]*")


def test_the_registry_holds_exactly_the_families_of_the_contract() -> None:
    assert set(FAMILIES) == set(Family)
    assert all(entry.family is family for family, entry in FAMILIES.items())


class TestTheTwoDescriptors:
    def test_the_short_descriptor_is_a_name_under_adr_d58(self) -> None:
        names = [entry.short_name for entry in FAMILIES.values()]
        assert len(set(names)) == len(names)
        for entry in FAMILIES.values():
            assert len(entry.short_name) <= 64
            assert _SLUG.fullmatch(entry.short_name)
            # The family's own slug: one name, so a sentence and a key agree.
            assert entry.short_name == entry.family.value

    def test_the_long_descriptor_is_a_distinct_sentence(self) -> None:
        descriptions = [entry.description for entry in FAMILIES.values()]
        assert len(set(descriptions)) == len(descriptions)
        for text in descriptions:
            assert text.endswith(".")
            assert text.isascii()
            assert len(text) > 40

    def test_each_long_descriptor_says_what_separates_its_family(self) -> None:
        # The line ADR-D60's decision 2 gives each family, which is what tells
        # it from the other two.
        assert "reranker" in FAMILIES[Family.EMBEDDING_RERANKER].description
        assert "JEPA" in FAMILIES[Family.WORLD_MODEL].description
        assert "PLM-free" in FAMILIES[Family.EXPLAINABLE_UNITS].description


class TestTheIntroductionsAreTheAuthors:
    def test_every_entry_carries_the_slot_and_none_is_filled(self) -> None:
        # When the author writes an introduction, this test changes in the same
        # commit: the slot moves from None to his text.
        assert all(entry.introduction is None for entry in FAMILIES.values())

    def test_the_milestone_check_names_every_family_still_waiting(self) -> None:
        assert families_without_introduction() == tuple(Family)

    def test_a_blank_introduction_still_counts_as_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import protea.core.families as module

        written: dict[Family, FamilyEntry] = {
            family: replace(entry, introduction="A text the author wrote.")
            for family, entry in FAMILIES.items()
        }
        written[Family.WORLD_MODEL] = replace(written[Family.WORLD_MODEL], introduction="   ")
        monkeypatch.setattr(module, "FAMILIES", written)
        assert families_without_introduction() == (Family.WORLD_MODEL,)


class TestLookup:
    @pytest.mark.parametrize("family", list(Family))
    def test_by_enum_and_by_slug(self, family: Family) -> None:
        assert family_entry(family) is FAMILIES[family]
        assert family_entry(family.value) is FAMILIES[family]

    def test_an_unknown_family_is_refused(self) -> None:
        # "arm" already names an evaluated configuration; it is not a family.
        with pytest.raises(ValueError, match="arm"):
            family_entry("arm")

    def test_the_registry_cannot_be_edited_at_run_time(self) -> None:
        with pytest.raises(TypeError):
            FAMILIES[Family.WORLD_MODEL] = FAMILIES[Family.WORLD_MODEL]  # type: ignore[index]
