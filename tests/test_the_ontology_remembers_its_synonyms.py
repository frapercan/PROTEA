"""What a snapshot says about an id it no longer uses as primary.

``go_term`` keeps five things about a term and none of them is identity across
releases. The OBO makes three statements this store has been dropping since
the first load, and they are not interchangeable:

    alt_id        this id IS that term, under an older name
    replaced_by   that is what this became
    consider      we are not telling you

A remapping may follow the first two and must not follow the third, so the
rows record which statement was made and leave the decision to the rule. The
lab's own ``build_remap`` ignores ``consider`` for exactly that reason.

MEASURED ON THE ARCHIVED 2022-07-01 OBO, which is the pivot of the long
validation window and whose sha256 is verified on both machines: 3,590
``alt_id``, 549 ``replaced_by``, 2,211 ``consider``, over 47,328 terms of
which 3,770 obsolete. The lab counted the same four numbers on their own copy
and on ``go.obo`` as well as ``go-basic.obo``, so these are the figures the
loader has to produce.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from protea.core.operations.load_ontology_aliases import (
    LoadOntologyAliasesPayload,
    parse_aliases,
)
from protea.infrastructure.orm.models.annotation.go_term_alias import (
    KIND_ALT_ID,
    KIND_CONSIDER,
    KIND_REPLACED_BY,
    KINDS,
    GOTermAlias,
)

OBO = """format-version: 1.2
data-version: releases/2022-07-01

[Term]
id: GO:0000003
name: reproduction
alt_id: GO:0019952
alt_id: GO:0050876
namespace: biological_process

[Term]
id: GO:0000108
name: obsolete repairosome
is_obsolete: true
replaced_by: GO:0000109

[Term]
id: GO:0000039
name: obsolete plasma membrane long-chain fatty acid transporter
is_obsolete: true
consider: GO:0005324

[Term]
id: GO:0000055
name: obsolete ribosomal large subunit export
is_obsolete: true
replaced_by: GO:0033750
replaced_by: GO:0000055

[Typedef]
id: part_of
alt_id: GO:9999999
"""

#: The archived pivot of the long window, by the path the test takes it from.
#: Skipped rather than fetched when absent: a test must never download 31 MB.
_PIVOT = Path.home() / "Thesis-laptop" / "PROTEA"


class TestWhatTheParserReads:
    def test_an_alt_id_points_from_the_old_id_to_the_term(self) -> None:
        # The direction a lookup needs: given an id an annotation still
        # carries, which term is it now.
        rows = parse_aliases(OBO)
        alt = {(a, p) for a, p, k in rows if k == KIND_ALT_ID}
        assert alt == {("GO:0019952", "GO:0000003"), ("GO:0050876", "GO:0000003")}

    def test_a_replaced_by_points_from_the_obsolete_to_its_successor(self) -> None:
        rows = parse_aliases(OBO)
        repl = {(a, p) for a, p, k in rows if k == KIND_REPLACED_BY}
        assert ("GO:0000108", "GO:0000109") in repl

    def test_several_successors_are_all_recorded(self) -> None:
        # The ROWS keep what the ontology said; refusing to remap a term with
        # more than one successor is the rule's decision and lives with the
        # rule. Recording only the first would take that decision here, in
        # the one place that cannot explain it.
        rows = parse_aliases(OBO)
        for_term = {p for a, p, k in rows if k == KIND_REPLACED_BY and a == "GO:0000055"}
        assert for_term == {"GO:0033750", "GO:0000055"}

    def test_consider_is_recorded_and_kept_apart(self) -> None:
        # Recorded because the ABSENCE of a successor is the evidence that an
        # annotation cannot be carried forward; kept apart because the lab's
        # remap ignores it and ours has to be able to.
        rows = parse_aliases(OBO)
        assert ("GO:0000039", "GO:0005324", KIND_CONSIDER) in rows

    def test_a_typedef_alt_id_is_not_a_term_alias(self) -> None:
        # The stanza is not a [Term], so its alt_id is not an id any
        # annotation can carry.
        assert "GO:9999999" not in {a for a, _, _ in parse_aliases(OBO)}

    def test_every_row_carries_one_of_the_three_kinds(self) -> None:
        assert {k for _, _, k in parse_aliases(OBO)} <= set(KINDS)

    def test_an_empty_file_yields_nothing(self) -> None:
        assert parse_aliases("") == []


class TestThePayloadAndTheTable:
    def test_the_snapshot_is_required(self) -> None:
        with pytest.raises(ValueError):
            LoadOntologyAliasesPayload(ontology_snapshot_id="   ")

    def test_it_does_not_rewrite_by_default(self) -> None:
        assert LoadOntologyAliasesPayload(ontology_snapshot_id="x").force is False

    def test_the_identity_of_a_row_is_the_statement_it_records(self) -> None:
        # (snapshot, alias, primary, kind): the same old id can be the
        # secondary id of one term and the `consider` of another, and
        # collapsing them would lose which statement was made.
        unique = [
            c
            for c in GOTermAlias.__table__.constraints
            if getattr(c, "name", "") == "uq_go_term_alias_identity"
        ]
        assert unique, "the identity constraint is gone"
        assert {col.name for col in unique[0].columns} == {
            "ontology_snapshot_id",
            "alias_go_id",
            "primary_go_id",
            "kind",
        }

    def test_the_lookup_the_remapping_does_has_an_index(self) -> None:
        names = {ix.name for ix in GOTermAlias.__table__.indexes}
        assert "ix_go_term_alias_lookup" in names


@pytest.mark.skipif(
    not (_PIVOT / ".env").is_file(),
    reason="needs this server's archived pivot; the counts below are its own",
)
class TestTheArchivedPivotGivesTheLabsNumbers:
    """The four numbers the lab counted on the same file, on both OBO flavours.

    This is the acceptance criterion for the loader: if the parser disagrees
    with these, the alias map describes a different ontology from the one the
    window is scored in.
    """

    EXPECTED = {KIND_ALT_ID: 3590, KIND_REPLACED_BY: 549, KIND_CONSIDER: 2211}

    @pytest.fixture(scope="class")
    def rows(self) -> list[tuple[str, str, str]]:
        import os

        local = os.environ.get("PROTEA_PIVOT_OBO", "")
        if not local or not Path(local).is_file():
            pytest.skip("set PROTEA_PIVOT_OBO to the archived 2022-07-01 go-basic.obo")
        return parse_aliases(Path(local).read_text(errors="replace"))

    @pytest.mark.parametrize("kind", sorted(EXPECTED))
    def test_the_count_matches(self, rows: list[tuple[str, str, str]], kind: str) -> None:
        assert sum(1 for _, _, k in rows if k == kind) == self.EXPECTED[kind]

    def test_the_nine_entries_live_in_the_replaced_by_half(
        self, rows: list[tuple[str, str, str]]
    ) -> None:
        # The two terms behind three of the four proteins that separate
        # PROTEA's delta from the lab's: obsolete in 2022 with a successor,
        # and revived by GO in 2025.
        repl = {a: p for a, p, k in rows if k == KIND_REPLACED_BY}
        assert repl.get("GO:0003788") == "GO:0042989"
        assert repl.get("GO:0009003") == "GO:0008233"
