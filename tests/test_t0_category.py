"""The t0 category: what a method may key by, read from the window's t0 side alone.

A protein is partial knowledge in an aspect when its closed t0 experimental
state holds a term there, limited when it holds one elsewhere only, and no
knowledge otherwise. The state is LAFA's t0 end: its rows named under t0's
ontology, its own t0 NOT terms (and everything under them) taken out of its
direct rows, closed upwards. Nothing of t1 is read, nor the truth, nor the
buckets: the window object handed over here carries no t1 attribute at all, and
on Postgres the result is the same with the t1 corpus emptied.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy.orm import Session

from protea.core import t0_category as module
from protea.core._lafa_frame_data import EndOntology, load_end_rows
from protea.core.domain.category import Category
from protea.core.t0_category import (
    T0Categories,
    T0CategoryUndefined,
    categories_of,
    disagreement_table,
    disagreements,
    t0_categories,
)
from tests.test_run_count_map import pg  # noqa: F401

NK, LK, PK = Category.NO_KNOWLEDGE, Category.LIMITED_KNOWLEDGE, Category.PARTIAL_KNOWLEDGE

T0 = EndOntology(
    live={
        "GO:0008150": "P",
        "GO:0000002": "P",
        "GO:0000003": "P",
        "GO:0000004": "P",
        "GO:0003674": "F",
        "GO:0000011": "F",
    },
    parents={
        "GO:0000002": frozenset({"GO:0008150"}),
        "GO:0000003": frozenset({"GO:0000002"}),
        "GO:0000004": frozenset({"GO:0000002"}),
        "GO:0000011": frozenset({"GO:0003674"}),
    },
    alt_ids={"GO:0000033": "GO:0000003"},
    replaced_by={"GO:0000044": "GO:0000004"},
)


def _t0_window(profile: str = "own_graph") -> SimpleNamespace:
    """A window holding its t0 side only: reading anything of t1 raises AttributeError."""
    return SimpleNamespace(
        id=uuid.uuid4(),
        truth_profile=profile,
        old_annotation_set_id=uuid.uuid4(),
        old_native_snapshot_id=uuid.uuid4(),
        not_codes="experimental",
    )


class TestTheCategories:
    def test_partial_limited_and_none_per_aspect(self) -> None:
        found = categories_of(
            ["P1", "P2", "P3"],
            {"P1": {"P": {"GO:0000002"}}, "P2": {"P": {"GO:0000002"}, "F": {"GO:0003674"}}},
        )
        assert found[("P1", "P")] is PK
        assert found[("P1", "F")] is LK and found[("P1", "C")] is LK
        assert found[("P2", "P")] is PK and found[("P2", "F")] is PK and found[("P2", "C")] is LK
        assert {found[("P3", a)] for a in "PFC"} == {NK}

    def test_an_aspect_with_an_empty_set_holds_nothing(self) -> None:
        found = categories_of(["P1"], {"P1": {"P": set()}})
        assert {found[("P1", a)] for a in "PFC"} == {NK}


class TestReadFromTheT0SideAlone:
    def _patched(self, monkeypatch: pytest.MonkeyPatch, asked: dict) -> None:
        def ontology(_session: object, snapshot: object) -> EndOntology:
            asked["snapshot"] = snapshot
            return T0

        def rows(_session: object, set_id: object, experimental_only: bool, accessions=None):
            asked.update(set=set_id, experimental_only=experimental_only, accessions=accessions)
            positives = {
                "P1": {"GO:0000033"},  # an alternative id, named as its primary
                "P2": {"GO:0000004", "GO:0000011"},
                "P3": {"GO:0000044"},  # replaced, so it names its successor
                "P4": {"GO:0000003"},
            }
            # P4 is denied at t0 what it holds: the NOT takes it out, with what is under it.
            return positives, {"P4": {"GO:0000002"}}

        monkeypatch.setattr(module, "load_end_ontology", ontology)
        monkeypatch.setattr(module, "load_end_rows", rows)

    def test_the_categories_come_from_the_t0_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        asked: dict = {}
        self._patched(monkeypatch, asked)
        window = _t0_window()
        found = t0_categories(None, window, ["P4", "P1", "P2", "P3", "P5"])  # type: ignore[arg-type]
        assert found.category("P1", "P") is PK and found.category("P1", "F") is LK
        assert found.category("P2", "P") is PK and found.category("P2", "F") is PK
        assert found.category("P3", "P") is PK
        # The t0 NOT removed P4's only direct term: P4 knows nothing at t0.
        assert {found.category("P4", a) for a in "PFC"} == {NK}
        assert {found.category("P5", a) for a in "PFC"} == {NK}
        assert asked == {
            "snapshot": window.old_native_snapshot_id,
            "set": window.old_annotation_set_id,
            "experimental_only": True,
            "accessions": ["P1", "P2", "P3", "P4", "P5"],
        }

    def test_a_window_of_another_frame_has_no_t0_category(self) -> None:
        with pytest.raises(T0CategoryUndefined, match="LAFA's frame"):
            t0_categories(None, _t0_window("as_closed"), ["P1"])  # type: ignore[arg-type]

    def test_a_protein_not_read_is_refused_rather_than_called_no_knowledge(self) -> None:
        found = T0Categories("w", categories_of(["P1"], {}))
        with pytest.raises(KeyError, match="read for named proteins only"):
            found.category("P2", "P")


class TestTheDisagreementTable:
    def test_counts_the_t0_category_against_the_evaluators(self) -> None:
        t0 = T0Categories("w", categories_of(["P1", "P2"], {"P1": {"P": {"GO:0000002"}}}))
        table = disagreement_table(
            t0, [("P1", "P", "PK"), ("P1", "F", "PK"), ("P2", "C", "NK"), ("P2", "P", "LK")]
        )
        assert table["PK"]["PK"] == 1
        assert table["LK"]["PK"] == 1
        assert table["NK"]["NK"] == 1 and table["NK"]["LK"] == 1
        assert disagreements(table) == 2
        assert t0.counts()["P"] == {"NK": 1, "LK": 0, "PK": 1}


# Against Postgres --------------------------------------------------------------------


@pytest.fixture()
def lafa_store(pg: Session) -> SimpleNamespace:  # noqa: F811
    """A t0 and a t1 corpus under one ontology, and the t0 side of a window over them."""
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.orm.models.annotation.go_term import GOTerm
    from protea.infrastructure.orm.models.annotation.go_term_alias import GOTermAlias
    from protea.infrastructure.orm.models.annotation.go_term_relationship import (
        GOTermRelationship,
    )
    from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
    from protea.infrastructure.orm.models.annotation.protein_go_annotation import (
        ProteinGOAnnotation,
    )
    from protea.infrastructure.orm.models.protein.protein import Protein

    snapshot = OntologySnapshot(obo_version="releases/t0-category", obo_url="file:///dev/null")
    pg.add(snapshot)
    pg.add_all(Protein(accession=a, canonical_accession=a) for a in ("P1", "P2", "P3", "P4"))
    pg.flush()
    terms = {
        go: GOTerm(go_id=go, aspect=aspect, ontology_snapshot_id=snapshot.id)
        for go, aspect in T0.live.items()
    }
    pg.add_all(terms.values())
    pg.flush()
    for child, parents in T0.parents.items():
        for parent in parents:
            pg.add(
                GOTermRelationship(
                    child_go_term_id=terms[child].id,
                    parent_go_term_id=terms[parent].id,
                    relation_type="is_a",
                    ontology_snapshot_id=snapshot.id,
                )
            )
    pg.add(
        GOTermAlias(
            ontology_snapshot_id=snapshot.id,
            alias_go_id="GO:0000033",
            primary_go_id="GO:0000003",
            kind="alt_id",
        )
    )
    corpora = {}
    rows = {
        "t0": [
            ("P1", "GO:0000003", "IDA", None),
            ("P2", "GO:0000011", "EXP", None),
            ("P3", "GO:0000003", "IDA", None),
            ("P3", "GO:0000002", "IMP", "NOT|involved_in"),
            ("P4", "GO:0000004", "IEA", None),
        ],
        "t1": [
            ("P1", "GO:0000011", "IDA", None),
            ("P2", "GO:0000011", "EXP", "NOT|enables"),
            ("P4", "GO:0000004", "IDA", None),
        ],
    }
    for end, body in rows.items():
        corpus = AnnotationSet(source="goa", ontology_snapshot_id=snapshot.id)
        pg.add(corpus)
        pg.flush()
        corpora[end] = corpus.id
        for accession, go, code, qualifier in body:
            pg.add(
                ProteinGOAnnotation(
                    annotation_set_id=corpus.id,
                    protein_accession=accession,
                    go_term_id=terms[go].id,
                    evidence_code=code,
                    qualifier=qualifier,
                )
            )
    pg.commit()
    return SimpleNamespace(
        window=SimpleNamespace(
            id=uuid.uuid4(),
            truth_profile="own_graph",
            old_annotation_set_id=corpora["t0"],
            old_native_snapshot_id=snapshot.id,
            not_codes="experimental",
        ),
        t1=corpora["t1"],
    )


@pytest.mark.integration
def test_on_postgres_the_t1_corpus_changes_nothing(
    pg: Session,  # noqa: F811
    lafa_store: SimpleNamespace,
) -> None:
    from sqlalchemy import delete

    from protea.infrastructure.orm.models.annotation.protein_go_annotation import (
        ProteinGOAnnotation,
    )

    accessions = ["P1", "P2", "P3", "P4"]
    before = t0_categories(pg, lafa_store.window, accessions)
    assert before.category("P1", "P") is PK and before.category("P1", "F") is LK
    assert before.category("P2", "F") is PK and before.category("P2", "P") is LK
    # P3's t0 NOT on GO:0000002 takes GO:0000003 under it: nothing is left at t0.
    assert {before.category("P3", a) for a in "PFC"} == {NK}
    # P4 holds only an electronic row at t0: not experimental, so no knowledge.
    assert {before.category("P4", a) for a in "PFC"} == {NK}
    pg.execute(
        delete(ProteinGOAnnotation).where(ProteinGOAnnotation.annotation_set_id == lafa_store.t1)
    )
    pg.commit()
    after = t0_categories(pg, lafa_store.window, accessions)
    assert after.categories == before.categories


@pytest.mark.integration
def test_on_postgres_the_rows_read_are_the_named_proteins_only(
    pg: Session,  # noqa: F811
    lafa_store: SimpleNamespace,
) -> None:
    window = lafa_store.window
    every, _ = load_end_rows(pg, window.old_annotation_set_id, True)
    some, denied = load_end_rows(pg, window.old_annotation_set_id, True, accessions=["P3"])
    assert set(every) == {"P1", "P2", "P3"}
    assert set(some) == {"P3"} and set(denied) == {"P3"}
    none, nothing = load_end_rows(pg, window.old_annotation_set_id, True, accessions=[])
    assert none == {} and nothing == {}
