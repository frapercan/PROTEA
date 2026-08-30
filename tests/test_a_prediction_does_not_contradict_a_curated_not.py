"""A curated NOT is evidence against exactly the inference this method makes.

WHY THIS TEST EXISTS. On the 2026-08-30 campaign the run predicted, in first
place, that fission yeast PTEN (O94526) has
phosphatidylinositol-4,5-bisphosphate 5-phosphatase activity. A curator had
already determined by direct assay that it does NOT, and said so in the bank
the run transferred from. The donor was at distance 0.015. The annotation
exists precisely because the homology is misleading there, and the run walked
into the trap the annotation documents.

Across the query set: 956 direct NOT annotations, 478 of them predicted anyway,
298 against experimental evidence, at a median k_position of 4.

Two properties have to hold together, and only the pair is worth anything.
A denial propagates DOWN the ontology, because "does not have the parent"
implies "does not have any descendant", so the filter must remove a child of a
denied term and not only the term itself. And it must remove nothing else: a
filter that quietly emptied the frame would post a perfect score on this test
while destroying every run. The last case is therefore the positive control,
and it is the reason a zero here cannot be vacuous.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from protea.core.operations._denials import (
    denied_pairs,
    drop_contradictions,
    drop_denied_records,
)
from protea.core.operations._run_cafa_artifacts import WritePredictionsContext
from protea.infrastructure.orm.base import Base

_PRED_SET = uuid.UUID("33333333-3333-3333-3333-333333333333")
_BANK = uuid.UUID("44444444-4444-4444-4444-444444444444")
_SNAP = uuid.UUID("55555555-5555-5555-5555-555555555555")

#: A three-term chain, so descent has somewhere to go: PARENT -is_a- CHILD
#: -is_a- GRANDCHILD. Plus one term off the chain that nothing denies.
_PARENT, _CHILD, _GRANDCHILD, _UNRELATED = 101, 102, 103, 104
_GO = {
    _PARENT: "GO:0000101",
    _CHILD: "GO:0000102",
    _GRANDCHILD: "GO:0000103",
    _UNRELATED: "GO:0000104",
}


def _seed(session: Session, *, relation: str = "is_a") -> None:
    session.execute(text("SET session_replication_role = 'replica'"))
    for tid, go_id in _GO.items():
        session.execute(
            text(
                "INSERT INTO go_term "
                "(id, go_id, name, aspect, is_obsolete, ontology_snapshot_id) "
                "VALUES (:i, :g, :g, 'F', false, :s)"
            ),
            {"i": tid, "g": go_id, "s": _SNAP},
        )
    for parent, child in ((_PARENT, _CHILD), (_CHILD, _GRANDCHILD)):
        session.execute(
            text(
                "INSERT INTO go_term_relationship "
                "(parent_go_term_id, child_go_term_id, relation_type, ontology_snapshot_id) "
                "VALUES (:p, :c, :r, :s)"
            ),
            {"p": parent, "c": child, "r": relation, "s": _SNAP},
        )
    # The curator denies the PARENT, with evidence, in the bank.
    session.execute(
        text(
            "INSERT INTO protein_go_annotation "
            "(annotation_set_id, protein_accession, go_term_id, qualifier, evidence_code) "
            "VALUES (:a, 'O94526', :t, 'NOT|enables', 'IDA')"
        ),
        {"a": _BANK, "t": _PARENT},
    )
    session.execute(
        text(
            "INSERT INTO prediction_set "
            "(id, annotation_set_id, ontology_snapshot_id, embedding_config_id, "
            "query_set_id, limit_per_entry, meta) "
            "VALUES (:i, :a, :s, :e, :q, 30, '{}'::jsonb)"
        ),
        {"i": _PRED_SET, "a": _BANK, "s": _SNAP, "e": uuid.uuid4(), "q": uuid.uuid4()},
    )
    session.flush()


def _ctx() -> WritePredictionsContext:
    return WritePredictionsContext(
        pred_set_id=_PRED_SET,
        delta_proteins={"O94526", "P00000"},
        max_distance=None,
        path="/dev/null",
    )


@pytest.mark.integration
class TestADenialReachesTheWholeSubtree:
    def test_it_descends_the_dag_it_does_not_stop_at_the_named_term(
        self, postgres_url: str
    ) -> None:
        engine = create_engine(postgres_url, future=True)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        with Session(engine, future=True) as session:
            _seed(session)
            denied = denied_pairs(session, _ctx())

        # One curated NOT, three denied pairs. The child and grandchild are
        # denied because the protein cannot have a descendant of something it
        # does not have.
        assert denied == {
            ("O94526", _GO[_PARENT]),
            ("O94526", _GO[_CHILD]),
            ("O94526", _GO[_GRANDCHILD]),
        }

    def test_it_does_not_travel_a_relation_a_denial_cannot_cross(
        self, postgres_url: str
    ) -> None:
        """A regulates edge does not carry an annotation, so it carries no
        denial either. Only is_a and part_of propagate."""
        engine = create_engine(postgres_url, future=True)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        with Session(engine, future=True) as session:
            _seed(session, relation="regulates")
            denied = denied_pairs(session, _ctx())

        assert denied == {("O94526", _GO[_PARENT])}

    def test_it_reads_the_bank_off_the_prediction_set_with_no_help(
        self, postgres_url: str
    ) -> None:
        """The depth cut spent a campaign unapplied because two construction
        sites both had to remember to fill a field. This one reads its own
        inputs off the row it is about, so there is nothing to forget."""
        engine = create_engine(postgres_url, future=True)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        with Session(engine, future=True) as session:
            _seed(session)
            ctx = _ctx()
            assert ctx.bank_annotation_set_id is None
            assert ctx.denial_snapshot_id is None
            assert len(denied_pairs(session, ctx)) == 3


class TestTheFilterRemovesTheDeniedAndNothingElse:
    """Run on the returned set directly: no database is needed to state what
    the filter must do once it has been told what is denied."""

    _DENIED = {("O94526", _GO[_PARENT]), ("O94526", _GO[_CHILD])}

    def _frame(self):
        import pandas as pd

        return pd.DataFrame(
            [
                ("O94526", _GO[_PARENT]),     # denied outright
                ("O94526", _GO[_CHILD]),      # denied by descent
                ("O94526", _GO[_UNRELATED]),  # not denied: must survive
                ("P00000", _GO[_PARENT]),     # denied for another protein only
            ],
            columns=["protein_accession", "go_id"],
        )

    def test_the_denied_pairs_go(self) -> None:
        out, removed = drop_contradictions(self._frame(), self._DENIED)
        assert removed == 2
        assert set(zip(out.protein_accession, out.go_id, strict=True)) == {
            ("O94526", _GO[_UNRELATED]),
            ("P00000", _GO[_PARENT]),
        }

    def test_a_denial_is_about_one_protein_not_about_the_term(self) -> None:
        """THE POSITIVE CONTROL. P00000 keeps the very term O94526 is denied,
        because nobody said anything about P00000. Without this the filter
        could pass every other case by emptying the frame."""
        out, _ = drop_contradictions(self._frame(), self._DENIED)
        assert ("P00000", _GO[_PARENT]) in set(
            zip(out.protein_accession, out.go_id, strict=True)
        )

    def test_nothing_denied_means_nothing_touched(self) -> None:
        frame = self._frame()
        out, removed = drop_contradictions(frame, set())
        assert removed == 0
        assert out is frame


@pytest.mark.integration
class TestEveryWriterAppliesIt:
    def test_the_reranker_path_filters_the_same_pairs(self, postgres_url: str) -> None:
        """The campaign runs through the reranker, which never builds the
        cached base frame. A filter some writers apply and others do not is
        the same defect wearing a different name."""
        engine = create_engine(postgres_url, future=True)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        with Session(engine, future=True) as session:
            _seed(session)
            records = [
                {"protein_accession": "O94526", "go_id": _GO[_GRANDCHILD]},
                {"protein_accession": "O94526", "go_id": _GO[_UNRELATED]},
                {"protein_accession": "P00000", "go_id": _GO[_PARENT]},
            ]
            kept = drop_denied_records(session, _ctx(), records)

        assert [r["go_id"] for r in kept] == [_GO[_UNRELATED], _GO[_PARENT]]
