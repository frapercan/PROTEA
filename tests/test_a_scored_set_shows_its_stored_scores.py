"""A scored set shows the scores it stored, over its candidate set's rows.

A scored set holds no ``go_prediction`` rows of its own (``score_prediction_set``):
its rows are its candidate set's, and its scores are one ``prediction_score`` row
per protein. The per-protein read used to look for rows of the scored set itself,
so the detail page of every scored set showed nothing. Now it reads the candidate
set's rows with the score each one stored, highest first, leaves out a candidate
the scoring removed, and the set's detail names the candidate set it came from.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine, insert, text
from sqlalchemy.orm import Session

from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.embedding.prediction_score import PredictionScore
from protea.services._embeddings_predictions_helpers import build_predictions_for_protein
from protea.services.embeddings_service import get_prediction_set_data
from tests.helpers.prediction_sets import make_parents, make_prediction_set

#: Three candidate terms of one protein, nearest donor first.
CANDIDATES = ((3, "GO:0000003", 0.1), (7, "GO:0000007", 0.2), (9, "GO:0000009", 0.3))


@pytest.fixture()
def sets(postgres_url: str):
    """A candidate set and a scored set of it; the scoring removed GO:0000007."""
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine, future=True) as session:
        parents = make_parents(session)
        candidates = make_prediction_set(session, parents, {})
        scored = make_prediction_set(
            session, parents, {"scored_from": str(candidates), "scoring_config_id": "cfg-1"}
        )
        session.execute(text("SET session_replication_role = 'replica'"))
        for term_id, go_id, distance in CANDIDATES:
            session.execute(
                text(
                    "INSERT INTO go_term (id, go_id, name, aspect, is_obsolete, "
                    "ontology_snapshot_id) VALUES (:i, :g, :g, 'F', false, :s)"
                ),
                {"i": term_id, "g": go_id, "s": parents["snapshot"]},
            )
            session.execute(
                text(
                    "INSERT INTO go_prediction (prediction_set_id, protein_accession, go_term_id, "
                    "ref_protein_accession, distance) VALUES (:p, 'P1', :t, 'R1', :d)"
                ),
                {"p": candidates, "t": term_id, "d": distance},
            )
        session.execute(
            insert(PredictionScore.__table__),
            [
                {
                    "prediction_set_id": scored,
                    "protein_accession": "P1",
                    "go_term_ids": [3, 9],
                    "scores": [0.2, 0.8],
                }
            ],
        )
        session.commit()
        yield engine, candidates, scored
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.mark.integration
class TestTheRowsOfAScoredSet:
    def test_they_are_its_candidates_with_the_score_each_stored_highest_first(self, sets) -> None:
        engine, _, scored = sets
        with Session(engine, future=True) as session:
            rows = build_predictions_for_protein(session, prediction_set_id=scored, accession="P1")
        assert [(r["go_id"], r["score"]) for r in rows] == [
            ("GO:0000009", 0.8),
            ("GO:0000003", 0.2),
        ]
        assert all(r["ref_protein_accession"] == "R1" for r in rows)

    def test_a_candidate_set_keeps_its_own_rows_and_carries_no_score(self, sets) -> None:
        engine, candidates, _ = sets
        with Session(engine, future=True) as session:
            rows = build_predictions_for_protein(
                session, prediction_set_id=candidates, accession="P1"
            )
        assert [r["go_id"] for r in rows] == ["GO:0000003", "GO:0000007", "GO:0000009"]
        assert {r["score"] for r in rows} == {None}

    def test_a_protein_the_scored_set_holds_no_scores_for_has_no_rows(self, sets) -> None:
        engine, _, scored = sets
        with Session(engine, future=True) as session:
            assert (
                build_predictions_for_protein(session, prediction_set_id=scored, accession="P2")
                == []
            )

    def test_the_detail_names_the_candidate_set_and_the_configuration(self, sets) -> None:
        engine, candidates, scored = sets
        with Session(engine, future=True) as session:
            detail = get_prediction_set_data(session, scored)
            plain = get_prediction_set_data(session, candidates)
        assert (detail["scored_from"], detail["scoring_config_id"]) == (str(candidates), "cfg-1")
        assert (plain["scored_from"], plain["scoring_config_id"]) == (None, None)
        assert isinstance(uuid.UUID(detail["scored_from"]), uuid.UUID)
