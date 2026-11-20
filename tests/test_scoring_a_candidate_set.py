"""A candidate set is scored once, by its method, and the evaluator reads the scores.

``score_prediction_set`` turns a transfer method's candidates into predictions
with the code the evaluator applied when it still scored: the nearest row per
term, the depth cut and its recount, the bank's denials, and the
ScoringConfig's arithmetic. That the two agreed byte for byte was checked while
both existed, here and on the development store
(``docs/source/_data/scored-set-parity-development-store.json``). These pin,
against Postgres, the prediction file the evaluator writes from a scored set,
value by value, under two configurations with and without a cut; that the chunk
size moves nothing; what the scored set records; and what is refused.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from protea.core.method_seal import EXECUTION_ONLY_FIELDS, METHOD_IDENTITY_FIELDS
from protea.core.operations import _run_cafa_artifacts as artifacts
from protea.core.operations.predict_go_terms._receipt import run_receipt
from protea.core.operations.score_prediction_set import (
    ScorePredictionSetOperation,
    ScorePredictionSetPayload,
)
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_score import PredictionScore
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
from tests.helpers.prediction_sets import make_parents
from tests.test_prediction_run_receipt import _payload

#: A chain PARENT -is_a- CHILD, so a denial of the parent reaches the child,
#: and two terms off it that nothing denies.
_PARENT, _CHILD, _T3, _T4 = 9101, 9102, 9103, 9104
_GO = {_PARENT: "GO:0009101", _CHILD: "GO:0009102", _T3: "GO:0009103", _T4: "GO:0009104"}

#: (protein, term, donor, distance, k_position, vote fraction, donor ledger).
#: Q1's CHILD is denied by the bank; Q2's T4 sits at depth 3 with one donor.
_CANDIDATES: tuple[tuple[str, int, str, float, int, float, list[int]], ...] = (
    ("Q1", _CHILD, "D1", 0.20, 1, 1.0, [1]),
    ("Q1", _T3, "D1", 0.20, 1, 1.0, [1, 2]),
    ("Q1", _T4, "D2", 0.60, 2, 0.5, [2]),
    ("Q2", _T3, "D3", 0.10, 1, 1.0, [1, 3]),
    ("Q2", _T4, "D4", 0.90, 3, 1 / 3, [3]),
    ("Q3", _T4, "D5", 0.30, 1, 1.0, [1]),
)
_PROTEINS = {"Q1", "Q2", "Q3"}


def _noop_emit(*_args: Any, **_kwargs: Any) -> None:
    return None


@pytest.fixture()
def pg(postgres_url: str) -> Any:
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine, future=True) as session:
        yield session
    engine.dispose()


def _seed(session: Session, *, source_meta: dict[str, Any] | None = None) -> dict[str, Any]:
    # The bank's NOT and the candidates name proteins and terms no fixture
    # loads; the foreign keys are not what these tests are about.
    session.execute(text("SET session_replication_role = 'replica'"))
    parents = make_parents(session)
    snap, bank = parents["snapshot"], parents["annotation"]
    for term_id, go_id in _GO.items():
        session.execute(
            text(
                "INSERT INTO go_term (id, go_id, name, aspect, is_obsolete, ontology_snapshot_id) "
                "VALUES (:i, :g, :g, 'F', false, :s)"
            ),
            {"i": term_id, "g": go_id, "s": snap},
        )
    session.execute(
        text(
            "INSERT INTO go_term_relationship "
            "(parent_go_term_id, child_go_term_id, relation_type, ontology_snapshot_id) "
            "VALUES (:p, :c, 'is_a', :s)"
        ),
        {"p": _PARENT, "c": _CHILD, "s": snap},
    )
    session.execute(
        text(
            "INSERT INTO protein_go_annotation "
            "(annotation_set_id, protein_accession, go_term_id, qualifier, evidence_code) "
            "VALUES (:a, 'Q1', :t, 'NOT|enables', 'IDA')"
        ),
        {"a": bank, "t": _PARENT},
    )
    source = PredictionSet(
        embedding_config_id=parents["embedding"],
        annotation_set_id=bank,
        ontology_snapshot_id=snap,
        limit_per_entry=30,
        meta=source_meta if source_meta is not None else {"metric": "cosine"},
    )
    session.add(source)
    session.flush()
    for protein, term, donor, distance, k, vote, ledger in _CANDIDATES:
        session.add(
            GOPrediction(
                prediction_set_id=source.id,
                protein_accession=protein,
                go_term_id=term,
                ref_protein_accession=donor,
                distance=distance,
                evidence_code="IDA",
                k_position=k,
                sequence_rank=k,
                neighbor_vote_fraction=vote,
                donor_count=len(ledger),
                donor_accessions=[f"{donor}-{i}" for i in ledger],
                donor_k_positions=ledger,
                donor_sequence_ranks=ledger,
                donor_distances=[distance + 0.01 * i for i in range(len(ledger))],
            )
        )
    configs = {
        name: ScoringConfig(name=name, formula=formula, weights=weights)
        for name, formula, weights in (
            ("embedding_only", "linear", {"embedding_similarity": 1.0}),
            ("vote_fraction", "linear", {"neighbor_vote_fraction": 1.0}),
            ("method-score", "method_score", {}),
            ("alignment_only", "linear", {"identity_nw": 0.6, "identity_sw": 0.4}),
        )
    }
    session.add_all(configs.values())
    session.flush()
    return {
        "source": source.id,
        "bank": bank,
        "snapshot": snap,
        "embedding": parents["embedding"],
        "configs": configs,
    }


def _score(session: Session, source: uuid.UUID, config: ScoringConfig, **over: Any) -> uuid.UUID:
    payload = {
        "prediction_set_id": str(source),
        "scoring_config_id": str(config.id),
        "_job_id": str(uuid.uuid4()),
        **over,
    }
    result = ScorePredictionSetOperation().execute(session, payload, emit=_noop_emit)
    return uuid.UUID(result.result["prediction_set_id"])


def _file(session: Session, set_id: uuid.UUID, path: Path) -> list[str]:
    """The prediction file the evaluator writes from a set, as lines."""
    artifacts.write_predictions(
        session,
        artifacts.WritePredictionsContext(
            pred_set_id=set_id, delta_proteins=set(_PROTEINS), path=str(path)
        ),
    )
    return path.read_text().splitlines()


def _lines(scores: dict[tuple[str, int], str]) -> list[str]:
    return [f"{protein}\t{_GO[term]}\t{score}" for (protein, term), score in sorted(scores.items())]


#: What each (configuration, cut) has to come to, worked by hand from the seed.
#: Q1's CHILD is gone in every case: the bank denies Q1 its parent. A cut at 2
#: leaves out Q2's T4, retrieved at depth 3, and recounts the vote fraction as
#: voters within the depth over the depth: Q1's T3 had voters at 1 and 2, Q2's
#: T3 at 1 and 3, the others one voter each.
_EXPECTED: dict[tuple[str, int | None], dict[tuple[str, int], str]] = {
    ("embedding_only", None): {
        ("Q1", _T3): "0.9000",
        ("Q1", _T4): "0.7000",
        ("Q2", _T3): "0.9500",
        ("Q2", _T4): "0.5500",
        ("Q3", _T4): "0.8500",
    },
    ("embedding_only", 2): {
        ("Q1", _T3): "0.9000",
        ("Q1", _T4): "0.7000",
        ("Q2", _T3): "0.9500",
        ("Q3", _T4): "0.8500",
    },
    ("vote_fraction", None): {
        ("Q1", _T3): "1.0000",
        ("Q1", _T4): "0.5000",
        ("Q2", _T3): "1.0000",
        ("Q2", _T4): "0.3333",
        ("Q3", _T4): "1.0000",
    },
    ("vote_fraction", 2): {
        ("Q1", _T3): "1.0000",
        ("Q1", _T4): "0.5000",
        ("Q2", _T3): "0.5000",
        ("Q3", _T4): "0.5000",
    },
}


@pytest.mark.integration
class TestTheEvaluatorReadsTheScoredSet:
    @pytest.mark.parametrize("config_name", ["embedding_only", "vote_fraction"])
    @pytest.mark.parametrize("cut", [None, 2], ids=["whole", "cut_at_2"])
    def test_the_prediction_file_holds_the_worked_scores(
        self, pg: Session, tmp_path: Path, config_name: str, cut: int | None
    ) -> None:
        seeded = _seed(pg)
        scored = _score(pg, seeded["source"], seeded["configs"][config_name], max_k_position=cut)
        lines = _file(pg, scored, tmp_path / "scored.tsv")
        assert lines == _lines(_EXPECTED[(config_name, cut)])

    def test_the_chunk_size_moves_nothing(self, pg: Session) -> None:
        seeded = _seed(pg)
        config = seeded["configs"]["vote_fraction"]
        one = _score(pg, seeded["source"], config, proteins_per_chunk=1)
        all_at_once = _score(pg, seeded["source"], config, proteins_per_chunk=5_000)
        assert _stored(pg, one) == _stored(pg, all_at_once)
        assert sum(len(terms) for _, terms, _ in _stored(pg, one)) == len(_CANDIDATES) - 1


def _stored(session: Session, set_id: uuid.UUID) -> list[tuple[str, list[int], list[float]]]:
    """A scored set's rows: protein, its terms and their scores."""
    return [
        (row.protein_accession, list(row.go_term_ids), list(row.scores))
        for row in session.execute(
            select(PredictionScore)
            .where(PredictionScore.prediction_set_id == set_id)
            .order_by(PredictionScore.protein_accession)
        ).scalars()
    ]


@pytest.mark.integration
class TestTheScoresAreStoredCompactly:
    def test_one_row_per_protein_with_its_terms_ascending_and_no_candidate_rows(
        self, pg: Session
    ) -> None:
        seeded = _seed(pg)
        scored = _score(pg, seeded["source"], seeded["configs"]["embedding_only"])
        assert _stored(pg, scored) == [
            ("Q1", [_T3, _T4], [0.9, 0.7]),
            ("Q2", [_T3, _T4], [0.95, 0.55]),
            ("Q3", [_T4], [0.85]),
        ]
        candidate_rows = pg.execute(
            select(GOPrediction.id).where(GOPrediction.prediction_set_id == scored)
        ).all()
        assert candidate_rows == [], "the candidates stay in the candidate set"


@pytest.mark.integration
class TestWhatTheScoredSetRecords:
    def test_its_columns_are_the_candidates_and_its_record_says_how(self, pg: Session) -> None:
        seeded = _seed(pg)
        config = seeded["configs"]["embedding_only"]
        scored = pg.get(PredictionSet, _score(pg, seeded["source"], config, max_k_position=2))
        source = pg.get(PredictionSet, seeded["source"])
        assert scored is not None and source is not None
        for column in ("embedding_config_id", "annotation_set_id", "ontology_snapshot_id"):
            assert getattr(scored, column) == getattr(source, column)
        assert scored.limit_per_entry == source.limit_per_entry
        meta = scored.meta
        assert meta["metric"] == "cosine", "the candidates' record travels with the scores"
        assert meta["scored_from"] == str(source.id)
        assert meta["scoring_config_id"] == str(config.id)
        assert meta["scoring"]["formula"] == "linear"
        assert meta["scoring"]["weights"] == {"embedding_similarity": 1.0}
        assert meta["scoring"]["depth"] == {
            "max_distance": None,
            "max_k_position": 2,
            "max_sequence_rank": None,
        }
        assert meta["scoring"]["code_revision"]
        assert meta["denials"]["honoured"] is True
        assert meta["denied_removed"] == 1
        assert "embedding_only" in (scored.description or "")

    def test_every_key_it_writes_over_a_receipt_is_classified(self, pg: Session) -> None:
        receipt = run_receipt(_payload(), uuid.uuid4())
        seeded = _seed(pg, source_meta=receipt)
        scored = pg.get(
            PredictionSet, _score(pg, seeded["source"], seeded["configs"]["vote_fraction"])
        )
        assert scored is not None
        classified = set(METHOD_IDENTITY_FIELDS) | set(EXECUTION_ONLY_FIELDS)
        assert set(scored.meta) - classified == set()
        assert "scoring" in METHOD_IDENTITY_FIELDS
        assert {"scored_from", "scoring_config_id"} <= set(EXECUTION_ONLY_FIELDS)


@pytest.mark.integration
class TestWhatIsRefused:
    def test_a_set_that_is_scored_already(self, pg: Session) -> None:
        seeded = _seed(pg)
        config = seeded["configs"]["embedding_only"]
        scored = _score(pg, seeded["source"], config)
        with pytest.raises(ValueError, match="is a scored set; its candidates are prediction set"):
            _score(pg, scored, config)

    def test_a_set_that_carries_its_own_scores(self, pg: Session) -> None:
        seeded = _seed(pg)
        pg.execute(
            text("UPDATE go_prediction SET method_score = 0.5 WHERE prediction_set_id = :s"),
            {"s": seeded["source"]},
        )
        with pytest.raises(ValueError, match="scored already"):
            _score(pg, seeded["source"], seeded["configs"]["embedding_only"])

    def test_the_formula_that_returns_a_score_the_candidates_lack(self, pg: Session) -> None:
        seeded = _seed(pg)
        with pytest.raises(ValueError, match="a candidate set has none"):
            _score(pg, seeded["source"], seeded["configs"]["method-score"])

    def test_arithmetic_over_signals_the_candidates_do_not_carry(self, pg: Session) -> None:
        """The seed computes no alignment, so an alignment score would be zero on every
        row: refused by name, with the signals and their coverage, before anything is
        written."""
        seeded = _seed(pg)
        with pytest.raises(ValueError, match=r"identity_nw \(column 'identity_nw': 0/6 rows\)"):
            _score(pg, seeded["source"], seeded["configs"]["alignment_only"])
        written = pg.execute(text("SELECT count(*) FROM prediction_set")).scalar_one()
        assert written == 1, "only the candidate set"

    def test_a_set_that_does_not_exist(self, pg: Session) -> None:
        seeded = _seed(pg)
        with pytest.raises(ValueError, match="not found"):
            _score(pg, uuid.uuid4(), seeded["configs"]["embedding_only"])

    def test_a_set_with_no_candidates(self, pg: Session) -> None:
        seeded = _seed(pg)
        empty = PredictionSet(
            embedding_config_id=seeded["embedding"],
            limit_per_entry=30,
            annotation_set_id=seeded["bank"],
            ontology_snapshot_id=seeded["snapshot"],
            meta={},
        )
        pg.add(empty)
        pg.flush()
        with pytest.raises(ValueError, match="no candidates"):
            _score(pg, empty.id, seeded["configs"]["embedding_only"])


class TestThePayload:
    def test_a_depth_is_counted_in_one_unit(self) -> None:
        with pytest.raises(ValidationError):
            ScorePredictionSetPayload(
                prediction_set_id=str(uuid.uuid4()),
                scoring_config_id=str(uuid.uuid4()),
                max_k_position=2,
                max_sequence_rank=2,
            )

    def test_the_arithmetic_is_named_never_defaulted(self) -> None:
        with pytest.raises(ValidationError):
            ScorePredictionSetPayload.model_validate({"prediction_set_id": str(uuid.uuid4())})
