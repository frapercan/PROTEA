"""A scored set stores only its scores: one row per protein, two aligned arrays (7c4e1b9d2a50).

Without a database: the migration follows the information accretion cohort and
installs the model's checks by the same text. Against Postgres: the checks
refuse arrays that do not line up and a protein with nothing scored, the
downgrade refuses while a scored set holds scores and on an empty table drops
it, and the upgrade builds it again.
"""

from __future__ import annotations

import importlib.util
import pathlib
import uuid
from types import ModuleType

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, insert, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.embedding import prediction_score as model
from protea.infrastructure.orm.models.embedding.prediction_score import PredictionScore
from tests.helpers.prediction_sets import make_parents, make_prediction_set

_VERSIONS = pathlib.Path(__file__).resolve().parents[1] / "alembic" / "versions"


def _module(revision: str) -> ModuleType:
    """A migration loaded by path: ``alembic/`` is not a package."""
    (path,) = _VERSIONS.glob(f"{revision}_*.py")
    spec = importlib.util.spec_from_file_location(f"_{revision}", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


MIGRATION = _module("7c4e1b9d2a50")


def test_it_follows_the_information_accretion_cohort() -> None:
    assert MIGRATION.down_revision == "2b7e4d9a6c31"


def test_it_installs_the_models_checks() -> None:
    assert MIGRATION.ALIGNED_CHECK == model.ALIGNED_CHECK
    assert MIGRATION.NOT_EMPTY_CHECK == model.NOT_EMPTY_CHECK


# --------------------------------------------------------------- against Postgres


@pytest.fixture()
def pg_engine(postgres_url: str):
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


def _a_scored_set(engine, *, go_term_ids: list[int], scores: list[float]) -> uuid.UUID:
    with Session(engine, future=True) as session:
        set_id = make_prediction_set(session, make_parents(session), {"scored_from": "x"})
        session.execute(
            insert(PredictionScore.__table__),
            [
                {
                    "prediction_set_id": set_id,
                    "protein_accession": "P1",
                    "go_term_ids": go_term_ids,
                    "scores": scores,
                }
            ],
        )
        session.commit()
        return set_id


def _migrate(engine, step: str) -> None:
    with engine.begin() as connection:
        with Operations.context(MigrationContext.configure(connection)):
            getattr(MIGRATION, step)()


@pytest.mark.integration
class TestTheChecks:
    def test_aligned_arrays_are_stored_to_the_last_digit(self, pg_engine) -> None:
        set_id = _a_scored_set(pg_engine, go_term_ids=[3, 7], scores=[0.81234567891, 0.5])
        with pg_engine.connect() as connection:
            stored = connection.execute(
                text(
                    "SELECT go_term_ids, scores FROM prediction_score WHERE prediction_set_id = :s"
                ),
                {"s": set_id},
            ).one()
        assert list(stored[0]) == [3, 7]
        assert list(stored[1]) == [0.81234567891, 0.5]

    def test_arrays_that_do_not_line_up_are_refused(self, pg_engine) -> None:
        with pytest.raises(IntegrityError, match="ck_prediction_score_aligned"):
            _a_scored_set(pg_engine, go_term_ids=[3, 7], scores=[0.5])

    def test_a_protein_with_nothing_scored_has_no_row(self, pg_engine) -> None:
        with pytest.raises(IntegrityError, match="ck_prediction_score_not_empty"):
            _a_scored_set(pg_engine, go_term_ids=[], scores=[])


def _with_terms(engine) -> None:
    """Two terms the arrays can name, in the snapshot of a fresh set's parents."""
    with Session(engine, future=True) as session:
        snapshot = session.execute(text("SELECT id FROM ontology_snapshot LIMIT 1")).scalar_one()
        for term_id, go_id in ((3, "GO:0000003"), (7, "GO:0000007")):
            session.execute(
                text(
                    "INSERT INTO go_term (id, go_id, name, aspect, is_obsolete, "
                    "ontology_snapshot_id) VALUES (:i, :g, :g, 'F', false, :s)"
                ),
                {"i": term_id, "g": go_id, "s": snapshot},
            )
        session.commit()


@pytest.mark.integration
class TestTheEvaluatorReadsTheArrays:
    def test_the_prediction_file_is_the_arrays_as_stored(self, pg_engine, tmp_path) -> None:
        from protea.core.operations import _method_score_predictions as reader
        from protea.core.operations._run_cafa_artifacts import WritePredictionsContext

        set_id = _a_scored_set(pg_engine, go_term_ids=[3, 7], scores=[0.81234567, 0.5])
        _with_terms(pg_engine)
        path = tmp_path / "scores.tsv"
        with Session(pg_engine, future=True) as session:
            reader.refuse_unless_scored(session, set_id)
            reader.write_method_predictions(
                session,
                WritePredictionsContext(pred_set_id=set_id, delta_proteins={"P1"}, path=str(path)),
            )
        assert path.read_text() == "P1\tGO:0000003\t0.8123\nP1\tGO:0000007\t0.5000\n"

    def test_a_set_holding_both_forms_is_refused(self, pg_engine) -> None:
        from protea.core.operations import _method_score_predictions as reader
        from protea.core.scoring import MethodScoreMissing

        set_id = _a_scored_set(pg_engine, go_term_ids=[3], scores=[0.5])
        _with_terms(pg_engine)
        with Session(pg_engine, future=True) as session:
            session.execute(
                text(
                    "INSERT INTO go_prediction (prediction_set_id, protein_accession, go_term_id, "
                    "ref_protein_accession, method_score) VALUES (:s, 'P2', 7, 'm', 0.4)"
                ),
                {"s": set_id},
            )
            with pytest.raises(MethodScoreMissing, match="both forms"):
                reader.refuse_unless_scored(session, set_id)


@pytest.mark.integration
class TestTheMigration:
    def test_the_downgrade_refuses_while_a_scored_set_holds_scores(self, pg_engine) -> None:
        _a_scored_set(pg_engine, go_term_ids=[3], scores=[0.5])
        with pytest.raises(RuntimeError, match="prediction_score holds 1 rows"):
            _migrate(pg_engine, "downgrade")

    def test_on_an_empty_table_it_drops_and_the_upgrade_builds_it_again(self, pg_engine) -> None:
        _migrate(pg_engine, "downgrade")
        assert "prediction_score" not in inspect(pg_engine).get_table_names()
        _migrate(pg_engine, "upgrade")
        assert "prediction_score" in inspect(pg_engine).get_table_names()
        checks = {c["name"] for c in inspect(pg_engine).get_check_constraints("prediction_score")}
        assert {"ck_prediction_score_aligned", "ck_prediction_score_not_empty"} <= checks
