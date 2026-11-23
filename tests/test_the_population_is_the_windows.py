"""The population an evaluation scores is the window's: every target, as LAFA scores.

Until 2026-11-20 a request could restrict the truth to the proteins its prediction set
predicted, and did by default, behind a gate on how much that dropped. LAFA counts an
unpredicted target as a failure and shows coverage beside the number; the restriction
scored a different quantity, and from #1246 on it read the predicted proteins from
``go_prediction`` only, so on a scored set, whose scores live in ``prediction_score``, it
found none, dropped every target and refused the evaluation.

Now the request cannot restrict, the evaluator writes the window's whole truth, and the
result records ``results.population``, which the frame seal reads: a result scored over
every target seals as a request from before that turned the restriction off, because
the two scored the same population.
"""

from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from protea.core.evaluation import EvaluationData
from protea.core.operations._run_cafa_setup import POPULATION_RULE, population_record
from protea.core.operations.run_cafa_evaluation import RunCafaEvaluationPayload
from protea.infrastructure.orm.base import Base


def _request(**extra: object) -> RunCafaEvaluationPayload:
    return RunCafaEvaluationPayload(
        evaluation_set_id=str(uuid.uuid4()), prediction_set_id=str(uuid.uuid4()), **extra
    )


class TestTheRequestCannotRestrict:
    def test_a_request_asking_to_restrict_is_refused_with_where_the_population_comes_from(
        self,
    ) -> None:
        with pytest.raises(
            ValueError, match="restrict_gt_to_predicted: the population is the window's"
        ):
            _request(restrict_gt_to_predicted=True)

    def test_a_request_that_turned_it_off_asked_for_the_window_and_still_runs(self) -> None:
        # The benchmark's documented call passed false: that is the window's
        # population, so it is accepted and dropped rather than refused.
        assert (
            "restrict_gt_to_predicted" not in _request(restrict_gt_to_predicted=False).model_dump()
        )

    def test_the_field_is_gone_from_the_request(self) -> None:
        assert "restrict_gt_to_predicted" not in RunCafaEvaluationPayload.model_fields


class TestTheResultRecordsItsPopulation:
    def test_the_record_names_the_rule_and_the_targets_of_each_category(self) -> None:
        data = EvaluationData(
            nk={"P1": {"GO:0000001"}},
            lk={"P2": {"GO:0000002"}, "P4": {"GO:0000002"}},
            pk={"P3": {"GO:0000003"}},
            known={},
            pk_known={},
        )
        assert population_record(data) == {
            "rule": POPULATION_RULE,
            "targets": {"NK": 1, "LK": 2, "PK": 1},
        }

    def test_the_rule_is_lafas(self) -> None:
        assert POPULATION_RULE == "all_targets"


@pytest.mark.integration
class TestTheSealReadsThePopulation:
    """The seal's material reads the population the result records, and the request's before."""

    @pytest.fixture()
    def pg(self, postgres_url: str):
        engine = create_engine(postgres_url, future=True)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        yield engine
        Base.metadata.drop_all(engine)
        engine.dispose()

    def _restriction_sealed(
        self, engine, *, recorded: str | None, requested: bool | None
    ) -> str | None:
        from protea.core.operations.seal_evaluation_frames import _ONE_ROW

        job_id, result_id = uuid.uuid4(), uuid.uuid4()
        payload = {} if requested is None else {"restrict_gt_to_predicted": requested}
        results = {} if recorded is None else {"population": {"rule": recorded}}
        with Session(engine, future=True) as session:
            session.execute(text("SET session_replication_role = 'replica'"))
            session.execute(
                text(
                    "INSERT INTO job (id, operation, queue_name, status, payload, meta, tags) "
                    "VALUES (:j, 'run_cafa_evaluation', 'protea.evaluations', 'SUCCEEDED', "
                    "CAST(:p AS jsonb), '{}'::jsonb, '{}')"
                ),
                {"j": job_id, "p": json.dumps(payload)},
            )
            session.execute(
                text(
                    "INSERT INTO evaluation_result (id, evaluation_set_id, prediction_set_id, job_id, "
                    "results) VALUES (:r, :e, :s, :j, CAST(:x AS jsonb))"
                ),
                {
                    "r": result_id,
                    "e": uuid.uuid4(),
                    "s": uuid.uuid4(),
                    "j": job_id,
                    "x": json.dumps(results),
                },
            )
            row = session.execute(_ONE_ROW, {"id": str(result_id)}).mappings().one()
            return row["restrict_gt_to_predicted"]

    def test_a_result_over_every_target_seals_as_unrestricted(self, pg) -> None:
        assert self._restriction_sealed(pg, recorded="all_targets", requested=None) == "false"

    def test_it_seals_alike_with_a_request_from_before_that_turned_it_off(self, pg) -> None:
        assert self._restriction_sealed(pg, recorded=None, requested=False) == "false"

    def test_a_result_from_before_is_sealed_by_its_request(self, pg) -> None:
        assert self._restriction_sealed(pg, recorded=None, requested=True) == "true"
        assert self._restriction_sealed(pg, recorded=None, requested=None) is None
