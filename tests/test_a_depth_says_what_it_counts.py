"""A depth names its unit, and is refused when the candidates cannot answer.

The failure this stands against is quiet. ``sequence_rank`` is null on
every row retrieved before the column existed, ``NULL <= 2`` is null, and
null is not true, so a sequence-depth cut against those rows selects no
rows at all. The evaluation then runs on an empty candidate set, writes
its metrics and reports success.

The verdict and the query that feeds it are tested apart, because they
fail in different ways. A verdict tested through a fake query proves
nothing about the query, and a query nothing reads returns a zero that
means "my lookup missed" rather than "there is nothing there". Both
halves have been wrong in this repository within the week.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from protea.core.operations._depth_unit_guard import (
    DepthUnitUnavailable,
    assert_depth_unit_is_available,
    ledger_coverage,
    why_the_unit_is_unavailable,
)
from protea.core.operations._pred_base_cache import _cache_paths
from protea.core.operations.run_cafa_evaluation import RunCafaEvaluationPayload
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction

_SET = uuid.UUID("11111111-1111-1111-1111-111111111111")
_OTHER = uuid.UUID("22222222-2222-2222-2222-222222222222")


class TestAPayloadNamesOneUnit:
    def test_naming_both_is_refused(self) -> None:
        """Two candidate sets in one run has no reading, so it is not resolved."""
        with pytest.raises(ValueError, match="proteins or in sequences"):
            RunCafaEvaluationPayload.model_validate({
                "evaluation_set_id": str(_SET),
                "prediction_set_id": str(_SET),
                "max_k_position": 3,
                "max_sequence_rank": 3,
            })

    def test_naming_neither_is_the_whole_neighbourhood(self) -> None:
        got = RunCafaEvaluationPayload.model_validate({
            "evaluation_set_id": str(_SET), "prediction_set_id": str(_SET),
        })
        assert got.max_k_position is None
        assert got.max_sequence_rank is None

    def test_either_one_alone_is_accepted(self) -> None:
        base = {"evaluation_set_id": str(_SET), "prediction_set_id": str(_SET)}
        assert RunCafaEvaluationPayload.model_validate(
            {**base, "max_sequence_rank": 3}
        ).max_sequence_rank == 3
        assert RunCafaEvaluationPayload.model_validate(
            {**base, "max_k_position": 3}
        ).max_k_position == 3


class TestTheVerdict:
    def test_a_set_with_no_ranks_at_all_would_score_nothing(self) -> None:
        said = why_the_unit_is_unavailable(3, 0, 2, _SET)
        assert said is not None
        assert "nothing at all" in said

    def test_a_partly_ranked_set_is_refused_just_as_firmly(self) -> None:
        """The surviving half would look like a complete answer."""
        said = why_the_unit_is_unavailable(3, 2, 2, _SET)
        assert said is not None
        assert "only the ranked part" in said
        assert "2 of its 3" in said

    def test_a_fully_ranked_set_has_no_complaint(self) -> None:
        assert why_the_unit_is_unavailable(3, 3, 2, _SET) is None

    def test_an_empty_set_is_refused_rather_than_scored(self) -> None:
        said = why_the_unit_is_unavailable(0, 0, 2, _SET)
        assert said is not None
        assert "no candidates" in said

    def test_the_complaint_says_what_to_do_about_it(self) -> None:
        """A refusal with no next step gets worked around rather than fixed."""
        said = why_the_unit_is_unavailable(3, 0, 2, _SET)
        assert said is not None
        assert "Re-retrieve the set, or count this depth in proteins" in said


class TestTheGuardWiresTheVerdictToTheStore:
    @pytest.mark.integration
    def test_it_reads_coverage_off_the_real_table(self, postgres_url: str) -> None:
        """COUNT of a nullable column counts the non-nulls, which is the whole
        mechanism. Asserted against Postgres rather than assumed."""
        engine = create_engine(postgres_url, future=True)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        with Session(engine, future=True) as session:
            # The foreign keys point at tables this test does not populate.
            # The guard counts rows; it does not traverse them.
            session.execute(text("SET session_replication_role = 'replica'"))
            for i in range(3):
                session.add(
                    GOPrediction(
                        prediction_set_id=_SET,
                        protein_accession=f"Q{i}",
                        ref_protein_accession=f"R{i}",
                        go_term_id=i + 1,
                        distance=0.1,
                        k_position=i + 1,
                        sequence_rank=1 if i == 0 else None,
                    )
                )
            session.flush()

            # COUNT of a nullable column counts the non-nulls: three rows,
            # one sequence rank between them, no ledger at all.
            assert ledger_coverage(session, _SET) == (3, 1, 0)
            assert ledger_coverage(session, _OTHER) == (0, 0, 0)

            # No ledger stops the cut before the unit is even considered,
            # which is the right order: a set that cannot be recounted
            # cannot be cut in either unit.
            with pytest.raises(DepthUnitUnavailable, match="donor ledger on 0 of its 3"):
                assert_depth_unit_is_available(session, _SET, max_sequence_rank=2)
            with pytest.raises(DepthUnitUnavailable, match="donor ledger on 0 of its 3"):
                assert_depth_unit_is_available(
                    session, _SET, max_sequence_rank=None, max_k_position=2
                )
            # No depth at all asks nothing of the set, so nothing is refused.
            assert_depth_unit_is_available(session, _SET, max_sequence_rank=None)

            # With the ledger present the unit check is what remains, and it
            # sees a set ranked on one row of three.
            for row in session.query(GOPrediction).all():
                row.donor_count = 1
            session.flush()
            with pytest.raises(DepthUnitUnavailable, match="only the ranked part"):
                assert_depth_unit_is_available(session, _SET, max_sequence_rank=2)
            # A protein depth needs no sequence rank, so it passes.
            assert_depth_unit_is_available(
                session, _SET, max_sequence_rank=None, max_k_position=2
            )


class TestTwoUnitsAreTwoCandidateSets:
    def test_the_same_number_in_each_unit_gets_its_own_cache_entry(self) -> None:
        """Sharing one would serve one arm's parquet to the other."""
        by_protein = _cache_paths(_SET, None, 3, ["P1"])[0].name
        by_sequence = _cache_paths(_SET, None, None, ["P1"], 3)[0].name
        assert by_protein != by_sequence

    def test_two_sequence_depths_get_their_own_entries(self) -> None:
        assert (
            _cache_paths(_SET, None, None, ["P1"], 2)[0].name
            != _cache_paths(_SET, None, None, ["P1"], 3)[0].name
        )

    def test_the_protein_key_is_unchanged_so_cached_arms_still_hit(self) -> None:
        """A new field must not invalidate every parquet already on disk."""
        assert "__snone__" in _cache_paths(_SET, None, 3, ["P1"])[0].name
