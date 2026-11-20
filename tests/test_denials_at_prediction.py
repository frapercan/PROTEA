"""A method honours the bank's denials when it predicts, and its record says so.

The closure's question 6 (2026-11-20): the evaluator scores what a method
stored and nothing else, so PROTEA's own transfer methods, the
k-nearest-neighbour transfer and the InterPro mirror, drop what the bank denies
before they write, with the policy of :mod:`protea.core.denials`. These pin the
policy's entry points, the last step of the k-nearest-neighbour pipeline, the
InterPro mirror's counters and record, the receipt and the method's identity,
and against Postgres that a writer drops exactly the pairs the evaluator's own
test (``test_a_prediction_does_not_contradict_a_curated_not.py``) drops.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from protea.core import denials
from protea.core.method_seal import METHOD_IDENTITY_FIELDS
from protea.core.operations.predict_go_terms import _post_knn_pipeline
from protea.core.operations.predict_go_terms_from_interpro import (
    PredictGOTermsFromInterProOperation,
    _ResolvedIds,
    _RunContext,
    _RunCounters,
)
from protea.infrastructure.orm.base import Base
from tests.test_a_prediction_does_not_contradict_a_curated_not import (
    _BANK,
    _CHILD,
    _GO,
    _SNAP,
    _UNRELATED,
    _seed,
)
from tests.test_prediction_run_receipt import _payload


class _Queue:
    """A session that answers each read with the next list of rows, in order."""

    def __init__(self, *answers: list[tuple[Any, ...]]) -> None:
        self.answers = list(answers)
        self.reads = 0

    def execute(self, _statement: Any, _params: dict[str, Any]) -> Any:
        self.reads += 1
        rows = self.answers.pop(0)
        result = MagicMock()
        result.all.return_value = rows
        result.__iter__.return_value = iter(rows)
        return result


class TestThePolicy:
    def test_the_denied_go_and_nothing_else(self) -> None:
        rows = [
            {"protein_accession": "P1", "go_id": "GO:2"},
            {"protein_accession": "P1", "go_id": "GO:3"},
            {"protein_accession": "P2", "go_id": "GO:2"},
        ]
        kept, removed = denials.drop_denied(rows, {("P1", "GO:2")}, lambda r: r["go_id"])
        # A denial is about one protein, not about the term.
        assert kept == rows[1:] and removed == 1

    def test_a_row_whose_term_cannot_be_read_is_kept(self) -> None:
        rows = [{"protein_accession": "P1", "go_term_id": 9}]
        assert denials.drop_denied(rows, {("P1", "GO:2")}, lambda _r: None) == (rows, 0)

    def test_term_ids_are_read_back_to_go_ids_in_one_query(self) -> None:
        session = _Queue([("P1", "GO:2")], [(2, "GO:2"), (3, "GO:3")])
        rows = [
            {"protein_accession": "P1", "go_term_id": 2},
            {"protein_accession": "P1", "go_term_id": 3},
            {"protein_accession": "P2", "go_term_id": 2},
        ]
        kept, removed = denials.honour_denials(session, uuid.uuid4(), uuid.uuid4(), rows)  # type: ignore[arg-type]
        assert kept == rows[1:] and removed == 1
        assert session.reads == 2

    def test_nothing_denied_touches_nothing_and_reads_no_term(self) -> None:
        session = _Queue([])
        rows = [{"protein_accession": "P1", "go_term_id": 2}]
        assert denials.honour_denials(session, uuid.uuid4(), uuid.uuid4(), rows) == (rows, 0)  # type: ignore[arg-type]
        assert session.reads == 1

    def test_no_rows_read_nothing(self) -> None:
        session = _Queue()
        assert denials.honour_denials(session, uuid.uuid4(), uuid.uuid4(), []) == ([], 0)  # type: ignore[arg-type]
        assert session.reads == 0

    def test_the_policy_is_the_one_written_on_the_record(self) -> None:
        assert denials.POLICY["max_depth"] == denials.MAX_DEPTH == 12
        assert denials.DENIAL_RELATIONS == ("is_a", "part_of")


class TestTheNearestNeighbourMethod:
    def test_its_last_step_drops_and_reports_how_many(self) -> None:
        ctx = SimpleNamespace(
            annotation_set_id=uuid.uuid4(),
            p=SimpleNamespace(ontology_snapshot_id=str(uuid.uuid4())),
        )
        rows = [
            {"protein_accession": "P1", "go_term_id": 2},
            {"protein_accession": "P1", "go_term_id": 3},
        ]
        emit = MagicMock()
        with patch.object(denials, "honour_denials", return_value=(rows[1:], 1)) as honour:
            kept = _post_knn_pipeline._honour_denials(MagicMock(), ctx, rows, emit)  # type: ignore[arg-type]
        assert kept == rows[1:]
        assert honour.call_args.args[1] == ctx.annotation_set_id
        assert emit.call_args.args[0] == "predict_go_terms_batch.denials"
        assert emit.call_args.args[2] == {"candidates": 2, "removed": 1}

    def test_every_receipt_says_the_denials_were_honoured_and_how(self) -> None:
        from protea.core.operations.predict_go_terms._receipt import run_receipt

        receipt = run_receipt(_payload(), uuid.uuid4())
        assert receipt["denials"] == {"honoured": True, **denials.POLICY}

    def test_the_denials_are_part_of_the_method(self) -> None:
        # Two runs that honour denials differently are two methods; a comparison
        # has to declare it as its axis.
        assert "denials" in METHOD_IDENTITY_FIELDS


class TestTheInterProMirror:
    def _ctx(self) -> _RunContext:
        ids = _ResolvedIds(uuid.uuid4(), uuid.uuid4(), uuid.uuid4())
        return _RunContext(
            p=SimpleNamespace(chunk_size=100),  # type: ignore[arg-type]
            ids=ids,
            accessions=["P1"],
            prediction_set_id=uuid.uuid4(),
        )

    def test_it_drops_before_the_insert_and_counts(self) -> None:
        op = PredictGOTermsFromInterProOperation()
        ctx = self._ctx()
        rows = [
            {"protein_accession": "P1", "go_term_id": 2},
            {"protein_accession": "P1", "go_term_id": 3},
        ]
        with (
            patch.object(op, "_build_prediction_rows", return_value=iter(rows)),
            patch.object(denials, "honour_denials", return_value=(rows[1:], 1)) as honour,
            patch.object(
                op, "_bulk_insert", side_effect=lambda _s, chunk, _n: len(chunk)
            ) as insert,
        ):
            counters = op._run_pipeline(MagicMock(), ctx)
        assert honour.call_args.args[1:3] == (
            ctx.ids.annotation_set_id,
            ctx.ids.ontology_snapshot_id,
        )
        assert insert.call_args.args[1] == rows[1:]
        assert (counters.denied_removed, counters.predictions_inserted) == (1, 1)

    def test_its_record_says_the_denials_were_honoured(self) -> None:
        prediction_set = SimpleNamespace(meta=None)
        p = SimpleNamespace(source_version="interpro2go-1", ipr_release_floor=None)
        counters = _RunCounters(denied_removed=4)
        PredictGOTermsFromInterProOperation._stamp_prediction_set_meta(
            MagicMock(), prediction_set, p, counters
        )  # type: ignore[arg-type]
        assert prediction_set.meta["denials"] == {"honoured": True, **denials.POLICY, "removed": 4}


@pytest.mark.integration
def test_a_writer_drops_exactly_what_the_evaluator_dropped(postgres_url: str) -> None:
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    rows = [
        {"protein_accession": "O94526", "go_term_id": _CHILD},
        {"protein_accession": "O94526", "go_term_id": _UNRELATED},
        {"protein_accession": "P00000", "go_term_id": _CHILD},
    ]
    with Session(engine, future=True) as session:
        _seed(session)
        kept, removed = denials.honour_denials(session, _BANK, _SNAP, rows)
    # The parent is denied, so its child is; the unrelated term and the other
    # protein are the positive control.
    assert removed == 1
    assert kept == rows[1:]
    assert _GO[_CHILD] == "GO:0000102"
    engine.dispose()
