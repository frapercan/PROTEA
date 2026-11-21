"""A prediction set says who it was asked about and what wrote it, so it can be compared.

A set asked about a query set names it in ``query_set_id``. A set handed a list
(a window's arms are handed its targets) records the list's size and digest in
``meta.query_population``, and the method seal reads either one as the population
declared. A native method's set records the code and libraries that wrote it, as
a retrieval's receipt does, and every key it carries is classified, so the seal
can compare it with anything.
"""

from __future__ import annotations

import hashlib
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from protea.core import method_write
from protea.core.method_seal import CrossedMethodAxes, assert_one_axis, method_identity
from protea.core.operations.predict_go_terms._receipt import run_receipt
from protea.core.query_population import population_of
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from tests.test_a_comparison_declares_its_axis import _a_clean_bank_contrast
from tests.test_prediction_run_receipt import _payload

#: Out of order, with a repeat, and with the isoform whose order a locale's
#: collation changes: ``-`` sorts before ``1`` by code point and is ignored by
#: en_US, which puts P123451 first.
ASKED = ["Q9Y6K9", "P12345-2", "A0A024R161", "P12345-2", "P123451"]


def whole_row(row: PredictionSet) -> dict:
    """The row as ``SELECT *`` returns it, which is what the seal reads."""
    return {a.columns[0].name: getattr(row, a.key) for a in PredictionSet.__mapper__.column_attrs}


class TestThePopulation:
    def test_it_is_the_digest_of_the_sorted_list_as_a_file(self) -> None:
        body = b"A0A024R161\nP12345-2\nP123451\nQ9Y6K9\n"
        assert population_of(ASKED) == {"proteins": 4, "sha256": hashlib.sha256(body).hexdigest()}

    def test_order_and_repeats_do_not_move_it(self) -> None:
        assert population_of(reversed(ASKED)) == population_of(sorted(set(ASKED)))

    def test_a_retrieval_handed_a_list_records_it(self) -> None:
        receipt = run_receipt(_payload(query_accessions=ASKED), uuid.uuid4())
        assert receipt["query_population"] == population_of(ASKED)

    def test_a_retrieval_over_a_query_set_names_that_instead(self) -> None:
        receipt = run_receipt(_payload(query_set_id=str(uuid.uuid4())), uuid.uuid4())
        assert "query_population" not in receipt


def _handed(row: dict, asked: list[str]) -> dict:
    row["query_set_id"] = None
    row["meta"] = {**row["meta"], "query_population": population_of(asked)}
    return row


class TestTheSealReadsTheDigest:
    def test_one_list_on_both_sides_is_one_population(self) -> None:
        a, b = (_handed(row, ASKED) for row in _a_clean_bank_contrast())
        assert_one_axis(["annotation_set_id"], a, b)

    def test_two_lists_are_a_difference_the_comparison_must_name(self) -> None:
        a, b = _a_clean_bank_contrast()
        a, b = _handed(a, ASKED), _handed(b, ASKED[:2])
        with pytest.raises(CrossedMethodAxes, match="query_population"):
            assert_one_axis(["annotation_set_id"], a, b)
        assert_one_axis(["annotation_set_id", "query_population"], a, b)

    def test_neither_a_query_set_nor_a_digest_is_still_refused(self) -> None:
        a, b = _a_clean_bank_contrast()
        a["query_set_id"] = None
        with pytest.raises(CrossedMethodAxes, match="does not record") as refused:
            assert_one_axis(["annotation_set_id"], a, b)
        assert "query_population" in str(refused.value)


def test_a_native_method_set_records_what_wrote_it_and_whom_it_asked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(method_write, "code_revision", lambda: "a" * 40)
    monkeypatch.setattr(
        method_write, "dependency_revisions", lambda: {"protea-contracts": "b" * 40}
    )
    method = SimpleNamespace(id=uuid.uuid4(), short_name="own-annotations-test-211")
    context = method_write.SetContext(
        annotation_set_id=uuid.uuid4(),
        ontology_snapshot_id=uuid.uuid4(),
        limit_per_entry=3,
        description="a test set",
        # A method's own keys cannot stand in for the writer's revision.
        meta={"evaluation_set_id": str(uuid.uuid4()), "code_revision": "not the writer's"},
        population=population_of(ASKED),
    )
    row = method_write.new_prediction_set(MagicMock(), method, context)
    assert row.meta["code_revision"] == "a" * 40
    assert row.meta["dependency_revisions"] == {"protea-contracts": "b" * 40}
    assert row.meta["query_population"] == population_of(ASKED)
    identity = method_identity(whole_row(row))
    assert identity["query_population"] == population_of(ASKED)
    assert identity["evaluation_set_id"] == context.meta["evaluation_set_id"]
