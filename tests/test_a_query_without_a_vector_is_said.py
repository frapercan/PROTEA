"""A member of the query set with no vector is not scored, so it has to be said.

The population join is an INNER JOIN against ``sequence_embedding``, so a
declared query with no vector under this configuration is neither an error nor
a zero row: it is simply absent from the result, under the same
``query_set_id`` and with a receipt that reads identically to a complete run.

Measured 2026-10-28: 25 of the 40.531 targets of ``c92deab6`` sit on sequences
the compute node's card cannot embed, and they run from 8.784 to 34.350
residues. Dividing a paired sigma by sqrt(n) with an unchecked n is the defect
this fixes.
"""

from __future__ import annotations

import uuid

from protea.core.operations.predict_go_terms._missing_query_vectors import (
    MAX_LISTED,
    report_queries_without_vector,
)

CFG = uuid.uuid4()


def _spy():
    seen: list[tuple] = []
    return seen, lambda kind, _msg, fields, level: seen.append((kind, fields, level))


class TestTheOrdinaryPath:
    def test_a_complete_batch_says_nothing(self):
        """Every declared query has a vector, which is the normal case and must
        not add an event to every batch of every run."""
        def boom(*_a, **_k):
            raise AssertionError("a complete batch is not an event")

        assert report_queries_without_vector(
            ["A", "B"], ["A", "B"], embedding_config_id=CFG, emit=boom) == []

    def test_an_empty_request_says_nothing(self):
        def boom(*_a, **_k):
            raise AssertionError("nothing requested is nothing dropped")

        assert report_queries_without_vector(
            [], [], embedding_config_id=CFG, emit=boom) == []

    def test_extra_scored_accessions_do_not_invent_a_drop(self):
        """``scored`` is a superset in no legitimate case, but a set difference
        in the wrong direction would report a negative hole as a positive one."""
        def boom(*_a, **_k):
            raise AssertionError("more scored than requested is not a drop")

        assert report_queries_without_vector(
            ["A"], ["A", "B"], embedding_config_id=CFG, emit=boom) == []


class TestTheHole:
    def test_the_dropped_accessions_are_named_and_counted(self):
        seen, emit = _spy()
        dropped = report_queries_without_vector(
            ["A", "B", "C"], ["A", "C"], embedding_config_id=CFG, emit=emit)
        assert dropped == ["B"]
        (kind, fields, level), = seen
        assert kind == "predict_go_terms.queries_without_vector"
        assert level == "warning"
        assert (fields["requested"], fields["scored"], fields["dropped"]) == (3, 2, 1)
        assert fields["accessions"] == ["B"]
        assert fields["truncated"] is False
        assert fields["embedding_config_id"] == str(CFG)

    def test_it_is_a_warning_because_the_arithmetic_cannot_complain(self):
        """Every count downstream still adds up; what changed is which proteins
        they are about."""
        seen, emit = _spy()
        report_queries_without_vector(["A"], [], embedding_config_id=CFG, emit=emit)
        assert seen[0][2] == "warning"

    def test_the_order_follows_the_request_so_two_receipts_diff(self):
        seen, emit = _spy()
        report_queries_without_vector(
            ["D", "C", "B", "A"], ["C"], embedding_config_id=CFG, emit=emit)
        assert seen[0][1]["accessions"] == ["D", "B", "A"]

    def test_a_whole_batch_with_no_vectors_is_reported_not_silent(self):
        """The empty-rows path returns early, and it used to return before
        saying anything at all: a batch dispatched against a configuration with
        no vectors scored nothing and looked like a batch of zero queries."""
        seen, emit = _spy()
        dropped = report_queries_without_vector(
            ["A", "B"], [], embedding_config_id=CFG, emit=emit)
        assert dropped == ["A", "B"]
        assert seen[0][1]["scored"] == 0

    def test_the_reason_says_why_two_configs_are_not_comparable(self):
        seen, emit = _spy()
        report_queries_without_vector(["A"], [], embedding_config_id=CFG, emit=emit)
        assert "same query_set_id" in seen[0][1]["reason"]


class TestTheCap:
    def test_the_list_is_capped_but_the_count_is_not(self):
        """A batch against the wrong configuration drops every query it holds,
        and a receipt is not a place for a thousand ids. Losing the COUNT would
        defeat the point of the module."""
        requested = [f"A{i}" for i in range(MAX_LISTED + 50)]
        seen, emit = _spy()
        report_queries_without_vector(requested, [], embedding_config_id=CFG, emit=emit)
        fields = seen[0][1]
        assert fields["dropped"] == MAX_LISTED + 50
        assert len(fields["accessions"]) == MAX_LISTED
        assert fields["truncated"] is True

    def test_exactly_the_cap_is_not_truncated(self):
        requested = [f"A{i}" for i in range(MAX_LISTED)]
        seen, emit = _spy()
        report_queries_without_vector(requested, [], embedding_config_id=CFG, emit=emit)
        assert seen[0][1]["truncated"] is False
        assert len(seen[0][1]["accessions"]) == MAX_LISTED


class TestItIsWiredIn:
    def test_both_return_paths_of_the_loader_report(self):
        """Fixed by reading the source, because a loader that reports on one
        path and not the other is silent exactly when the hole is total."""
        import inspect

        from protea.core.operations.predict_go_terms import _batch_op_feature as mod

        src = inspect.getsource(mod._FeatureLoadingMixin._load_query_embeddings)
        assert src.count("report_queries_without_vector(") == 2
        # and the early return for an empty result is one of them
        head = src.split("valid_accessions = ")[0]
        assert "report_queries_without_vector(" in head
