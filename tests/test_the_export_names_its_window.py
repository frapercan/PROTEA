"""A dump says which declared window its truth comes from, or refuses.

THE PAIR STOPPED BEING AN IDENTITY and the export had not caught up. The
three propagation graphs and the three truth rules are part of a window's
identity, so on this campaign's database **62 of 69 distinct pairs carry more
than one window**. A resolver keyed on the pair alone does not work on 90
percent of the series.

AND ONE OF THE TWO COPIES HAD ROTTED. The train path hit this on 2026-05-12,
the LB.1 incident, and was repaired to collect candidates and name them. The
test path's copy of the same question still called ``one_or_none()`` when
these tests were written, so it was one test pair away from raising
``MultipleResultsFound`` from deep inside SQLAlchemy. There is one copy of the
rule now, which is the only durable fix for that.
"""

from __future__ import annotations

import inspect
import uuid
from typing import Any

import pytest

from protea.core.training_dump._window_choice import (
    resolve_window,
    window_identity,
)

PIVOTE = uuid.uuid4()
OTRO_PIVOTE = uuid.uuid4()


class _Eset:
    """An ``EvaluationSet`` row, as the resolver reads one."""

    def __init__(self, pivot: uuid.UUID = PIVOTE, profile: str = "projected") -> None:
        self.id = uuid.uuid4()
        self.pivot_snapshot_id = pivot
        self.old_native_snapshot_id = pivot
        self.new_native_snapshot_id = pivot
        self.truth_profile = profile
        self.toi_rule = "live_in_both_ends"
        self.not_codes = "experimental"
        self.code_revision = None


class _Session:
    """Answers the one query the resolver makes, and asserts it is that one."""

    def __init__(self, candidates: list[_Eset]) -> None:
        self._candidates = candidates

    def query(self, model: Any) -> _Session:
        assert model.__name__ == "EvaluationSet", model
        return self

    def filter_by(self, **kwargs: Any) -> _Session:
        assert set(kwargs) == {"old_annotation_set_id", "new_annotation_set_id"}, kwargs
        return self

    def all(self) -> list[_Eset]:
        return self._candidates


def _resolve(candidates: list[_Eset], named: set[str], pivot: Any = PIVOTE) -> Any:
    return resolve_window(
        _Session(candidates), uuid.uuid4(), uuid.uuid4(), named, pivot, "train pair (204->205)"
    )


class TestWhenThePairIsEnoughAndWhenItIsNot:
    def test_one_window_and_no_names_resolves(self) -> None:
        only = _Eset()
        assert _resolve([only], set()).id == only.id

    def test_two_windows_and_no_names_refuses_and_names_them(self) -> None:
        a, b = _Eset(), _Eset(profile="as_closed")
        with pytest.raises(RuntimeError) as err:
            _resolve([a, b], set())
        # The refusal has to carry what differs, or the reader cannot choose.
        assert "2 EvaluationSets exist" in str(err.value)
        assert str(a.id) in str(err.value) and str(b.id) in str(err.value)
        assert "projected/live_in_both_ends/experimental" in str(err.value)
        assert "as_closed" in str(err.value)

    def test_naming_one_of_two_resolves_to_it(self) -> None:
        a, b = _Eset(), _Eset(profile="as_closed")
        assert _resolve([a, b], {str(b.id)}).id == b.id

    def test_naming_both_refuses_because_it_says_nothing(self) -> None:
        a, b = _Eset(), _Eset(profile="as_closed")
        with pytest.raises(RuntimeError, match="names 2 evaluation sets"):
            _resolve([a, b], {str(a.id), str(b.id)})

    def test_a_named_id_that_is_not_a_window_of_this_pair_refuses(self) -> None:
        # The typo case, and the "window from another series" case. Ignoring
        # the name and falling back to the single candidate would be the
        # silent wrong answer: the caller asked for something else.
        only = _Eset()
        with pytest.raises(RuntimeError, match="none of them is a window of"):
            _resolve([only], {str(uuid.uuid4())})

    def test_no_window_at_all_says_how_to_make_one(self) -> None:
        with pytest.raises(RuntimeError, match="Materialize it"):
            _resolve([], set())


class TestTheDeclaredAxisIsEnforced:
    def test_a_window_on_another_pivot_is_refused(self) -> None:
        # A window pivoting elsewhere expresses its truth on another term
        # axis, so a manifest declaring one axis over windows using another
        # is wrong about its own vocabulary, and the parquet does not say so.
        with pytest.raises(RuntimeError, match="expresses its truth on another term axis"):
            _resolve([_Eset(pivot=OTRO_PIVOTE)], set(), pivot=PIVOTE)

    def test_a_matching_pivot_passes(self) -> None:
        assert _resolve([_Eset(pivot=PIVOTE)], set(), pivot=PIVOTE) is not None

    def test_no_declared_axis_skips_the_check(self) -> None:
        # For a caller with no axis to declare. Not the export, which always
        # has one, but the check is the caller's to ask for.
        assert _resolve([_Eset(pivot=OTRO_PIVOTE)], set(), pivot=None) is not None


class TestWhatTheRecordHasToSay:
    def test_the_identity_carries_the_rules_the_pivot_and_the_revision(self) -> None:
        # The three rules say what the delta is by DECLARATION, the pivot says
        # which axis it is expressed on, and the revision says what the delta
        # is by IMPLEMENTATION. A dataset that recorded only its annotation
        # sets said nothing about any of the three.
        ident = window_identity(_Eset())
        assert set(ident) == {
            "evaluation_set_id",
            "pivot_snapshot_id",
            "old_native_snapshot_id",
            "new_native_snapshot_id",
            "truth_profile",
            "toi_rule",
            "not_codes",
            "code_revision",
        }
        assert ident["truth_profile"] == "projected"
        assert ident["code_revision"] is None

    def test_a_row_without_the_revision_column_still_reads(self) -> None:
        # The column lands in a separate migration, so the identity must not
        # require it: a dump run against a database one migration behind
        # should report NULL, not raise.

        class _Antigua:
            id = uuid.uuid4()
            pivot_snapshot_id = PIVOTE
            old_native_snapshot_id = PIVOTE
            new_native_snapshot_id = PIVOTE
            truth_profile = "as_closed"
            toi_rule = "pivot_terms"
            not_codes = "any"

        assert window_identity(_Antigua())["code_revision"] is None


def _code_of(obj: Any) -> str:
    """The source with its docstring removed.

    Searching the whole source searches the prose too, and the prose here
    NAMES the call it replaced. The first version of this test asserted that
    ``one_or_none`` was absent and failed on the docstring that explains why
    it is: a grep over documentation is not a check on behaviour.
    """
    source = inspect.getsource(obj)
    doc = inspect.getdoc(obj)
    if not doc:
        return source
    for line in doc.splitlines():
        source = source.replace(line, "")
    return source


class TestTheSeamsThatRotted:
    def test_neither_resolver_asks_one_or_none_any_more(self) -> None:
        # The LB.1 shape. ``one_or_none`` on the bare pair is the call that
        # raises from three frames down when the pair is not an identity.
        from protea.core import _training_dump_loaders as loaders
        from protea.core.training_dump import _train_split

        for fn in (loaders._resolve_test_eval_inputs, _train_split._resolve_train_split_eval):
            assert "one_or_none" not in _code_of(fn), fn.__name__
            assert "resolve_window" in _code_of(fn), fn.__name__

    def test_both_resolvers_pass_the_declared_axis_through(self) -> None:
        # A resolver that asked for the window and not for the axis would
        # leave the pivot check unreachable, which is how a guard stops being
        # one without anybody editing it.
        from protea.core import _training_dump_loaders as loaders
        from protea.core.training_dump import _runner, _train_split

        assert "ontology_snapshot_id" in _code_of(_train_split._resolve_train_split_eval)
        assert "pivot_must_be" in _code_of(loaders._resolve_test_eval_inputs)
        # ``_run_test_split`` lives on the runner, not on the operation class:
        # the first version of this test asserted on the wrong one and passed
        # nothing useful.
        assert "ontology_snapshot_id" in inspect.getsource(_runner._DumpRunner._run_test_split)

    def test_the_payload_can_name_them(self) -> None:
        from protea.core.training_dump._payload import TrainRerankerAutoPayload

        field = TrainRerankerAutoPayload.model_fields["evaluation_set_ids"]
        assert field.default_factory is not None or field.default == []
