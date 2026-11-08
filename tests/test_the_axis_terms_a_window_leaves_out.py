"""``window_terms_out``: the axis terms a window's rule leaves out.

WHY THE TABLE EXISTS. The laboratory's loader draws negatives from the term
axis. Without this table it draws them from the WHOLE axis, so a term created
after an early window appears as a negative there and the model learns that new
terms are never gained -- an artefact of the rule, not biology.

WHY IT IS ``axis - TOI`` AND NOT "THE COMPLEMENT". Measured on 2026-11-08 over
the 64 windows of revision ``d5416224``, one axis of 43.558 live terms:
``axis - TOI`` is 207 to 4.087 per window (106.010 rows in all), while
``TOI - axis`` is 0 to 3.286 -- and in **42 of the 64** the TOI holds MORE terms
than the axis does. A term can be live in both ENDS and obsolete in the pivot's
era. So ``|TOI| + |out| == |axis|`` is false for most windows, and the counter
for the off-axis part is emitted rather than left to be inferred.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from protea.core.operations.export_transitions_dataset import (
    IMPLEMENTED,
    TABLES,
    ExportTransitionsDatasetOperation,
    ExportTransitionsDatasetPayload,
    _refuse_unstamped,
    _window_or_refuse,
)
from protea.core.transitions_export import _window_terms_out as W

SHA = "d54162248eba37ee9d5e4c6fb961f749763f257e"
OTRA = "beafbd4f2e4141d59fa89e7587ab74355191b955"


# ------------------------------------------------------- la regla no se reescribe


def test_the_toi_rule_is_the_evaluation_s_own_function():
    """Imported, not reimplemented, and this is what keeps it that way.

    A second spelling of the rule is this project's recurring defect: the
    exported negatives would be about a slightly different TOI from the one the
    gain was trimmed by, and nothing in either artefact would say so.
    """
    from protea.core import _truth_rules

    assert W._terms_of_interest is _truth_rules._terms_of_interest


# ------------------------------------------------------------------ la aritmetica


class _Eset:
    def __init__(self) -> None:
        self.id = uuid.uuid4()
        self.old_annotation_set_id = uuid.uuid4()
        self.new_annotation_set_id = uuid.uuid4()
        self.pivot_snapshot_id = uuid.uuid4()
        self.old_native_snapshot_id = self.pivot_snapshot_id
        self.new_native_snapshot_id = self.pivot_snapshot_id
        self.truth_profile = "projected"
        self.toi_rule = "live_in_both_ends"
        self.not_codes = "experimental"
        self.code_revision = SHA


def _facts(eset=None, data=None, version_t="200", version_t_prime="201", dias=30):
    from protea.core.transitions_export._window_facts import WindowFacts

    e = eset or _Eset()
    return WindowFacts(
        eset=e,
        window=str(e.id),
        data=data if data is not None else _Data(set()),
        version_t=version_t,
        version_t_prime=version_t_prime,
        horizon_days=dias,
    )


class _Data:
    """The three scored maps, which is all any table reads from the truth."""

    def __init__(self, terms, proteinas=("P1", "P2")) -> None:
        self.nk = {p: set(terms) for p in proteinas}
        self.lk: dict[str, set[str]] = {}
        self.pk: dict[str, set[str]] = {}


def test_the_counters_separate_the_two_directions(monkeypatch):
    """The off-axis part of the TOI is reported, not inferred.

    ``axis - TOI`` is the table; ``TOI - axis`` is terms the loader never sees,
    because they are not on the axis it draws from. Both are counted because a
    reader given only two of the three numbers would compute the third wrongly.
    """
    eje = {"GO:1", "GO:2", "GO:3", "GO:4"}
    toi = {"GO:3", "GO:4", "GO:9", "GO:8", "GO:7"}  # tres fuera del eje
    monkeypatch.setattr(W, "_terms_of_interest", lambda _s, _w: toi)
    monkeypatch.setattr(W, "_refuse_gains_outside_toi", lambda _f, _o: 7)

    r = W.terms_out(object(), _facts(), eje)

    assert r.out == ["GO:1", "GO:2"]
    assert r.axis_size == 4
    assert r.toi_size == 5
    assert r.toi_off_axis == 3
    assert r.restricts is True
    assert r.gain_terms == 7
    # Y el caso que motiva el contador: el TOI es MAYOR que el eje.
    assert r.toi_size > r.axis_size


def test_an_axis_with_no_live_terms_is_refused(monkeypatch):
    """Otherwise every window exports an empty complement.

    Which reads as "the loader may use the whole axis as negatives": a table
    that is complete, consistent and about nothing.

    The question moved to ``axis_of``, which answers it once for the three
    tables that need the axis. ``axis_live_terms`` used to ask it again here
    and was a second query for the same answer.
    """
    from protea.core.transitions_export import _terms as T

    monkeypatch.setattr(T, "_live_terms_with_aspect", lambda _s, _c: (set(), {}))

    with pytest.raises(ValueError, match="no live terms"):
        T.axis_of(object(), uuid.uuid4())


# --------------------------------------------------------------- los mismos bytes


def _wt(window: str, out: list[str]):
    return W.WindowTerms(
        window=window,
        out=out,
        axis_size=10,
        toi_size=8,
        toi_off_axis=0,
        restricts=True,
        gain_terms=5,
    )


def test_the_same_windows_give_the_same_bytes_whatever_order_they_arrive(tmp_path: Path):
    """The sha of this file is what a consumer quotes.

    A digest over whatever order the query plan happened to pick is a digest
    that moves without the data moving, which is a measured hazard here and not
    a worry: ties at the k-th neighbour already move a metric by 1e-3 for
    exactly this reason.
    """
    a, b = _wt("w2", ["GO:2", "GO:1"]), _wt("w1", ["GO:3"])

    uno, otro = tmp_path / "uno.parquet", tmp_path / "otro.parquet"
    assert W.write_table([a, b], str(uno)) == 3
    assert W.write_table([b, a], str(otro)) == 3

    assert hashlib.sha256(uno.read_bytes()).hexdigest() == (
        hashlib.sha256(otro.read_bytes()).hexdigest()
    )
    tabla = pq.read_table(uno).to_pydict()
    assert tabla["window"] == ["w1", "w2", "w2"]
    assert tabla["go_id"] == ["GO:3", "GO:1", "GO:2"]


# ------------------------------------------------------------------ las negativas


class TestWhatTheOperationRefuses:
    def test_a_table_declared_by_the_adr_but_not_written_yet(self):
        """Refused and not skipped, and the message says which of the two it is."""
        with pytest.raises(ValueError, match="does not write them yet"):
            # ``gains`` landed, so the example moved to one of the four that
            # have not: a test whose subject gets implemented has to move with
            # it or it stops testing the refusal.
            ExportTransitionsDatasetPayload(
                windows=["w"], cut="c", cut_version="211", tables=["query_state"]
            )

    def test_a_table_no_adr_declares(self):
        with pytest.raises(ValueError, match="unknown tables"):
            ExportTransitionsDatasetPayload(
                windows=["w"], cut="c", cut_version="211", tables=["inventada"]
            )

    def test_a_repeated_window(self):
        """Exported twice its terms are written twice and the count stops counting."""
        with pytest.raises(ValueError, match="repeats"):
            ExportTransitionsDatasetPayload(windows=["w", "w"], cut="c", cut_version="211")

    def test_a_window_that_is_not_an_id(self):
        with pytest.raises(ValueError, match="named by id"):
            _window_or_refuse(object(), "211-227")

    def test_an_unstamped_window_is_refused_and_not_passed(self):
        """``revisions_conflict`` says False when either side is unidentifying.

        That is NOT agreement, so the unstamped case is refused on its own
        rather than falling through the comparison.
        """
        eset = _Eset()
        eset.code_revision = None

        with pytest.raises(ValueError, match="cannot say what built it"):
            _refuse_unstamped(eset, [], SHA)

    def test_a_window_built_by_another_revision_is_refused(self):
        with pytest.raises(ValueError, match="accept_revisions"):
            _refuse_unstamped(_Eset(), [], OTRA)

    def test_naming_the_revision_admits_it_and_a_boolean_would_not(self):
        """Admitting in bulk would be the same as not checking."""
        _refuse_unstamped(_Eset(), [SHA], OTRA)
        _refuse_unstamped(_Eset(), [SHA[:12]], OTRA)

        assert not hasattr(ExportTransitionsDatasetPayload, "accept_any_revision")


def test_the_ten_tables_are_declared_and_six_are_written():
    """The gap between the two lists is the honest state of this operation.

    Hard-coded on purpose: a table landing has to come with a line here, so
    the list cannot drift into claiming more than is written. The four left
    are ``query_state``, ``neighbourhood``, ``neighbour_state`` and
    ``feature_tokens``.
    """
    assert len(TABLES) == 10
    assert IMPLEMENTED == {
        "window_terms_out",
        "queries",
        "gains",
        "terms",
        "term_ancestors",
        "residue_refs",
    }
    assert IMPLEMENTED <= set(TABLES)
    assert set(TABLES) - IMPLEMENTED == {
        "query_state",
        "neighbourhood",
        "neighbour_state",
        "feature_tokens",
    }


def test_the_operation_says_it_writes_no_dataset_row():
    """A dataset row over a partial bundle is a row claiming to be a dataset."""
    op = ExportTransitionsDatasetOperation()

    assert "writes no dataset row" in op.description.lower()
    assert "by id" in op.description


# ------------------------------------------- la regla que no restringe, y el cruce


def test_a_rule_that_does_not_trim_gets_an_empty_table(monkeypatch):
    """``pivot_terms`` does not restrict the gain, so nothing is excluded.

    AND IT IS THE DEFAULT RULE, which is what makes this worth a test.
    ``_terms_of_interest`` ignores ``toi_rule`` and always intersects the two
    ends, so writing that intersection for such a window would remove terms
    from the loader's negatives that its gain never excluded. The 64 windows
    of this campaign are all ``live_in_both_ends``, so nothing in the data
    would have shown it. The desktop session asked the question.
    """
    eset = _Eset()
    eset.toi_rule = "pivot_terms"
    monkeypatch.setattr(W, "_terms_of_interest", lambda _s, _w: {"GO:3"})
    monkeypatch.setattr(W, "_refuse_gains_outside_toi", lambda _f, _o: 0)

    r = W.terms_out(object(), _facts(eset=eset), {"GO:1", "GO:2", "GO:3"})

    assert r.out == []
    assert r.restricts is False
    # The counts are still reported, so a reader can see the rule did nothing
    # rather than conclude the intersection covered the axis.
    assert r.axis_size == 3
    assert r.toi_size == 1


def test_the_condition_lives_in_one_place():
    """The export and the evaluation ask the SAME function.

    ``_terms_of_interest`` does not look at the rule, and ``pivot_terms`` is
    the default, so a caller that computed the intersection and forgot the
    condition would restrict a gain that was never restricted. Two spellings
    of that condition is how that happens.
    """
    import inspect

    from protea.core import evaluation
    from protea.core._truth_rules import toi_restricts_the_gain

    assert W.toi_restricts_the_gain is toi_restricts_the_gain
    fuente = inspect.getsource(evaluation)
    assert "toi_restricts_the_gain(window.toi_rule)" in fuente
    assert 'window.toi_rule == "live_in_both_ends"' not in fuente


class TestTheCrossTableInvariant:
    """No gained term may be in its own window's ``window_terms_out``.

    A gain is ``new - old`` restricted to ``(new & TOI) | (new & old)``. A
    gained term is by definition not in ``old``, so it comes from
    ``new & TOI`` and lies inside the terms of interest. This is the only
    check that can see in the DATA what importing the same function cannot:
    if the two rules ever differ, the function identity test still passes.

    Measured on 2026-11-08 over four windows: 0 violations against 3.346 to
    5.241 gained terms each, and then over all 64 in one run: 64 of 64 with
    their gain read and zero violations.
    """

    def test_a_gain_inside_the_excluded_set_is_refused(self):
        with pytest.raises(ValueError, match="not the same rule"):
            W._refuse_gains_outside_toi(_facts(data=_Data({"GO:1", "GO:9"})), {"GO:9"})

    def test_a_clean_window_reports_its_gained_terms(self):
        assert W._refuse_gains_outside_toi(_facts(data=_Data({"GO:1", "GO:2"})), {"GO:9"}) == 2

    def test_it_reads_the_truth_from_the_facts_and_loads_nothing(self):
        """One read per window, which is why the facts object exists.

        Two loads mean two fetches of the same parquet, two digest
        verifications, and two chances for one bundle's tables to be about
        slightly different reads.
        """
        import inspect

        fuente = inspect.getsource(W._refuse_gains_outside_toi)

        assert "load_evaluation_data_for_set" not in fuente
        assert "facts.data" in fuente
