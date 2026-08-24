"""Gating on the cohort restriction instead of only reporting it.

Dropping ground-truth proteins the prediction set never covered is correct CAFA
practice for a finished run and a trap for an unfinished one: it turns "we
predicted two thirds of the targets" into "we scored the cohort we predicted", so
a partially written prediction set yields a metric that is plausible, well formed,
and indistinguishable on the board from a complete one.

On 2026-08-18 two evaluations reached the board over 66 and 83 per cent of the
population. The before and after counts this gate reads were already being emitted
at the time. Nothing divided them, which is the project's own standing rule about
never narrating a check you do not act on, broken by the code rather than by a
person.
"""

from __future__ import annotations

import pytest

from protea.config.tuning import get_tuning
from protea.core.operations._run_cafa_data_helpers import (
    _refuse_a_restricted_cohort,
    restriction_fraction,
)


@pytest.fixture(autouse=True)
def _fresh_tuning():
    get_tuning.cache_clear()
    yield
    get_tuning.cache_clear()


def _limit(monkeypatch, value: str) -> None:
    monkeypatch.setenv("PROTEA_TUNING__operation__max_ground_truth_restriction", value)
    get_tuning.cache_clear()


def _check(before: int, after: int) -> None:
    _refuse_a_restricted_cohort(before, after, restriction_fraction(before, after))


# --------------------------------------------------------------------------- the fraction

def test_no_restriction_is_zero():
    assert restriction_fraction(6_216, 6_216) == 0.0


def test_a_two_thirds_cohort_reports_a_third_dropped():
    assert restriction_fraction(6_216, 4_096) == pytest.approx(0.3411, abs=1e-4)


def test_an_empty_cohort_does_not_divide_by_zero():
    """A ground truth with no proteins is a different fault and must not become
    a ZeroDivisionError inside the guard that exists to report faults."""
    assert restriction_fraction(0, 0) == 0.0


# --------------------------------------------------------------------------- the gate

def test_the_two_real_incidents_are_refused(monkeypatch):
    """The regression, at the populations that actually reached the board."""
    _limit(monkeypatch, "0.10")

    with pytest.raises(ValueError, match="dropped"):
        _check(6_216, 4_096)
    with pytest.raises(ValueError, match="dropped"):
        _check(6_216, 5_192)


def test_a_complete_cohort_passes(monkeypatch):
    _limit(monkeypatch, "0.10")

    _check(6_216, 6_216)


def test_a_small_legitimate_restriction_passes(monkeypatch):
    """Proteins the booster failed on, or excluded at query time, are normal."""
    _limit(monkeypatch, "0.10")

    _check(6_216, 6_000)


def test_the_message_names_both_counts_and_the_share(monkeypatch):
    """A reader must be able to tell an unfinished run from a deliberate cohort
    without opening the event log."""
    _limit(monkeypatch, "0.10")

    with pytest.raises(ValueError) as excinfo:
        _check(6_216, 4_096)

    message = str(excinfo.value)
    assert "6,216" in message
    assert "4,096" in message
    assert "34.1%" in message


def test_the_message_names_the_usual_cause_and_the_fix(monkeypatch):
    """A guard that only refuses becomes a guard someone disables."""
    _limit(monkeypatch, "0.10")

    with pytest.raises(ValueError) as excinfo:
        _check(6_216, 4_096)

    assert "SUCCEEDED" in str(excinfo.value)


def test_one_can_score_a_reduced_cohort_deliberately(monkeypatch):
    """The decision moves into the environment rather than an unexamined default."""
    _limit(monkeypatch, "1.0")

    _check(6_216, 1)


def test_the_boundary_is_inclusive(monkeypatch):
    _limit(monkeypatch, "0.5")

    _check(1_000, 500)


def test_just_past_the_boundary_is_refused(monkeypatch):
    _limit(monkeypatch, "0.5")

    with pytest.raises(ValueError):
        _check(1_000, 499)


def test_the_default_separates_the_incidents_from_normal_attrition():
    """Calibration stated as a test rather than left in a commit message."""
    default = get_tuning().operation.max_ground_truth_restriction

    assert restriction_fraction(6_216, 6_000) < default
    assert default < restriction_fraction(6_216, 5_192)


# --------------------------------------------------------------------------- the floor

def test_a_cohort_too_small_to_evaluate_is_not_gated(monkeypatch):
    """Calibration, kept as behaviour rather than as a story in a commit message.

    The guard first hit five existing tests whose mocked sessions return no
    predicted accessions at all, over ground truths of two proteins. Reading every
    hit is what the standing rule asks for, and what they say is that a cohort
    that small is a fixture or a degenerate case, not a measurement.
    """
    _limit(monkeypatch, "0.10")

    _check(2, 0)


def test_the_floor_does_not_excuse_a_real_cohort(monkeypatch):
    """The floor must not become a hole: at evaluation scale the gate still bites."""
    from protea.core.operations._run_cafa_data_helpers import MIN_GATED_COHORT

    _limit(monkeypatch, "0.10")

    with pytest.raises(ValueError):
        _check(MIN_GATED_COHORT, 0)


def test_the_floor_sits_far_below_the_real_population():
    """6,216 is the population these evaluations actually run over."""
    from protea.core.operations._run_cafa_data_helpers import MIN_GATED_COHORT

    assert MIN_GATED_COHORT < 6_216 / 100


def test_the_refusal_offers_the_safe_routes_before_the_global_one(monkeypatch):
    """An escape hatch that does not name its cost gets taken by default.

    The message used to end at "raise
    PROTEA_TUNING__operation__max_ground_truth_restriction", which reads as the
    remedy. It is process-wide: it lifts the gate for every evaluation the
    worker runs afterwards, including ones somebody else dispatched. Two local
    remedies exist and belong ahead of it.
    """
    _limit(monkeypatch, "0.10")

    with pytest.raises(ValueError) as excinfo:
        _check(23_828, 1_739)

    msg = str(excinfo.value)
    assert "ground truth built" in msg, "the local remedy must be offered"
    assert "stored predictions" in msg, "and so must the other one"
    assert "PROCESS-WIDE" in msg, "the global knob must state its scope"
    assert msg.index("ground truth built") < msg.index("PROTEA_TUNING__"), (
        "the safe routes come first, or the global one reads as the remedy"
    )
