"""Refusing a dispatch that selected a population without naming one.

Giving neither ``query_set_id`` nor ``query_accessions`` selects every protein
that has an embedding. That reads like a filter which matched everything and is
in fact a filter that was never applied. The two are indistinguishable from
outside: the job succeeds, every row it writes is correct, and only the population
is wrong.

On 2026-08-18 it ran twelve jobs over 616,846 proteins where 6,216 were intended.
Nothing failed, one run reached 37 minutes and 3.3 million discarded rows, and it
was caught by comparing two numbers reported an hour apart rather than by anything
the system said.

The submission path never had the hole: ``protea-predict`` requires
``--query_file``. These tests pin the internal path to the same stance.
"""

from __future__ import annotations

import pytest

from protea.config.tuning import get_tuning
from protea.core.operations.predict_go_terms._coordinator import (
    _refuse_an_undeclared_corpus,
)


@pytest.fixture(autouse=True)
def _fresh_tuning(monkeypatch):
    """Tuning is cached, so an env override needs the cache cleared to be seen."""
    get_tuning.cache_clear()
    yield
    get_tuning.cache_clear()


def _limit(monkeypatch, value: str) -> None:
    monkeypatch.setenv(
        "PROTEA_TUNING__operation__max_implicit_query_population", value
    )
    get_tuning.cache_clear()


def test_a_corpus_scale_undeclared_population_is_refused(monkeypatch):
    """The regression, at the real number that caused it."""
    _limit(monkeypatch, "50000")

    with pytest.raises(ValueError, match="named no query population"):
        _refuse_an_undeclared_corpus(616_846)


def test_the_refusal_names_both_counts(monkeypatch):
    """A caller has to see at a glance whether the number is the one they meant."""
    _limit(monkeypatch, "50000")

    with pytest.raises(ValueError) as excinfo:
        _refuse_an_undeclared_corpus(616_846)

    assert "616,846" in str(excinfo.value)
    assert "50,000" in str(excinfo.value)


def test_the_refusal_says_what_to_pass(monkeypatch):
    """A guard that only refuses turns into a guard someone disables."""
    _limit(monkeypatch, "50000")

    with pytest.raises(ValueError) as excinfo:
        _refuse_an_undeclared_corpus(616_846)

    message = str(excinfo.value)
    assert "query_accessions" in message
    assert "query_set_id" in message


def test_an_evaluation_sized_population_passes(monkeypatch):
    """6,216 is the real delta. The guard must not stand in the way of the work."""
    _limit(monkeypatch, "50000")

    _refuse_an_undeclared_corpus(6_216)


def test_exactly_the_limit_is_allowed(monkeypatch):
    """The boundary is inclusive, so a limit set to a known population works."""
    _limit(monkeypatch, "6216")

    _refuse_an_undeclared_corpus(6_216)


def test_one_above_the_limit_is_refused(monkeypatch):
    _limit(monkeypatch, "6216")

    with pytest.raises(ValueError):
        _refuse_an_undeclared_corpus(6_217)


def test_zero_disables_the_guard_entirely(monkeypatch):
    """An escape hatch that does not require editing code under time pressure."""
    _limit(monkeypatch, "0")

    _refuse_an_undeclared_corpus(10_000_000)


def test_the_default_sits_between_the_delta_and_the_corpus():
    """Calibration, stated as a test rather than left in a commit message.

    The guard is worthless if it fires on real work and worthless if it misses the
    case it exists for, so the default has to separate 6,216 from 616,846 with
    room on both sides.
    """
    default = get_tuning().operation.max_implicit_query_population

    assert 6_216 < default < 616_846
