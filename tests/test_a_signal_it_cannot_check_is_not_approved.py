"""A weighted signal with no mapped column is reported, not skipped.

WHY THIS TEST EXISTS. ``compute_missing_signals`` used to walk
``_SIGNAL_TO_COLUMN`` and ask, for each of its entries, whether the config
weighted it. A weighted signal absent from that map was therefore never asked
about. The check returned "nothing missing", the service raised nothing, and
the scoring ran weighting a column that is NULL on every row, contributing 0.0
to every score in silence.

It is not hypothetical. ``anc2vec_neighbor_cos`` and
``anc2vec_neighbor_maxcos`` are real GOPrediction columns, non-null in 0 of
2,441,584 rows of the current prediction set, and neither is in the map. No
scoring config weights them today (0 of 8), so nothing has been scored wrongly
yet, and the next config to try one would have been approved.

The direction matters. Walking the map asks only about signals someone
remembered to map, so an unmapped one passes by never being asked, which is the
opposite of a check. Walking the weights asks about everything the config
actually uses.
"""

from __future__ import annotations

import uuid
from typing import Any

from protea.services._scoring_validation_helpers import (
    _SIGNAL_TO_COLUMN,
    compute_missing_signals,
)


class _Config:
    """Only the two attributes the check reads."""

    def __init__(self, weights: dict[str, float], formula: str = "linear") -> None:
        self.weights = weights
        self.formula = formula


class _NeverAsked:
    """A session that fails loudly if the check tries to query.

    The unmapped-signal path must decide without touching the database, and a
    Mock returning a Mock would let a query that should not happen pass
    unnoticed.
    """

    def execute(self, *_a: Any, **_k: Any) -> Any:  # pragma: no cover - must not run
        raise AssertionError("the check queried the database for an unmapped signal")


def test_a_weighted_signal_with_no_column_is_reported() -> None:
    config = _Config({"anc2vec_neighbor_cos": 0.4})
    missing = compute_missing_signals(_NeverAsked(), uuid.uuid4(), config)  # type: ignore[arg-type]

    assert len(missing) == 1
    assert "anc2vec_neighbor_cos" in missing[0]
    # The message has to say what to do. "missing" alone sends a reader looking
    # for absent data when the data may be there and only the mapping is not.
    assert "_SIGNAL_TO_COLUMN" in missing[0]


def test_a_zero_weight_signal_is_not_reported() -> None:
    """Only signals the config actually uses.

    Reporting a signal present at weight 0 would fire on every config that
    lists a signal it has switched off, and a check that fires constantly is
    one that gets switched off itself.
    """
    config = _Config({"anc2vec_neighbor_cos": 0.0, "unknown_thing": 0})
    assert compute_missing_signals(_NeverAsked(), uuid.uuid4(), config) == []  # type: ignore[arg-type]


def test_a_config_of_only_mapped_signals_still_asks_the_database() -> None:
    """The change must not turn the real coverage check off.

    This asserts by the exception the fake session raises: reaching it means
    the mapped path still queries, which is the behaviour the whole helper
    exists for.
    """
    config = _Config({"identity_nw": 0.5})
    try:
        compute_missing_signals(_NeverAsked(), uuid.uuid4(), config)  # type: ignore[arg-type]
    except AssertionError as exc:
        assert "queried the database" in str(exc)
    else:  # pragma: no cover - would mean the coverage check stopped running
        raise AssertionError("a mapped signal no longer triggers the coverage query")


def test_the_map_covers_the_signals_the_shipped_configs_weight() -> None:
    """Every signal a real config weights has to be checkable.

    Read from the scoring config model's own defaults rather than a list
    written here, so a signal added there is covered the day it lands.
    """
    from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig

    defaults = getattr(ScoringConfig, "DEFAULT_WEIGHTS", None) or {}
    unmapped = sorted(
        s for s, w in defaults.items() if float(w or 0) > 0 and s not in _SIGNAL_TO_COLUMN
    )
    assert not unmapped, f"weighted by default and not checkable: {unmapped}"
