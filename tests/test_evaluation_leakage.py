"""The evaluation refuses an encoding the frame cannot certify.

``leakage_guard`` shipped on the morning of 2026-08-20 with a correct
rule, a full docstring and its own tests, and nothing called it.
``plans/rungs.yaml`` declares for rung 2 that CI refuses an artifact whose
cut falls inside the frame it is about to be scored in, and CI did not,
because the refusal lived in a function with no caller. Three fitted
artifacts were about to be evaluated against a gate that was decorative.

So the first test is not about leakage. It asserts that the evaluation
path calls the guard, which is the property that was missing and the one
a reader of either file could not have checked.
"""

from __future__ import annotations

import inspect

import pytest

from protea.core._evaluation_leakage import (
    FITTED_BACKENDS,
    refuse_uncertifiable_encoding,
)
from protea.core.leakage_guard import Frame, LeakageRefusal, check_training_cut
from protea.core.operations import batch_rescore_evaluation, run_cafa_evaluation


def test_the_evaluation_path_calls_the_guard():
    src = inspect.getsource(
        run_cafa_evaluation.RunCafaEvaluationOperation._load_evaluation_inputs
    )
    assert "refuse_uncertifiable_encoding" in src


def test_the_rescore_path_shares_that_loader():
    # Both evaluation operations resolve inputs through the same method, so
    # wiring the refusal there covers the rescore too. If that stops being
    # true the rescore silently loses the guard, which is how it was lost
    # in the first place.
    assert "_load_evaluation_inputs" in inspect.getsource(batch_rescore_evaluation)


class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class _Session:
    """Enough of a Session to answer the gets the caller makes."""

    def __init__(self, rows):
        self._rows = rows

    def get(self, model, pk):
        return self._rows.get((model.__name__, pk))


def _session(*, backend, cut_release, frame=("220", "230")):
    rows = {
        ("AnnotationSet", "old"): _Row(source_version=frame[0]),
        ("AnnotationSet", "new"): _Row(source_version=frame[1]),
        ("EmbeddingConfig", "cfg"): _Row(
            model_backend=backend,
            model_name="arm",
            trained_on_annotation_set_id="cut" if cut_release else None,
        ),
    }
    if cut_release:
        rows[("AnnotationSet", "cut")] = _Row(source_version=cut_release)
    return _Session(rows)


def _call(session):
    refuse_uncertifiable_encoding(
        session,
        _Row(embedding_config_id="cfg"),
        _Row(old_annotation_set_id="old", new_annotation_set_id="new"),
    )


def test_a_pretrained_backbone_passes_without_declaring_anything():
    # NULL means not fitted, the honest state for a backbone used as it
    # ships. Refusing it would refuse every arm of rung 1.
    _call(_session(backend="ankh", cut_release=None))


def test_a_fitted_encoding_that_declares_nothing_is_refused():
    # The case that was about to go through. The artifact was fitted and
    # said nothing, so NULL read as "not fitted".
    with pytest.raises(LeakageRefusal, match="declares no training release"):
        _call(_session(backend="learned-code", cut_release=None))


def test_a_cut_inside_the_frame_is_refused():
    with pytest.raises(LeakageRefusal, match="lies inside"):
        _call(_session(backend="residue-sparse", cut_release="227"))


def test_a_cut_at_the_frame_start_is_allowed():
    # What rung 2's arms do: fitted on 220, scored on 220 to 230. The
    # encoder saw the world at t0, which every arm is entitled to, and
    # refusing it would refuse the experiment.
    _call(_session(backend="residue-sparse", cut_release="220"))


def test_a_frame_without_release_ordinals_is_not_refused():
    # A property of older evaluation sets rather than of the encoding. The
    # guard has nothing to say, so it must not block the work.
    _call(_session(backend="learned-code", cut_release="220", frame=("v1", "v2")))


def test_fitted_is_decided_by_the_backend_tag():
    # Not by the cut column: an artifact fitted and undeclared has NULL
    # there, and reading NULL as "not fitted" waves through exactly the
    # case this exists to catch.
    assert FITTED_BACKENDS == {"residue-sparse", "learned-code"}


def test_the_boundary_is_where_the_guard_says_it_is():
    check_training_cut(fitted=True, training_release=220, frame=Frame(220, 230))
    with pytest.raises(LeakageRefusal):
        check_training_cut(fitted=True, training_release=221, frame=Frame(220, 230))
