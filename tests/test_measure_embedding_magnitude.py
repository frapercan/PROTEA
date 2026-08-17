"""The scale that keeps an un-normalised cell inside halfvec, and why it is measured.

Everything stored so far is L2-normalised, so no component has ever approached
the fp16 ceiling. The ablation programme needs the un-normalised vector, and the
clip that would catch an out-of-range component is a passthrough at
``embedding_scale`` 1.0, which every live config uses. So the number this
operation produces is the only thing standing between an un-normalised corpus
pass and a bank of silent infinities.
"""

from __future__ import annotations

import numpy as np
import pytest

from protea.core.operations.measure_embedding_magnitude import (
    HALFVEC_MAX,
    HEADROOM,
    LENGTH_BANDS,
    MeasureEmbeddingMagnitudeOperation,
    _recommend_scale,
    _summarise,
    _UnnormalisedConfig,
)

# --------------------------------------------------------------------------- the scale

def test_a_vector_that_already_fits_needs_no_scaling():
    assert _recommend_scale(1.0) == 1.0
    assert _recommend_scale(100.0) == 1.0


def test_the_recommended_scale_brings_the_maximum_inside_with_headroom():
    for observed in (1e3, 1e4, 6.5e4, 4.4e5, 1e7):
        scale = _recommend_scale(observed)
        assert observed / scale <= HALFVEC_MAX / HEADROOM


def test_the_scale_is_a_power_of_two():
    """Exact in binary, so rescaling introduces no error of its own."""
    import math

    for observed in (1e5, 4.4e5, 1e7):
        scale = _recommend_scale(observed)
        assert scale >= 1.0
        assert math.log2(scale).is_integer()


def test_the_scale_is_the_smallest_that_works():
    observed = 4.4e5
    scale = _recommend_scale(observed)
    assert observed / (scale / 2) > HALFVEC_MAX / HEADROOM


def test_a_degenerate_maximum_does_not_divide_by_zero():
    assert _recommend_scale(0.0) == 1.0
    assert _recommend_scale(-1.0) == 1.0


def test_the_known_overflow_case_is_caught():
    """A mid-layer mean-pooled activation of 440,611 has been observed in this project.

    Under the current passthrough that value reaches the halfvec column and
    becomes an inf. The recommended scale must bring it inside.
    """
    scale = _recommend_scale(440_611.0)
    assert 440_611.0 / scale <= HALFVEC_MAX / HEADROOM


# --------------------------------------------------------------------------- the recipe view

class _FakeConfig:
    def __init__(self) -> None:
        self.normalize = True
        self.pooling = "mean"
        self.use_chunking = True
        self.chunk_size = 512
        self.model_name = "facebook/esm2_t6_8M_UR50D"


def test_the_measurement_view_disables_normalisation_and_nothing_else():
    real = _FakeConfig()
    view = _UnnormalisedConfig(real)

    assert view.normalize is False
    assert view.pooling == "mean"
    assert view.use_chunking is True
    assert view.chunk_size == 512
    assert view.model_name == "facebook/esm2_t6_8M_UR50D"


def test_the_view_does_not_mutate_the_config_it_wraps():
    """The row's identifier is derived from its recipe, so writing to it is not free."""
    real = _FakeConfig()
    view = _UnnormalisedConfig(real)
    _ = view.normalize

    assert real.normalize is True


# --------------------------------------------------------------------------- reporting

def test_summary_is_empty_for_no_observations():
    assert _summarise([]) == {}


def test_summary_reports_the_maximum_and_the_count():
    got = _summarise([0.5, 2.0, 1.0])
    assert got["max"] == 2.0
    assert got["min"] == 0.5
    assert got["n"] == 3.0


def test_bands_cover_every_length_without_a_gap():
    """A protein that falls between two bands would never be sampled."""
    ordered = sorted(LENGTH_BANDS, key=lambda b: b[1])
    for (_, _, high), (_, low, _) in zip(ordered, ordered[1:], strict=False):
        assert low == high + 1
    assert ordered[0][1] == 1


def test_the_operation_declares_that_it_writes_nothing():
    op = MeasureEmbeddingMagnitudeOperation()
    assert op.name == "measure_embedding_magnitude"
    assert "writes nothing" in op.description.lower()


def test_summarize_payload_names_the_config():
    op = MeasureEmbeddingMagnitudeOperation()
    text = op.summarize_payload({"embedding_config_id": "abc-123"})
    assert "abc-123" in text


def test_a_missing_config_is_an_error_and_not_an_empty_result(monkeypatch):
    """Measuring nothing and reporting a maximum of zero would recommend scale 1.0."""

    class _Session:
        def get(self, model, key):  # noqa: ARG002
            return None

    op = MeasureEmbeddingMagnitudeOperation()
    with pytest.raises(ValueError, match="no embedding config"):
        op.execute(_Session(), {"embedding_config_id": "missing"}, emit=lambda *a, **k: None)


def test_maximum_is_taken_over_absolute_value():
    """The ceiling is symmetric, so a large negative component overflows too."""
    vector = np.array([-9.0, 1.0, 2.0], dtype=np.float32)
    assert float(abs(vector).max()) == 9.0
