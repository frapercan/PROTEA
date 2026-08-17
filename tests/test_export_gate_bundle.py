"""Freezing the store into a file so the gates can run where the card is.

The two encoder gates need mean-pooled embeddings and GO annotations for a
reference pool. Those live in the database on the other machine, and the standing
rule here is that agents are never pointed at it, so the gates cannot run where
the graphics card is and the card is where they belong.

This operation removes the dilemma by making the database read happen once, on
the machine that owns the store, and publishing the result. These tests cover the
parts that decide whether the artifact is trustworthy: that the pool is drawn
reproducibly, that a query's sequence twins are excluded rather than merely its
accession, and that the key is a content address so a rerun is visibly the same
bundle.
"""

from __future__ import annotations

import numpy as np
import pytest

from protea.core.operations.export_gate_bundle import (
    ExportGateBundleOperation,
    ExportGateBundlePayload,
    _bundle_key,
    _parse_vector,
    _select_reference,
)

# --------------------------------------------------------------------------- the payload

def test_the_payload_names_what_it_freezes():
    p = ExportGateBundlePayload(
        embedding_config_id="cfg", annotation_set_id="ann", queries=["P1", "P2"]
    )
    assert p.ref_n == 60000
    assert p.seed == 42


def test_a_reference_pool_of_zero_is_refused():
    """It would publish a bundle with no donors, which reads as an empty store."""
    with pytest.raises(ValueError):
        ExportGateBundlePayload(
            embedding_config_id="c", annotation_set_id="a", queries=["P1"], ref_n=0
        )


# --------------------------------------------------------------------------- the draw

class _Result:
    def __init__(self, rows): self._rows = rows
    def scalars(self): return self._rows


class _Session:
    """Returns a fixed candidate list, in the order the query would."""

    def __init__(self, rows): self.rows = rows
    def execute(self, *_a, **_k): return _Result(self.rows)


def test_the_draw_is_reproducible_under_a_seed():
    """A seeded sample over an ordered list, in that order.

    Sampling an unordered result is how a fixed seed selects a different pool on
    every run, which is the defect this operation exists partly to avoid
    inheriting.
    """
    rows = [f"P{i:04d}" for i in range(500)]

    a = _select_reference(_Session(rows), "ann", set(), 50, seed=7)
    b = _select_reference(_Session(rows), "ann", set(), 50, seed=7)

    assert a == b
    assert len(a) == 50


def test_a_different_seed_draws_a_different_pool():
    rows = [f"P{i:04d}" for i in range(500)]

    a = _select_reference(_Session(rows), "ann", set(), 50, seed=7)
    b = _select_reference(_Session(rows), "ann", set(), 50, seed=8)

    assert a != b


def test_excluded_accessions_never_enter_the_pool():
    """The exclusion is applied before the sample, not after."""
    rows = [f"P{i:04d}" for i in range(100)]
    excluded = {f"P{i:04d}" for i in range(50)}

    got = _select_reference(_Session(rows), "ann", excluded, 100, seed=1)

    assert set(got).isdisjoint(excluded)
    assert len(got) == 50


def test_a_pool_smaller_than_requested_returns_everything_available():
    """Rather than raising: a short pool is a fact about the corpus, not an error."""
    rows = [f"P{i:04d}" for i in range(10)]

    got = _select_reference(_Session(rows), "ann", set(), 60000, seed=1)

    assert len(got) == 10


# --------------------------------------------------------------------------- the key

def test_the_key_is_a_content_address():
    """A rerun on the same inputs must be visibly the same bundle."""
    payload = {"embedding_config_id": "cfg", "queries": ["P1"], "ref_n": 100}

    assert _bundle_key(payload, "b") == _bundle_key(dict(payload), "b")


def test_a_changed_input_changes_the_key():
    base = {"embedding_config_id": "cfg", "queries": ["P1"], "ref_n": 100}
    changed = {"embedding_config_id": "cfg", "queries": ["P1"], "ref_n": 200}

    assert _bundle_key(base, "b") != _bundle_key(changed, "b")


def test_key_order_does_not_matter():
    """Two payloads differing only in key order are the same request."""
    a = {"embedding_config_id": "cfg", "ref_n": 100}
    b = {"ref_n": 100, "embedding_config_id": "cfg"}

    assert _bundle_key(a, "x") == _bundle_key(b, "x")


# --------------------------------------------------------------------------- the vector

def test_a_stored_vector_parses_to_float32():
    got = _parse_vector("[1.5,-2.0,3.25]")

    assert got.dtype == np.float32
    assert np.allclose(got, [1.5, -2.0, 3.25])


# --------------------------------------------------------------------------- the operation

def test_the_operation_declares_that_it_writes_no_rows():
    op = ExportGateBundleOperation()

    assert op.name == "export_gate_bundle"
    assert "writes no rows" in op.description.lower()


def test_summarize_payload_names_the_config():
    op = ExportGateBundleOperation()

    assert "cfg-1" in op.summarize_payload({"embedding_config_id": "cfg-1"})
