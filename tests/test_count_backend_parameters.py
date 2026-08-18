"""Counting what a backend executes, and keeping that claim honest over time.

The operation writes a number that is deliberately smaller than the published one
for two of the deployed checkpoints, so the risk it carries is not that it
crashes. The risk is that it keeps returning a number that was true once: a
backend changes which module its forward path calls, the map here does not, and
the count stays plausible while becoming wrong in exactly the direction that
matters.

So the load-bearing test in this file is not a count. It is the one that reads
each backend's own source and fails when the submodule this operation names stops
appearing in its forward path.
"""

from __future__ import annotations

import inspect

import pytest

from protea.core.operations.count_backend_parameters import (
    EXECUTED_SUBMODULE,
    CountBackendParametersOperation,
    CountBackendParametersPayload,
    _Checkpoint,
    count_checkpoint,
    group_by_checkpoint,
)


class _Param:
    """Stands in for a tensor; only numel is ever read."""

    def __init__(self, n: int) -> None:
        self._n = n

    def numel(self) -> int:
        return self._n


class _Module:
    def __init__(self, *sizes: int, **children: object) -> None:
        self._params = [_Param(s) for s in sizes]
        for name, child in children.items():
            setattr(self, name, child)

    def parameters(self):
        return iter(self._params)


class _Plugin:
    """A backend plugin that hands back a prepared model."""

    def __init__(self, model: object) -> None:
        self.model = model
        self.calls: list[tuple[str, str]] = []

    def load_model(self, model_name: str, device: str, emit):
        self.calls.append((model_name, device))
        return self.model, object()


def _emit(*_a, **_k) -> None:
    return None


# --------------------------------------------------------------------------- payload

def test_the_default_is_every_configuration_still_missing_a_count():
    """The ordinary invocation takes no arguments and is idempotent."""
    p = CountBackendParametersPayload()

    assert p.only_missing is True
    assert p.embedding_config_ids is None
    assert p.dry_run is False


def test_it_counts_on_cpu_by_default():
    """numel does not depend on device, and the card is for inference."""
    assert CountBackendParametersPayload().device == "cpu"


def test_an_empty_selection_is_refused_because_it_reads_as_its_own_opposite():
    """Omitting the field means all; an empty list looks like none but is falsy.

    Without this, a selection that narrowed to nothing upstream would widen to
    everything here and load every checkpoint in the registry, ProstT5 included.
    """
    with pytest.raises(ValueError, match="empty list"):
        CountBackendParametersPayload(embedding_config_ids=[])


def test_omitting_the_selection_still_means_every_configuration():
    assert CountBackendParametersPayload().embedding_config_ids is None


# --------------------------------------------------------------------------- grouping

class _Config:
    def __init__(self, cid: str, backend: str, model_name: str) -> None:
        self.id = cid
        self.model_backend = backend
        self.model_name = model_name


def test_configurations_sharing_a_checkpoint_are_loaded_once():
    """The whole reason this is affordable: eight configs are not eight loads."""
    configs = [
        _Config("a", "t5", "Rostlab/ProstT5"),
        _Config("b", "t5", "Rostlab/ProstT5"),
        _Config("c", "t5", "Rostlab/ProstT5"),
    ]

    got = group_by_checkpoint(configs)

    assert len(got) == 1
    assert set(got[0].config_ids) == {"a", "b", "c"}


def test_the_same_name_under_a_different_backend_is_a_different_checkpoint():
    """Grouping on the name alone would merge two models that load differently."""
    configs = [_Config("a", "t5", "X"), _Config("b", "ankh", "X")]

    assert len(group_by_checkpoint(configs)) == 2


def test_an_empty_selection_groups_to_nothing():
    assert group_by_checkpoint([]) == []


# --------------------------------------------------------------------------- counting

def test_an_ordinary_backend_counts_the_whole_loaded_model():
    plugin = _Plugin(_Module(100, 50))

    got = count_checkpoint(plugin, _Checkpoint("t5", "Rostlab/ProstT5", ("a",)), "cpu", _emit)

    assert got.loaded == 150
    assert got.executed == 150


def test_a_declared_submodule_is_counted_instead_of_the_whole_model():
    """ProtST loads a text tower it never runs, and the gap is about 111M."""
    model = _Module(1_000, protein_model=_Module(600))
    plugin = _Plugin(model)

    got = count_checkpoint(plugin, _Checkpoint("protst", "mila-intel/ProtST-esm1b", ("a",)),
                           "cpu", _emit)

    assert got.loaded == 1_000
    assert got.executed == 600
    assert got.module == "protein_model"


def test_a_stale_map_raises_rather_than_falling_back_to_the_loaded_total():
    """The regression this file exists for.

    Falling back silently would publish the checkpoint total under the name of an
    executed count, which is wrong in exactly the direction the operation exists
    to correct, and nothing downstream could see it.
    """
    plugin = _Plugin(_Module(1_000))

    with pytest.raises(ValueError, match="stale"):
        count_checkpoint(plugin, _Checkpoint("protst", "m", ("a",)), "cpu", _emit)


def test_the_plugin_is_asked_for_the_model_rather_than_the_checkpoint_opened_here():
    """A backend that changes how it instantiates must change this count with it."""
    plugin = _Plugin(_Module(10))

    count_checkpoint(plugin, _Checkpoint("t5", "Rostlab/ProstT5", ("a",)), "cpu", _emit)

    assert plugin.calls == [("Rostlab/ProstT5", "cpu")]


# --------------------------------------------------------------- the anti-drift check

def test_every_declared_submodule_still_appears_in_its_backend_forward_path():
    """Reads the backend's own source, so the map cannot drift away from it quietly.

    This is the load-bearing test. If a backend stops calling the submodule named
    here, the count silently becomes a number that was true once, and no other
    check in the project would notice.
    """
    for backend, attribute in EXECUTED_SUBMODULE.items():
        module = pytest.importorskip(f"protea_backends.{backend}")
        source = inspect.getsource(module)

        assert f".{attribute}(" in source, (
            f"backend {backend!r} no longer calls {attribute!r} in its forward "
            f"path; EXECUTED_SUBMODULE in count_backend_parameters is stale"
        )


def test_the_map_only_names_backends_that_exist():
    """A typo here would count the whole model and never say so."""
    known = {"esm", "esm3c", "t5", "ankh", "protst"}

    assert set(EXECUTED_SUBMODULE) <= known


# --------------------------------------------------------------------------- operation

def test_the_operation_says_what_it_writes():
    op = CountBackendParametersOperation()

    assert op.name == "count_backend_parameters"
    assert "param_count" in op.description


def test_the_description_warns_that_the_number_is_below_the_published_one():
    """A reader comparing against a paper must not conclude this is a bug."""
    op = CountBackendParametersOperation()

    assert "encoder-only" in op.description


def test_summarize_names_the_scope():
    op = CountBackendParametersOperation()

    assert "missing" in op.summarize_payload({})
    assert "2 configurations" in op.summarize_payload({"embedding_config_ids": ["a", "b"]})


def test_it_is_registered_in_the_catalog():
    """An operation that is not registered cannot be dispatched, which is the
    entire point of giving the measurement a producer."""
    from protea.core.operation_catalog import build_operation_registry

    registry = build_operation_registry()

    assert registry.get("count_backend_parameters").name == "count_backend_parameters"
