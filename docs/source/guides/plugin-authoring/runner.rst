Runner plugin guide
===================

An experiment runner plugin encapsulates a training/evaluation
lifecycle: it consumes a frozen dataset published by PROTEA, trains or
evaluates a model, and returns structured result objects. ``protea-core``
resolves runners by the ``name`` attribute via the ``protea.runners``
entry-point group.

Existing runners shipped in `protea-runners
<https://github.com/frapercan/protea-runners>`_: ``knn``, ``baseline``,
``lightgbm``.

.. _runner-abc:

The ABC
-------

Your class must subclass :class:`protea_contracts.ExperimentRunner`
and implement three abstract methods:

.. code-block:: python

   from protea_contracts.experiment_runner import (
       ExperimentRunner,
       RunResult,
       EvalResult,
   )
   from typing import Any

   class ExperimentRunner(ABC):
       name: str

       @abstractmethod
       def fit(
           self,
           spec: dict[str, Any],
           dataset_uri: str,
           *,
           emit: Any,
       ) -> RunResult:
           ...

       @abstractmethod
       def evaluate(
           self,
           model_uri: str,
           eval_dataset_uri: str,
           *,
           emit: Any,
       ) -> EvalResult:
           ...

       @abstractmethod
       def export(
           self,
           run_id: str,
           output_uri: str,
           *,
           emit: Any,
       ) -> dict[str, Any]:
           ...

The return types from ``protea-contracts``:

.. code-block:: python

   @dataclass(frozen=True)
   class RunResult:
       model_uri: str           # opaque store URI (local or s3://)
       metrics: dict[str, Any]  # runner-side metrics persisted by platform
       extras: dict[str, Any]   # runner-specific diagnostics

   @dataclass(frozen=True)
   class EvalResult:
       metrics: dict[str, Any]  # canonical CAFA schema: fmax, auc_pr, coverage
       extras: dict[str, Any]   # runner-specific diagnostics

Key invariants:

- ``fit`` receives ``spec`` (runner-specific hyperparameters, validated
  by the implementation) and ``dataset_uri`` (an opaque store URI
  resolved by ``ArtifactStore``). It returns a ``RunResult`` containing
  the URI of the produced artefact.
- ``evaluate`` loads a previously produced artefact by ``model_uri``
  and scores it against ``eval_dataset_uri``. Return ``metrics`` in the
  canonical CAFA schema (``fmax``, ``auc_pr``, ``coverage`` per aspect)
  so the platform can persist them alongside the run row.
- ``export`` writes the full artefact triple
  (``model.txt`` / ``spec.yaml`` / ``run.json`` or equivalent) under
  ``output_uri`` and returns a dict of URIs + content hashes.
- All three methods receive an ``emit`` callback (same signature as
  in the backend guide).
- If a runner genuinely has no training step (e.g. KNN has no
  parameters), ``fit`` should raise ``NotImplementedError`` with a
  clear message rather than silently returning a dummy result.

.. _runner-packaging:

Packaging snippet
-----------------

.. code-block:: toml

   [tool.poetry]
   name = "protea-runners-myrunner"
   version = "0.1.0"
   packages = [{ include = "protea_runners_myrunner", from = "src" }]

   [tool.poetry.dependencies]
   python = ">=3.12,<4.0"
   protea-contracts = ">=0.2"
   numpy = ">=1.24"
   pyarrow = ">=14"

   [tool.poetry.plugins."protea.runners"]
   myrunner = "protea_runners_myrunner:plugin"

.. _runner-tests:

Test scaffold
-------------

Copy and adapt from ``protea-runners/tests/test_knn.py``:

.. code-block:: python

   """Smoke tests for the myrunner plugin."""

   from importlib.metadata import entry_points

   import pytest
   from protea_contracts import ExperimentRunner
   from protea_runners_myrunner import MyRunner, plugin


   def test_plugin_is_myrunner_instance() -> None:
       assert isinstance(plugin, MyRunner)


   def test_plugin_implements_experiment_runner_abc() -> None:
       assert isinstance(plugin, ExperimentRunner)


   def test_plugin_name_matches_entry_point_key() -> None:
       assert plugin.name == "myrunner"


   def test_plugin_resolvable_via_entry_points() -> None:
       eps = entry_points(group="protea.runners")
       matches = [ep for ep in eps if ep.name == "myrunner"]
       assert len(matches) == 1
       assert matches[0].load() is plugin


   def test_fit_returns_run_result() -> None:
       noop = lambda *a, **k: None   # noqa: E731
       result = plugin.fit({}, "file:///tmp/demo_dataset/", emit=noop)
       assert result.model_uri.startswith("file://")


   def test_evaluate_returns_eval_result() -> None:
       noop = lambda *a, **k: None   # noqa: E731
       result = plugin.evaluate(
           "file:///tmp/model.txt", "file:///tmp/eval_dataset/", emit=noop
       )
       assert "fmax" in result.metrics

.. _runner-toy-example:

Worked example: ``toy`` runner
--------------------------------

The ``toy`` runner is a minimal ``ExperimentRunner`` that stores a
constant ``model_uri`` (no actual model file), reports dummy CAFA
metrics, and exports an empty JSON manifest. It demonstrates the full
lifecycle contract without any ML dependency.

.. code-block:: python

   # src/protea_runners_toy/__init__.py
   """Toy experiment runner: no-op fit/evaluate/export for testing.

   Implements the full ExperimentRunner contract without any ML
   dependency. Useful as a template and in CI pipelines that need to
   exercise the runner dispatch path without a real training run.

   Install:
       pip install -e .
   """

   from __future__ import annotations

   import json
   import os
   from typing import Any

   from protea_contracts.experiment_runner import (
       EvalResult,
       ExperimentRunner,
       RunResult,
   )

   _DUMMY_MODEL_URI = "file:///dev/null"


   class ToyRunner(ExperimentRunner):
       """No-op runner that satisfies the ExperimentRunner contract.

       fit:      records the spec as a JSON file under dataset_uri and
                 returns a RunResult pointing at a /dev/null model.
       evaluate: returns fixed CAFA-schema metrics (all 1.0).
       export:   writes a minimal JSON manifest to output_uri.
       """

       name = "toy"

       def fit(
           self,
           spec: dict[str, Any],
           dataset_uri: str,
           *,
           emit: Any,
       ) -> RunResult:
           """Record the spec and return a dummy model_uri."""
           emit("runner.toy.fit_start", None, {"dataset_uri": dataset_uri}, "info")

           # Write spec.json next to the dataset so there is at least one
           # artefact the caller can inspect.
           spec_path = os.path.join(dataset_uri.replace("file://", ""), "spec.json")
           os.makedirs(os.path.dirname(spec_path), exist_ok=True)
           with open(spec_path, "w") as fh:
               json.dump(spec, fh)

           emit("runner.toy.fit_done", None, {}, "info")
           return RunResult(
               model_uri=_DUMMY_MODEL_URI,
               metrics={"dummy_loss": 0.0},
           )

       def evaluate(
           self,
           model_uri: str,
           eval_dataset_uri: str,
           *,
           emit: Any,
       ) -> EvalResult:
           """Return perfect CAFA-schema metrics (placeholder)."""
           emit("runner.toy.eval_start", None, {}, "info")
           return EvalResult(
               metrics={
                   "fmax": 1.0,
                   "auc_pr": 1.0,
                   "coverage": 1.0,
               }
           )

       def export(
           self,
           run_id: str,
           output_uri: str,
           *,
           emit: Any,
       ) -> dict[str, Any]:
           """Write a minimal JSON manifest to output_uri."""
           out_dir = output_uri.replace("file://", "")
           os.makedirs(out_dir, exist_ok=True)
           manifest = {"run_id": run_id, "model_uri": _DUMMY_MODEL_URI}
           path = os.path.join(out_dir, "manifest.json")
           with open(path, "w") as fh:
               json.dump(manifest, fh)
           return {"manifest_uri": f"file://{path}"}


   #: Module-level instance discovered via ``protea.runners`` entry_points.
   plugin = ToyRunner()

The corresponding ``pyproject.toml`` entry-point stanza:

.. code-block:: toml

   [tool.poetry.plugins."protea.runners"]
   toy = "protea_runners_toy:plugin"

Verify end-to-end in a scratch directory:

.. code-block:: python

   import tempfile, os
   from protea_runners_toy import plugin

   noop = lambda *a, **k: None

   with tempfile.TemporaryDirectory() as tmp:
       dataset_uri = f"file://{tmp}/dataset/"
       os.makedirs(dataset_uri.replace("file://", ""), exist_ok=True)

       run = plugin.fit({"lr": 0.1}, dataset_uri, emit=noop)
       print(run.model_uri)        # Expected output: file:///dev/null
       print(run.metrics)          # Expected output: {'dummy_loss': 0.0}

       ev = plugin.evaluate(run.model_uri, dataset_uri, emit=noop)
       print(ev.metrics["fmax"])   # Expected output: 1.0

       out_uri = f"file://{tmp}/export/"
       ex = plugin.export("run_001", out_uri, emit=noop)
       print(ex["manifest_uri"])   # Expected output: file:///tmp/.../manifest.json
