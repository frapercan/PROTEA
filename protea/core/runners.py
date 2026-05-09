"""Experiment-runner plugin adapter (T2A.8).

Mirrors the backend-plugin adapter in
:mod:`protea.core.operations.compute_embeddings`: discovers runners via
``importlib.metadata.entry_points("protea.runners")`` and exposes a
:func:`resolve_runner` lookup that maps a string identifier (e.g.
``"knn"`` / ``"baseline"`` / ``"lightgbm"``) to a runner plugin
instance implementing :class:`protea_contracts.ExperimentRunner`.

The ``protea-runners`` package declares the canonical plugin set:

* ``knn``: KNN-only baseline (no reranker), evaluates against a frozen
  dataset for ablation.
* ``baseline``: predict-by-frequency baseline (most-frequent GO terms in
  the training set).
* ``lightgbm``: the v9 / v18 LightGBM reranker, currently trained
  out-of-tree in ``protea-reranker-lab``.

PROTEA does not yet dispatch to runners at inference time (the active
KNN + reranker path lives in ``PredictGOTermsBatchOperation`` and stays
there until F2C of master plan v3 hoists the inference core into a
package both PROTEA and protea-runners can depend on). The adapter
exists today so:

1. The plugin registry endpoint (``GET /v1/registry/runners``) has a
   stable resolver under the hood (currently still using its own
   ``_discover`` for the API shape; that path is untouched here).
2. Future code that wants a runner instance has a one-line entry:
   ``from protea.core.runners import resolve_runner``.
3. The discovery + name-mismatch hard error follows the same pattern
   as backends, so the lab + plugin authoring workflow is uniform
   across both groups.
"""

from __future__ import annotations

from typing import Any

from protea.core.plugins import discover_plugins

PROTEA_RUNNERS_GROUP = "protea.runners"


def get_runner_plugins() -> dict[str, Any]:
    """Return the discovered runner plugins keyed by ``plugin.name``.

    Wraps :func:`protea.core.plugins.discover_plugins` so the runner
    callers do not need to know the entry-point group string; the
    constant lives here next to the consumers.
    """
    return discover_plugins(PROTEA_RUNNERS_GROUP)


def resolve_runner(name: str) -> Any:
    """Resolve a runner identifier to a plugin instance.

    Raises ``ValueError`` listing the discovered runners when the
    identifier is unknown, so the failure message is actionable. The
    returned object implements
    :class:`protea_contracts.ExperimentRunner`.
    """
    plugins = get_runner_plugins()
    if name not in plugins:
        raise ValueError(
            f"Unknown runner: {name!r}. "
            f"Discovered: {sorted(plugins)}"
        )
    return plugins[name]
