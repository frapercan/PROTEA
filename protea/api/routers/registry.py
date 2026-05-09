"""Plugin registry endpoints.

Three read-only endpoints listing the plugins discovered at runtime
via :mod:`importlib.metadata.entry_points`:

  * ``GET /backends``: embedding backend plugins (``protea.backends``)
  * ``GET /sources``: annotation source plugins (``protea.sources``)
  * ``GET /runners``: experiment runner plugins (``protea.runners``)

Each response is a flat list of :class:`PluginInfo` records describing
the entry-point name, class, module path, and any plugin-specific
metadata exposed via attributes (e.g. :attr:`AnnotationSource.version`).

The endpoints are intentionally stateless: they re-scan
``entry_points`` on every call rather than caching, so a worker
that's just been restarted with a newly-installed extra surfaces in
the next request without an API restart. The scan is cheap (sub-ms
on the working set of ~10 plugins).
"""

from __future__ import annotations

from importlib.metadata import entry_points
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter(tags=["registry"])


_KNOWN_GROUPS = {
    "backends": "protea.backends",
    "sources": "protea.sources",
    "runners": "protea.runners",
}


class PluginInfo(BaseModel):
    """Metadata for one discovered plugin."""

    name: str = Field(
        ...,
        description=(
            "Entry-point name (e.g. ``esm``, ``goa``, ``lightgbm``); "
            "matches the plugin's ``name`` class attribute by "
            "convention."
        ),
    )
    cls: str = Field(
        ...,
        description="Plugin class name (e.g. ``EsmBackend``, ``GoaSource``).",
    )
    module: str = Field(
        ...,
        description=(
            "Fully-qualified entry-point value "
            "(e.g. ``protea_backends.esm:plugin``)."
        ),
    )
    extra: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Plugin-specific metadata read from the loaded instance. "
            "Today carries ``version`` for sources; empty for backends "
            "and runners."
        ),
    )


class PluginListResponse(BaseModel):
    """Response shape for the three registry endpoints."""

    group: str = Field(
        ...,
        description=(
            "The ``entry_points`` group queried "
            "(e.g. ``protea.backends``)."
        ),
    )
    plugins: list[PluginInfo] = Field(
        ...,
        description="Discovered plugins, sorted alphabetically by ``name``.",
    )


def _discover(group: str) -> list[PluginInfo]:
    """Resolve all entry points in ``group`` and build their PluginInfo.

    Loading the entry-point fires the plugin module's import side
    effects but should not raise for any first-party plugin (the
    bootstrapping pattern keeps top-level imports cheap). If a
    third-party plugin's load raises, the caller surfaces it as a
    500: better to fail loud than silently hide a broken install.
    """
    discovered: list[PluginInfo] = []
    for ep in entry_points(group=group):
        plugin = ep.load()
        extra: dict[str, Any] = {}
        version = getattr(plugin, "version", None)
        if isinstance(version, str):
            extra["version"] = version
        discovered.append(
            PluginInfo(
                name=ep.name,
                cls=type(plugin).__name__,
                module=ep.value,
                extra=extra,
            )
        )
    discovered.sort(key=lambda p: p.name)
    return discovered


def _list_for(slug: str) -> PluginListResponse:
    """Shared body for the three endpoints; looks up the canonical
    ``entry_points`` group from ``_KNOWN_GROUPS`` and returns the
    discovered plugins.
    """
    group = _KNOWN_GROUPS.get(slug)
    if group is None:
        raise HTTPException(status_code=404, detail=f"unknown registry: {slug!r}")
    return PluginListResponse(group=group, plugins=_discover(group))


@router.get("/backends", response_model=PluginListResponse)
def list_backends() -> PluginListResponse:
    """List all installed embedding backend plugins.

    The plugin set depends on which ``protea-backends[<extra>]``
    extras are installed (esm, t5, ankh, esm3c). With the default
    install all four are discoverable; only the ones whose lazy
    imports succeed at ``stream_*`` time will actually run on GPU.
    """
    return _list_for("backends")


@router.get("/sources", response_model=PluginListResponse)
def list_sources() -> PluginListResponse:
    """List all installed annotation source plugins.

    Today: ``goa``, ``quickgo``, ``uniprot`` (all real after
    F2A.6-real). The ``extra.version`` field surfaces the
    :attr:`AnnotationSource.version` declared on each plugin
    (e.g. ``"uniprot-goa"``, ``"quickgo-rest"``)."""
    return _list_for("sources")


@router.get("/runners", response_model=PluginListResponse)
def list_runners() -> PluginListResponse:
    """List all installed experiment runner plugins.

    Today: ``baseline``, ``knn``, ``lightgbm``. The latter two are
    contract-surface stubs until F2A.7 (lab → ``protea-runners
    .lightgbm`` migration) and F2C.1 (``protea-method`` extraction)
    move the real implementations here.
    """
    return _list_for("runners")
