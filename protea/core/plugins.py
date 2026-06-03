"""Generic entry-point plugin discovery (T2A.5 + T2A.8).

Both backends (T2A.5) and runners (T2A.8) follow the same shape: a
package declares plugins in ``[tool.poetry.plugins."protea.<group>"]``
mapping ``name = "module:plugin"``, PROTEA discovers them via
``importlib.metadata.entry_points``, and a resolver maps a string
identifier to a plugin instance for dispatch.

This module centralises that discovery + cache + name-mismatch hard
error so the backend and runner code paths share one implementation
instead of forking. See ``protea.core.runners`` and the
``_get_backend_plugins`` shim in
``protea.core.operations.compute_embeddings`` for the call sites.
"""

from __future__ import annotations

from typing import Any

# Per-group cache. ``None`` means "not yet discovered"; an empty dict
# means "no plugins installed" (which is a hard error at the resolver
# level, not a registry warning).
_PLUGIN_CACHES: dict[str, dict[str, Any] | None] = {}


def discover_plugins(group: str) -> dict[str, Any]:
    """Discover and cache plugins in the given ``entry_points`` group.

    Returns a dict keyed by ``plugin.name``. Each plugin must declare a
    ``name`` class attribute that matches its entry-point name; mismatches
    raise ``RuntimeError`` at discovery time so the failure surface is
    a clear "your declaration drifted from your entry_points file"
    rather than a confusing "Unknown <something>" later on.

    Discovery happens once per process per group; subsequent calls return
    the cached map. The cache key is the group string itself, so the
    same group cannot be discovered twice (which keeps the import-time
    side effects stable across reload-heavy test runs).
    """
    cache = _PLUGIN_CACHES.get(group)
    if cache is None:
        from importlib.metadata import entry_points

        new_cache: dict[str, Any] = {}
        for ep in entry_points(group=group):
            plugin = ep.load()
            plugin_name = getattr(plugin, "name", None)
            if plugin_name != ep.name:
                raise RuntimeError(
                    f"Plugin name mismatch in group {group!r}: entry_point "
                    f"{ep.name!r} resolves to plugin with name "
                    f"{plugin_name!r}"
                )
            new_cache[ep.name] = plugin
        _PLUGIN_CACHES[group] = new_cache
        cache = new_cache
    return cache


def reset_plugin_cache(group: str | None = None) -> None:
    """Drop the cached plugin map for one (or every) group.

    Test-only seam: lets unit tests force re-discovery without restarting
    the process. Passing ``group=None`` clears every group; passing a
    specific name clears only that one. Production callers should not
    invoke this; the cache is intentionally process-local.
    """
    if group is None:
        _PLUGIN_CACHES.clear()
    else:
        _PLUGIN_CACHES.pop(group, None)
