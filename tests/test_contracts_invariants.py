"""Cross-repo invariant tests between PROTEA, protea-contracts and the
plugin repositories (T1.7 of master plan v3).

The four invariants below pin contract-level guarantees that bind the
umbrella consumer (PROTEA) to the canonical contracts package and the
three plugin entry-point groups (``protea.sources``, ``protea.runners``,
``protea.backends``). Any drift here is a silent cross-repo break that
would otherwise surface either at job-dispatch time (payloads), at
plugin discovery (ABC compliance), or after hours of dump compute (the
T1.8 canonical-column boundary).

Each invariant is one focused test function. The file is intentionally
self-contained: it inspects already-installed packages via Python's
public introspection surface (``inspect``, ``importlib.metadata``, AST)
rather than wiring a separate plugin-version matrix. A follow-up slice
(see PR body) is expected to lift this scaffolding into a CI matrix
that pins each plugin's tagged version.

Invariants enforced here:

1. ``test_operation_payloads_derive_from_protea_payload``: every
   operation registered in PROTEA's ``OperationRegistry`` declares
   (via ``XxxPayload.model_validate(payload)`` inside its ``execute``)
   a payload class that is a subclass of
   ``protea_contracts.ProteaPayload``. Catches inline ``pydantic.BaseModel``
   payloads that bypass the canonical base.
2. ``test_plugin_entry_points_implement_abc``: every entry point in
   ``protea.sources`` / ``protea.runners`` / ``protea.backends`` loads
   a plugin instance whose class is a subclass of the matching ABC
   from ``protea_contracts``.
3. ``test_canonical_features_are_subset_of_leaf_record_outputs``: the
   canonical ``ALL_FEATURES`` column set is a strict subset of the
   keys produced by ``_LeafRecordBuilder.make_leaf_record`` (the
   training-dump producer). Catches the T-RES.1b incident pattern
   (column registered in contracts, no producer wired, dump fails the
   T1.8 invariant after hours of compute).
4. ``test_operation_names_are_unique_across_plugins``: no two plugins
   (or PROTEA operations) register the same dispatch name. Catches
   accidental shadowing between ``protea.sources`` entries and
   ``protea.runners`` / ``protea.backends`` entries.
"""

from __future__ import annotations

import ast
import inspect
from collections.abc import Iterable
from importlib import metadata
from types import ModuleType

import protea_contracts
import pytest
from protea_contracts import (
    ALL_FEATURES,
    EMBEDDING_PCA_DIM,
    AnnotationSource,
    EmbeddingBackend,
    ExperimentRunner,
    ProteaPayload,
)

from protea.core.contracts.registry import OperationRegistry
from protea.core.operation_catalog import build_operation_registry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PLUGIN_GROUP_TO_ABC: dict[str, type] = {
    "protea.sources": AnnotationSource,
    "protea.runners": ExperimentRunner,
    "protea.backends": EmbeddingBackend,
}


def _operation_registry() -> OperationRegistry:
    """Build the live registry once per test invocation.

    Kept as a tiny helper so the test functions stay declarative and
    so the heavy import (operation_catalog pulls every operation module
    transitively) is paid once per test rather than at module import.
    """
    return build_operation_registry()


def _operations_payload_class(op: object) -> set[str]:
    """Return the names of every class used as ``X.model_validate(payload)``
    inside the given operation's ``execute`` method.

    Operations without a payload model (e.g. the ``ping`` smoke op) yield
    an empty set. The result is the set of *names* rather than resolved
    classes so the helper works for static-analysis use cases too; the
    caller resolves names against the module's namespace.
    """
    src = inspect.getsource(op.__class__)
    tree = ast.parse(src)
    found: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr != "model_validate":
            continue
        base = node.func.value
        if isinstance(base, ast.Name):
            found.add(base.id)
    return found


def _resolve_payload_classes(op: object, names: Iterable[str]) -> list[type]:
    """Resolve payload class names against the operation module's namespace."""
    mod: ModuleType | None = inspect.getmodule(op.__class__)
    assert mod is not None, f"operation {op!r} has no enclosing module"
    resolved: list[type] = []
    for name in names:
        cls = getattr(mod, name, None)
        if not isinstance(cls, type):
            pytest.fail(
                f"Payload class {name!r} referenced by "
                f"{op.__class__.__name__}.execute is not importable from "
                f"{mod.__name__}"
            )
        resolved.append(cls)
    return resolved


def _string_keys_in_module_dict_literals(module: ModuleType) -> set[str]:
    """Statically collect every string literal used as a key in any
    ``ast.Dict`` literal inside the module's source.

    This is the workhorse for invariant (3): the training-dump leaf
    record is built incrementally via ``rec.update({...})`` calls plus
    a final ``rec.update({f"emb_pca_query_{i}": ...})`` comprehension.
    Walking every Dict in the module captures the union of column names
    the builder ever writes, which is exactly the surface the T1.8
    canonical-column boundary asserts against.
    """
    src = inspect.getsource(module)
    tree = ast.parse(src)
    keys: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        for k in node.keys:
            if isinstance(k, ast.Constant) and isinstance(k.value, str):
                keys.add(k.value)
    return keys


# ---------------------------------------------------------------------------
# Invariant 1: every operation's request payload derives from ProteaPayload
# ---------------------------------------------------------------------------


def test_operation_payloads_derive_from_protea_payload() -> None:
    """Every registered operation's request payload subclasses
    ``protea_contracts.ProteaPayload``.

    Operations with no payload (the smoke-test ``ping`` op) are
    exempt. For all others, the class referenced via
    ``XxxPayload.model_validate(payload)`` inside ``execute`` must be a
    ``ProteaPayload`` subclass so the strict-validation contract
    (``model_config = ConfigDict(strict=True, frozen=True)``) holds
    uniformly across the platform.
    """
    registry = _operation_registry()
    offenders: list[tuple[str, str]] = []

    for op_name, op in registry._ops.items():
        names = _operations_payload_class(op)
        if not names:
            continue
        for cls in _resolve_payload_classes(op, names):
            if not issubclass(cls, ProteaPayload):
                offenders.append((op_name, cls.__name__))

    assert offenders == [], (
        "Operations declaring payload classes that do not derive from "
        "protea_contracts.ProteaPayload: "
        f"{offenders!r}. Add ``ProteaPayload`` to the payload's bases "
        "(or import the canonical payload from protea_contracts)."
    )


# ---------------------------------------------------------------------------
# Invariant 2: every plugin entry point implements its ABC
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("group,abc", list(PLUGIN_GROUP_TO_ABC.items()))
def test_plugin_entry_points_implement_abc(group: str, abc: type) -> None:
    """Every entry point in the three plugin groups loads a plugin
    instance whose class subclasses the matching ABC from
    ``protea_contracts``.

    Parametrised on the (group, ABC) pairs so failures pinpoint which
    plugin repo is in violation (the test id encodes the group name).
    Skips with a clear reason when zero entry points are registered for
    a group (that is itself a different failure: plugin install
    missing in the venv), but it is not a contract drift we want to
    conflate with ABC compliance.
    """
    eps = list(metadata.entry_points(group=group))
    if not eps:
        pytest.skip(
            f"No entry points registered for {group!r}; "
            "install the plugin package in the test venv before "
            "running this invariant."
        )

    offenders: list[tuple[str, str]] = []
    for ep in eps:
        plugin = ep.load()
        cls = plugin if isinstance(plugin, type) else plugin.__class__
        if not issubclass(cls, abc):
            offenders.append((ep.name, cls.__name__))

    assert offenders == [], (
        f"Entry points in {group!r} whose plugin class does not "
        f"subclass {abc.__name__}: {offenders!r}. Inherit from the ABC "
        f"in protea_contracts."
    )


# ---------------------------------------------------------------------------
# Invariant 3: ALL_FEATURES is a subset of leaf-record producer outputs
# ---------------------------------------------------------------------------


def test_canonical_features_are_subset_of_leaf_record_outputs() -> None:
    """Every column in ``protea_contracts.ALL_FEATURES`` must be writable
    by ``protea.core._leaf_record_builder._LeafRecordBuilder`` (the
    training-dump producer that feeds the T1.8 parquet_export boundary).

    Why this matters (see project memory
    ``project_canonical_feature_producer_consumer.md``): adding a
    column to ``ALL_FEATURES`` is a one-line edit in protea-contracts,
    but the downstream invariant
    ``parquet_export._assert_canonical_columns`` requires every leaf
    record to carry that column. Without a producer wiring, the dump
    pipeline runs for hours and then fails the canonical-column gate
    at write time. T-RES.1b shipped exactly this drift (4 ``lineage_*``
    columns, no dump-side producer) and lost ~5 h of LB.1 compute.

    The check walks ``_LeafRecordBuilder``'s module source via AST,
    collects every string key used in a ``rec.update({...})`` dict
    literal, expands the ``emb_pca_query_{i}`` family up to
    ``EMBEDDING_PCA_DIM``, and asserts the union covers
    ``ALL_FEATURES``.
    """
    import protea.core._leaf_record_builder as builder_module
    from protea.core.features._bindings import _POOL_INJECTED_FEATURES

    produced = _string_keys_in_module_dict_literals(builder_module)
    # The emb_pca_query_* family is materialised via an f-string in a
    # dict comprehension; the AST walk above does not see it as a
    # literal so we expand the contract-side family explicitly.
    for i in range(EMBEDDING_PCA_DIM):
        produced.add(f"emb_pca_query_{i}")

    # Pool-injected columns are exempt: PROTEA declares them but the lab stamps
    # them when pooling manifests, so the leaf-record producer must NOT write
    # them and the export boundary does not ask for them.
    missing = set(ALL_FEATURES) - produced - set(_POOL_INJECTED_FEATURES)
    assert missing == set(), (
        "Columns in protea_contracts.ALL_FEATURES that the leaf-record "
        f"producer never writes: {sorted(missing)!r}. Wire a real producer in "
        "protea.core._leaf_record_builder._LeafRecordBuilder.make_leaf_record, "
        "or, if the column is genuinely produced elsewhere, add it to "
        "protea.core.features._bindings._POOL_INJECTED_FEATURES. Do NOT reach "
        "for a zero-fill default: a constant zero is indistinguishable from a "
        "measurement, which is the seam ADR-D45 documents. Emit NaN when no "
        "producer ran, and record the family as declared-absent."
    )


# ---------------------------------------------------------------------------
# Invariant 4: dispatch names are unique across plugins and operations
# ---------------------------------------------------------------------------


def test_operation_names_are_unique_across_plugins() -> None:
    """No two registered dispatch names (PROTEA operations + plugin
    entry-point names across all three groups) collide.

    Each plugin group has its own dispatch namespace at runtime, but a
    collision across groups (e.g. an annotation source called ``knn``
    while a runner is also called ``knn``) is a strong signal of a
    naming mistake that will confuse operators and surface as the
    wrong handler being invoked through the UI. The test enforces a
    global uniqueness contract spanning the registry and the plugin
    groups so the naming surface stays unambiguous.
    """
    seen: dict[str, str] = {}
    collisions: list[tuple[str, str, str]] = []

    registry = _operation_registry()
    for op_name in registry._ops:
        seen[op_name] = "protea.core.operations"

    for group in PLUGIN_GROUP_TO_ABC:
        for ep in metadata.entry_points(group=group):
            if ep.name in seen:
                collisions.append((ep.name, seen[ep.name], group))
            else:
                seen[ep.name] = group

    assert collisions == [], (
        "Dispatch-name collisions across PROTEA operations and plugin "
        f"groups: {collisions!r}. Rename one of the conflicting "
        "entries so the global dispatch namespace stays unambiguous."
    )


# ---------------------------------------------------------------------------
# Defensive sanity check: ProteaPayload re-export identity
# ---------------------------------------------------------------------------


def test_protea_payload_is_canonical_in_core_contracts() -> None:
    """PROTEA's ``protea.core.contracts.operation.ProteaPayload`` is the
    same object as ``protea_contracts.ProteaPayload``.

    Not strictly one of the four headline invariants but a cheap
    safeguard against accidentally re-defining ``ProteaPayload`` locally
    inside PROTEA, which would silently break invariant (1); every
    op would still subclass *a* ``ProteaPayload``, just not the
    canonical one in protea-contracts. Mirrors the identity test in
    ``tests/test_feature_contract.py`` for the feature-schema family.
    """
    from protea.core.contracts.operation import ProteaPayload as core_payload

    assert core_payload is protea_contracts.ProteaPayload
