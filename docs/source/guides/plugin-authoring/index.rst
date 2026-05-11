Plugin authoring guide
======================

PROTEA's plugin system lets you extend the platform without touching
``protea-core``. New protein language models, annotation sources, and
experiment runners ship as independent Python packages discovered at
runtime via ``importlib.metadata`` entry points. This guide explains
the common mechanics that apply to every plugin layer, and links to the
per-layer guides with concrete worked examples.

.. toctree::
   :maxdepth: 2

   backend
   runner
   source

.. _plugin-contracts:

Contracts package
-----------------

Every plugin depends on `protea-contracts
<https://github.com/frapercan/protea-contracts>`_ (``pip install
protea-contracts``). This package contains:

- The abstract base classes (ABCs) that plugins must subclass.
- The payload and record types that cross the plugin/operation
  boundary.
- The feature registry contract used by in-process feature plugins.

``protea-contracts`` has no dependency on ``torch``, ``sqlalchemy``,
``fastapi``, or ``protea-core``. Keeping the dependency graph shallow
is intentional: consumers (labs, runners, external tools) can import
the contracts without pulling in the full platform stack.

.. _entry-points:

Entry points
------------

Plugin discovery uses the standard Python entry-points mechanism.
``protea-core`` calls::

    importlib.metadata.entry_points(group="protea.<layer>")

at startup for each of the three external layers:

.. list-table::
   :header-rows: 1
   :widths: 25 30 45

   * - Layer
     - Entry-point group
     - ABC
   * - PLM backends
     - ``protea.backends``
     - :class:`protea_contracts.EmbeddingBackend`
   * - Experiment runners
     - ``protea.runners``
     - :class:`protea_contracts.ExperimentRunner`
   * - Annotation sources
     - ``protea.sources``
     - :class:`protea_contracts.AnnotationSource`

Each entry point resolves to a **module-level instance** of the plugin
class (named ``plugin`` by convention). ``protea-core`` then verifies
that ``plugin.name == ep.name``; a mismatch raises ``RuntimeError``
before the worker starts.

.. _plugin-anatomy:

Anatomy of a plugin
-------------------

Regardless of layer, every plugin follows the same five-step pattern:

1. A Python module under the relevant repository, e.g.
   ``src/protea_backends/myplugin/__init__.py``.
2. A class that subclasses the relevant ABC and implements all
   abstract methods, with a ``name`` class attribute matching the
   entry-point key.
3. A module-level sentinel ``plugin = MyPlugin()`` that the entry
   point resolves to.
4. An entry in ``pyproject.toml`` under
   ``[tool.poetry.plugins."protea.<group>"]``::

       myplugin = "protea_<group>.myplugin:plugin"

5. A test file covering: ABC compliance, ``name`` attribute,
   entry-point discoverability, and method signatures.

Heavy optional dependencies belong behind `Poetry extras
<https://python-poetry.org/docs/pyproject/#extras>`_ and are imported
lazily inside the method body, not at module top. This keeps plugin
discovery import-cheap: ``protea-core`` does not pay for ``torch`` at
startup unless that backend is actually invoked.

.. _plugin-semver:

SemVer and the contract surface
--------------------------------

Plugin packages follow `Semantic Versioning <https://semver.org/>`_
with these rules inherited from the ``protea-contracts`` contract:

- **Patch**: documentation, internal refactor, bug fix. No interface
  change. No consumer action required.
- **Minor**: additive contract extension (new optional argument, new
  optional return field). Consumers MAY adopt but are not forced.
- **Major**: breaking change (renamed method, required argument added,
  return type changed, ABC method removed). All consumers of that
  layer must be updated together.

When ``protea-contracts`` bumps its minor or major version,
``protea-core`` and all three plugin repositories update their
``protea-contracts`` dependency in the same PR (coordinated bump
protocol, documented in ``protea-contracts/CHANGELOG.md``).

.. _plugin-shipping:

Shipping checklist
------------------

Before opening a PR for a new plugin:

- [ ] All abstract methods implemented (``mypy --strict`` passes).
- [ ] ``plugin.name`` matches the entry-point key in ``pyproject.toml``.
- [ ] Tests: ABC compliance, name attribute, entry-point resolution.
- [ ] Heavy ML deps are extras, not hard deps; lazy imports confirmed.
- [ ] ``ruff check .`` passes (line-length 100, ``E501`` ignored).
- [ ] ``CHANGELOG.md`` entry under ``[Unreleased]`` with the new
  plugin name.
- [ ] PR opened against ``develop``; label ``feat:`` or ``plugin:``.
