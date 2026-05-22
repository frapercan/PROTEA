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

**Architecture overview**

``protea-core`` is the platform: the ORM, the FastAPI surface, the
RabbitMQ workers, the orchestration loop. Plugins live in four
sibling repositories. Each repository declares its plugins through
the Python ``entry_points`` mechanism (one mechanism, four named
groups). At startup ``protea-core`` queries
``importlib.metadata.entry_points`` for each group and loads the
plugin instances; from that moment on, every dispatch by name is a
dictionary lookup.

**The four plugin layers**

.. list-table::
   :header-rows: 1
   :widths: 18 28 26 28

   * - Layer
     - ABC
     - Repository
     - Entry-point group
   * - Annotation sources
     - :class:`protea_contracts.AnnotationSource`
     - `protea-sources <https://github.com/frapercan/protea-sources>`_
     - ``protea.sources``
   * - PLM backends
     - :class:`protea_contracts.EmbeddingBackend`
     - `protea-backends <https://github.com/frapercan/protea-backends>`_
     - ``protea.backends``
   * - Experiment runners
     - :class:`protea_contracts.ExperimentRunner`
     - `protea-runners <https://github.com/frapercan/protea-runners>`_
     - ``protea.runners``
   * - Per-candidate features
     - :class:`protea_contracts.FeatureRegistry`
     - ``protea-core/protea/core/features/``
     - in-process registry (no entry-point group)

**Picking the right layer**

*You want to ingest a new annotation source* (a database release, a
file format, a web API that produces ``ProteinGOAnnotation`` rows):
implement ``AnnotationSource`` in ``protea-sources``. Examples
shipped today: ``goa``, ``quickgo``, ``uniprot``.

*You want to add a new protein language model* (a HuggingFace
checkpoint, a structure-aware encoder, a distilled variant):
implement ``EmbeddingBackend`` in ``protea-backends``. Examples
shipped today: ``esm``, ``t5``, ``ankh``, ``esm3c``.

*You want to add a new training method* (a different boosting
algorithm, a graph neural network, a retrieval-neural ranker):
implement ``ExperimentRunner`` in ``protea-runners``. Examples
shipped today: ``knn``, ``baseline``, ``lightgbm``.

*You want to add a feature to the re-ranker* (a new sequence metric,
a new ontology-aware embedding, a new taxonomic signal): register a
:class:`protea_contracts.Feature` in
``protea-core/protea/core/features/<family>.py``. This is in-process
and does not use ``entry_points``: the registry is gathered at import
time from a fixed list of family modules. The feature's ``family``
field decides where it appears in the dataset schema and feeds into
``compute_schema_sha``
(see :doc:`/adr/D10-schema-sha-parallel-migration`).

If your idea fits none of these layers, it probably belongs in
``protea-core`` itself. Open an issue describing what you want to
add; the architecture review may suggest a fifth layer or surface a
hidden constraint.

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

.. _plugin-discovery-code:

.. rubric:: Verifying discoverability

To confirm a plugin is discoverable from a Python shell::

    from importlib.metadata import entry_points

    eps = entry_points(group="protea.backends")
    for ep in eps:
        print(ep.name, "->", ep.value)
        plugin = ep.load()
        print("  name attr:", plugin.name)

This is exactly what ``protea-core`` does at startup. The only thing
``protea-core`` adds is a sanity check: ``plugin.name`` must equal
``ep.name`` or the worker raises ``RuntimeError`` rather than start.
This catches typos in the entry-point declaration the only place
they could otherwise hide.

.. _plugin-schema-invariants:

.. rubric:: Schema invariants and reproducibility

Plugins must respect the platform's reproducibility contract. Two
specific places this matters:

- **Feature plugins** participate in ``compute_schema_sha``. Adding a
  feature changes the digest, which is correct: existing re-ranker
  boosters trained against the old digest will refuse to load
  against the new one. Bump the package minor and re-train.
  See :doc:`/adr/D10-schema-sha-parallel-migration` for the
  parallel-column migration that brings every consumer onto a single
  source of truth.
- **Embedding backends** must return float16 embeddings of shape
  ``(batch_size, hidden_dim)``. Special tokens (``CLS``, ``EOS``,
  ``BOS``, prefix tokens) must be stripped before pooling. Variations
  in tokenisation policy across backends are acceptable as long as
  the final pooled vector is a faithful per-protein representation.

Both invariants are enforced by tests in ``protea-core`` and by
golden parquet bit-exact comparisons in F2 (T2B.2 of the master
plan). Breaking either is loud, not silent.

.. _plugin-per-repo-guides:

.. rubric:: Per-repository guides

Each plugin repository ships its own contributing guide with a
runnable template, the SemVer policy that applies to its public
surface, and CI expectations:

- **protea-backends**: see ``docs/source/contributing.rst`` in the
  repository, and the per-backend pages
  (``docs/source/backends/{esm,t5,ankh,esm3c}.rst``) for examples of
  how to document a backend's quirks (numerical type, pooling rule,
  tokeniser idiosyncrasies).
- **protea-contracts**: see ``docs/source/contributing.rst`` for
  the SemVer rules that govern when a contract change is patch,
  minor or major, the procedure for adding a feature to
  ``ALL_FEATURES`` (which changes the schema sha and forces booster
  retraining), and the ABC additive-vs-breaking guidance.
- **protea-sources** and **protea-runners**: Sphinx scaffolding for
  these is on the doc lane (Doc-T8); until it lands, the existing
  README plus the ``protea-backends`` guide above are the closest
  template (the patterns transfer: substitute the ABC and the
  entry-point group).

.. _plugin-roadmap:

.. rubric:: Roadmap

Several phases of the master plan directly affect plugin authors:

- **F2A.7**: ``protea-runners.lightgbm`` absorbs the standalone
  ``protea-reranker-lab`` repository as the canonical LightGBM
  runner.
- **F2B**: the in-process ``FeatureRegistry`` is wired into
  ``parquet_export`` and ``predict_go_terms`` so that every
  registered feature flows end-to-end without manual list
  maintenance.
- **F2C**: ``protea-method`` extracts the inference path as a
  pure-Python package consumable without the platform; this becomes
  the single shippable target for downstream adopters and for the
  LAFA submission containers (F-LAFA).
- **F9** (post-defense): if third parties publish plugins, the per
  group repositories may split into per-plugin repositories. See
  :doc:`/adr/D14-plugin-granularity`.
