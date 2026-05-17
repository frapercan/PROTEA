Plugin author guide
====================

PROTEA is built around a plugin architecture. Annotation sources, PLM
backends, experiment runners and per-candidate features are added as
out-of-tree contributions without modifying ``protea-core``. This
page is the top-level guide for plugin authors: which abstract base
class to implement, where the implementation lives, how the platform
discovers it, and where to find the per-repo guides with concrete
templates.

.. seealso::

   The detailed per-layer guides with worked toy-plugin examples live
   under :doc:`guides/plugin-authoring/index`. Start there if you want
   a runnable code template to copy.

The canonical source of truth for the contracts themselves is the
`protea-contracts <https://github.com/frapercan/protea-contracts>`_
package and its Sphinx documentation; this page links to it
throughout.

Architecture in one paragraph
-----------------------------

``protea-core`` is the platform: the ORM, the FastAPI surface, the
RabbitMQ workers, the orchestration loop. Plugins live in four
sibling repositories. Each repository declares its plugins through
the Python ``entry_points`` mechanism (one mechanism, four named
groups). At startup ``protea-core`` queries
``importlib.metadata.entry_points`` for each group and loads the
plugin instances; from that moment on, every dispatch by name is a
dictionary lookup.

The four plugin layers
----------------------

.. list-table::
   :header-rows: 1
   :widths: 18 30 26 26

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

Picking the right ABC
---------------------

**You want to ingest a new annotation source** (a database release, a
file format, a web API that produces ``ProteinGOAnnotation`` rows).
Implement ``AnnotationSource`` in ``protea-sources``. Examples
shipped today: ``goa``, ``quickgo``, ``uniprot``.

**You want to add a new protein language model** (a HuggingFace
checkpoint, a structure-aware encoder, a distilled variant).
Implement ``EmbeddingBackend`` in ``protea-backends``. Examples
shipped today: ``esm``, ``t5``, ``ankh``, ``esm3c``.

**You want to add a new training method** (a different boosting
algorithm, a graph neural network, a retrieval-neural ranker).
Implement ``ExperimentRunner`` in ``protea-runners``. Examples
shipped today: ``knn``, ``baseline``, ``lightgbm`` (the latter
materialises in F2A.7).

**You want to add a feature to the re-ranker** (a new sequence
metric, a new ontology-aware embedding, a new taxonomic signal).
Register a :class:`protea_contracts.Feature` in
``protea-core/protea/core/features/<family>.py``. This is in-process
and does not use ``entry_points``: the registry is gathered at import
time from a fixed list of family modules. The feature's ``family``
field decides where it appears in the dataset schema and feeds into
``compute_schema_sha`` (see :doc:`adr/D10-schema-sha-parallel-migration`).

If your idea fits none of these layers, it probably belongs in
``protea-core`` itself. Open an issue describing what you want to
add; the architecture review may suggest a fifth layer or surface a
hidden constraint.

Anatomy of a plugin
-------------------

Independent of the layer, every plugin follows the same shape:

1. A Python module under the relevant repository, named after the
   plugin (``protea_backends/myplugin/__init__.py``).
2. A class that subclasses the relevant ABC and implements the
   abstract methods, with a class attribute ``name`` matching the
   entry-point name.
3. A module-level instance ``plugin = MyPlugin()`` that is what the
   entry-point resolves to.
4. A line under ``[tool.poetry.plugins."protea.<group>"]`` in the
   repository's ``pyproject.toml``::

      myplugin = "protea_<group>.myplugin:plugin"

5. A test file that exercises the contract: instance type, ABC
   compliance, ``name`` attribute, discoverability via
   ``entry_points(group="protea.<group>")``, and the public method
   signatures. The existing test files in each repository are good
   templates.

Heavy dependencies belong behind Poetry extras and are imported
lazily inside the method that needs them, not at module top. This
keeps plugin discovery import-cheap; ``protea-core`` does not pay for
``torch`` (or any other heavy dependency) at startup if no caller is
actually invoking the backend that uses it.

Where to find the concrete guides
---------------------------------

Each plugin repository ships its own contributing guide with a
runnable template, the SemVer policy that applies to its public
surface, and CI expectations:

- **protea-backends**: see ``docs/source/contributing.rst`` in
  the repository, and the per-backend pages
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

Discovery in code
-----------------

Should you want to verify a plugin is discoverable from a Python
shell::

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

Schema invariants and reproducibility
-------------------------------------

Plugins must respect the platform's reproducibility contract. Two
specific places this matters:

- **Feature plugins** participate in ``compute_schema_sha``. Adding a
  feature changes the digest, which is correct: existing re-ranker
  boosters trained against the old digest will refuse to load
  against the new one. Bump the package minor and re-train.
  See :doc:`adr/D10-schema-sha-parallel-migration` for the parallel-column migration
  that brings every consumer onto a single source of truth.
- **Embedding backends** must return float16 embeddings of shape
  ``(batch_size, hidden_dim)``. Special tokens (``CLS``, ``EOS``,
  ``BOS``, prefix tokens) must be stripped before pooling. Variations
  in tokenisation policy across backends are acceptable as long as
  the final pooled vector is a faithful per-protein representation.

Both invariants are enforced by tests in ``protea-core`` and by
golden parquet bit-exact comparisons in F2 (T2B.2 of the master
plan). Breaking either is loud, not silent.

Roadmap
-------

Several phases of the master plan revision 3 directly affect plugin authors:

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
  :doc:`adr/D14-plugin-granularity`.
