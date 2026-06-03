ADR-D36: PLM axis explicit in dataset naming
============================================

:Status: Accepted
:Date: 2026-05-20

Context
~~~~~~~

The reranker benchmark dataset historically known as
``bench-v1-K5-v226-lineage`` was built against ProstT5 embeddings
(``embedding_config_id=c0ae5b69-d6dc-41cf-a711-1739d3d2e170``), but the
PLM choice was not encoded in the dataset name. The same name was used
across publishable prose, lab YAMLs, scripts, ADR D34, and the PROTEA
``Dataset`` registry row.

Once the multi-PLM v226 sweep plan landed
(memory ``project_multi_plm_v226_sweep_plan``, 2026-05-19), the slate of
eight canonical PLMs (ADR D35) each needs its own per-K dataset. A
single name shared across PLMs is structurally ambiguous: it cannot
distinguish the existing ProstT5 build from the seven sibling builds
scheduled by FARM-EXP.13, and it silently breaks any axis-tuple
shortid lookup that takes ``eval_set_name`` as part of the canonical
payload.

The transversal catalog
(``protea-reranker-lab/experiments/_catalog/transversal.yaml``) made
the underlying problem visible: 96 cells carried the PLM-blind
``eval_set: bench-v1-K5-v226-lineage`` value while their ``plm``
axis ranged across eight PLMs, meaning the catalog implicitly
overloaded one dataset name to mean eight different things depending
on which other axis the reader was holding fixed.

Decision
~~~~~~~~

1. **Per-PLM dataset name template.** The canonical name for a lineage
   reranker dataset is

   .. code-block:: text

      bench-v1-K{k}-v{val_band}-lineage-{plm_short}

   where ``k`` is the KNN neighbour count, ``val_band`` is the
   evaluation snapshot band (``226`` for the v226 to v230 window), and
   ``plm_short`` is the canonical short key from ADR D35:

   .. list-table:: Canonical PLM short keys
      :header-rows: 1
      :widths: 30 70

      * - plm_short
        - HF / SDK checkpoint
      * - ``esm2_150m``
        - facebook/esm2_t30_150M_UR50D
      * - ``esm2_650m``
        - facebook/esm2_t33_650M_UR50D
      * - ``esm2_3b``
        - facebook/esm2_t36_3B_UR50D
      * - ``prot_t5``
        - Rostlab/prot_t5_xl_half_uniref50-enc
      * - ``prostt5``
        - Rostlab/ProstT5
      * - ``ankh_base``
        - ElnaggarLab/ankh-base
      * - ``ankh_large``
        - ElnaggarLab/ankh-large
      * - ``esmc_600m``
        - esmc_600m (EvolutionaryScale SDK)

   The ``esmc_300m`` baseline (orphan per ADR D35) is permitted as a
   ninth ``plm_short`` value for chapter-6 single-PLM comparisons.

2. **The existing build is renamed to ``-prostt5``.** The
   pre-FARM-EXP.12 ``bench-v1-K5-v226-lineage`` row in the PROTEA
   ``Dataset`` registry refers to the ProstT5 build
   (``c0ae5b69``); it is renamed in place to
   ``bench-v1-K5-v226-lineage-prostt5`` via the Alembic data-only
   migration ``f1a2b3c4d5e6_farm_exp_12_rename_lineage_dataset_to_prostt5.py``
   (file path symbolic; see the migrations directory for the
   actual filename). The legacy name is recorded in the row's
   ``meta`` JSONB under the ``alias_names`` key so any code path
   still resolving by the old name has a traceable fallback path
   (the column ``Dataset.meta`` already exists; no schema change is
   required).

3. **Backward-compat handle.** Lookups by the legacy name
   (``bench-v1-K5-v226-lineage``) continue to resolve through the
   ``alias_names`` field for one release cycle. A follow-up slice
   removes the alias once every caller has migrated to the canonical
   per-PLM name.

4. **Lab-side rename.** The lab repo
   (``protea-reranker-lab``) rewrites every prose / yaml / python
   reference to the legacy name to the canonical
   ``bench-v1-K5-v226-lineage-prostt5`` form. The transversal catalog
   builder (``scripts/build_study_specs.py``) gains a
   ``_resolve_eval_set(family, plm, k)`` helper that expands the
   ``lineage`` family into one
   ``bench-v1-K{k}-v226-lineage-{plm_short}`` per PLM, so the catalog
   now carries 12 cells per PLM (96 cells total) rather than one
   PLM-blind block.

5. **Pre-commit / CI linter.** The lab repo ships
   ``scripts/lint_dataset_names.py`` which rejects any committed file
   containing ``bench-v1-K{k}-v{val_band}-lineage`` without a
   ``-{plm_short}`` or ``-mini`` suffix. The linter is wired into
   ``.pre-commit-config.yaml`` and a dedicated CI workflow
   (``.github/workflows/dataset-name-lint.yml``).

6. **Champion records updated.** ``champions.md`` is rewritten so
   every axis tuple that pointed at the legacy name now reads the
   ``-prostt5`` form; the 9-cell selective-rerank champion row
   (study_v23, post-replication-artefact fix) keeps its measured numbers
   (``selective_avg_cafaeval = 0.6215``); only the ``dataset`` column
   text changes.

7. **No data movement.** Train / eval parquet contents and the
   manifest ``key_prefix`` are not rewritten in the artifact store as
   part of this slice; only the registry ``Dataset.name`` and lab
   prose change. The artifact ``key_prefix`` (e.g.
   ``datasets/bench-v1-K5-v226-lineage/``) keeps its legacy form in
   the storage backend so prior pulls and pre-computed caches keep
   resolving. A future cleanup slice (post-FARM-EXP.13 sibling builds)
   may rename the storage prefix in lockstep.

Consequences
~~~~~~~~~~~~

**Positive**

- The PLM axis is now explicit at dataset-name level; multi-PLM
  sweeps (FARM-EXP.13) can name their eight sibling datasets without
  ambiguity.
- Axis-tuple shortid lookups across repos stay consistent: a
  ``ExperimentRun.axis_tuple_shortid`` row produced before the rename
  is still recoverable via the ``alias_names`` field on the
  ``Dataset`` row.
- A linter prevents the legacy untagged form from re-entering prose.
- The transversal catalog goes from a PLM-blind 12-cell block to a
  PLM-explicit 96-cell block (12 per PLM), surfacing structural
  coverage the previous catalog had hidden.

**Negative**

- 48 lab files plus 6 PROTEA files were rewritten in a single sweep;
  reviewers must read the diff for content drift (mostly
  search-and-replace, but the catalog regeneration changes 96 cell
  shortids because the eval-set name is part of the axis payload).
- The Alembic migration is data-only against the live registry;
  rolling back the migration restores the legacy name but does not
  rewind the lab prose. The downgrade is a registry-only revert.
- Any external consumer that pulled the dataset by the legacy name
  before the alias was wired must update its config.

**Neutral**

- The artifact-store ``key_prefix`` keeps the legacy directory name
  (``datasets/bench-v1-K5-v226-lineage/``). Storage rename is
  scheduled as a follow-up so disk and S3 layouts stay stable while
  the rename propagates through downstream tools.

References
~~~~~~~~~~

- ADR D35 (canonical 8-PLM embedding config IDs).
- ADR D34 (selective rerank resurrection; live PROTEA inference
  policy on the prostt5 build).
- Memory entry ``project_canonical_8plm_embedding_configs``
  (canonical PLM short keys).
- Memory entry ``project_multi_plm_v226_sweep_plan`` (locked
  per-PLM dataset naming, 2026-05-19).
- Slice ``FARM-EXP.12`` (farm-platform loop) and the follow-up
  ``FARM-EXP.13`` per-PLM dataset-family build.
- Lab linter ``scripts/lint_dataset_names.py`` and CI workflow
  ``.github/workflows/dataset-name-lint.yml`` in
  ``protea-reranker-lab``.
