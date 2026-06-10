ADR-D38: Defer neural-head champion; pivot to curated dataset packaging
=======================================================================

:Status: Accepted
:Date: 2026-05-25
:Author: Francisco Miguel Pérez Canales
:Phase: F-DATA-PACK

Context
~~~~~~~

By 2026-05-24 the reranker track had produced a publishable LightGBM champion
(three-seed replication labelled ``binary-multiseed`` in the lab run log,
axis tuple below): NK+LK cafaeval Fmax **0.7291 ± 0.0028** on
``bench-v1-K5-v226-lineage-prostt5`` (NK+LK CIs strictly positive at 95%,
LB.3 paired bootstrap N=10000).

.. code-block:: text

   plm=prostt5  k=5  rr=binary  feat=generalist
   eval=v226    prop=lineage    ens=none

The champion was designated for generalisation evaluation on the v226 to
v230 window across all 8 canonical PLMs (FARM-EXP.13, 13 export jobs in
flight as of this decision).

In parallel, a deep-learning alternative was explored: a frozen ESMC-300M
neural head trained on a binary NK+LK task over 84k proteins x 10 epochs.
The run was launched 2026-05-24 at approximately 15:28 UTC
(``~/Thesis2/protea-neural-head/``, gitignored scaffold, 40 tests passing,
95.6% coverage). Its internal Fmax estimate at epoch-end is 0.119 (cafaeval
normalisation artefact; comparable 0.7291 is not directly attainable from
a first-run scaffold without significant tuning).

Three factors converged to make neural-head continuation unattractive:

1. **No obvious path to surpass the binary-label LightGBM champion on the
   v226 to v230 window.** The champion combines 9 aspect-stable generalist
   features (LM.3 feature-importance analysis, 2026-05-18). Attention-based
   features score 0 gain in the champion; the neural head would need to
   capture something qualitatively new rather than re-learn the same signal
   through a different architecture.

2. **Opportunity cost: multi-PLM grid already produced.** FARM-EXP.13
   is materialising 24 (PLM x K) datasets across 8 PLMs and K in {3, 5,
   10}. Packaging that grid as FAIR-compliant, documented research datasets
   (manifest validator, per-dataset READMEs, dataset cards, Zenodo/HuggingFace
   upload) is a concrete, bounded deliverable that directly supports
   reproducibility requirements for thesis chapter 6 and any external
   reviewer. The neural-head track does not produce this artefact and would
   consume the same GPU time budget.

3. **Thesis timeline.** The thesis chapter 6 narrative fits the
   LightGBM track cleanly: methodological champion is established,
   ablated, and replicated. Introducing a DL competitor that is neither
   trained to convergence nor validated on the same benchmarks at
   submission time would weaken the chapter rather than strengthen it.

Decision
~~~~~~~~

1. **Defer FARM-NEURAL.* slate.** All neural-head experiment slices are
   deferred indefinitely. The prerequisite for resumption is a completed
   v226 to v230 Fmax gap analysis showing that the LightGBM ceiling is at
   least 0.005 Fmax units below a retrained neural head on the same
   benchmark (comparable to the GeOKG NO-GO precondition, 2026-05-17).

2. **Binary-label LightGBM remains methodological champion.** The
   three-seed replication run (lab run log key ``binary-multiseed``, axis
   tuple: plm=prostt5, k=5, rr=binary, feat=generalist, eval=v226,
   prop=lineage, ens=none) is the champion reported in thesis chapter 6.
   The three-seed replication result (0.7291 ± 0.0028) is the publishable
   number.

3. **Materialise F-DATA-PACK loop.** The next active loop after
   FARM-EXP.13 completes is F-DATA-PACK, which delivers:

   - A manifest validator (schema + content hash checks) wired into the
     PROTEA export pipeline.
   - Per-dataset READMEs and dataset cards following the HuggingFace
     ``DatasetCard`` and Zenodo metadata schemas.
   - A FAIR/coverage documentation page in the Sphinx docs
     (``docs/source/datasets/``).
   - Zenodo and/or HuggingFace Hub upload of the 24-dataset grid produced
     by FARM-EXP.13.

4. **Neural-head scaffold stays gitignored.** The
   ``~/Thesis2/protea-neural-head/`` directory is not committed to any
   repo. The scaffold and the 10-epoch ESMC-300M run are preserved locally
   as exploratory artefacts. They are not referenced from any PROTEA
   registry row, any ``Dataset`` row, or any thesis chapter prose.

5. **Thesis ch6 framing.** Chapter 6 acknowledges the neural-head
   exploration as a boundary experiment: it was run, it did not surpass
   the LightGBM champion within the thesis timeline, and the decision to
   ship the curated dataset grid instead is explicitly justified by the
   opportunity-cost argument in this ADR. No DL results are presented in
   chapter 6; the neural-head section is bounded to a single paragraph
   in the "limitations and future work" section.

Consequences
~~~~~~~~~~~~

**Positive**

- Thesis chapter 6 has a single, cleanly validated champion with
  three-seed replication and paired bootstrap statistics. No competing
  DL track dilutes the narrative.
- F-DATA-PACK produces a tangible, externally reproducible artefact
  (24 FAIR datasets) before the thesis submission deadline, independently
  of any further training runs.
- GPU budget freed by deferring FARM-NEURAL.* is available to complete
  FARM-EXP.13 (13 jobs in flight) without contention.
- The manifest validator and dataset cards benefit all future consumers
  of the reranker grid, not only the thesis.

**Negative**

- The ESMC-300M run launched 2026-05-24 ran to completion but its
  results are not incorporated into the thesis. Approximately 10 GPU-hours
  are sunk cost.
- Any future reviewer who wants to evaluate the neural-head track must
  retrain from the published dataset grid; the 2026-05-24 checkpoint is
  not archived in a public artefact store.
- FARM-NEURAL.* slice numbers are reserved but empty in the plan catalog;
  a future conductor session must not interpret them as blocked (they are
  deferred, not failed).

**Neutral**

- The ``bench-v1-K5-v226-lineage-prostt5`` champion row and its three-seed
  metrics (binary-label LightGBM, 0.7291 ± 0.0028) remain unchanged. This
  ADR does not alter any existing dataset registry row, ``RerankerModel``
  row, or cafaeval result.
- The GeOKG NO-GO precondition (ADR-internal, 2026-05-17) and the
  neural-head resumption precondition stated in decision point 1 above
  are structurally parallel: both require a demonstrated Fmax gap before
  a new modality is promoted.

Supersedes
~~~~~~~~~~

Nothing. No prior ADR addressed the DL direction for PROTEA. This record is
the first formal decision on the neural-head track.

References
~~~~~~~~~~

- Memory entry ``project_v27_binary_multiseed_2026_05_18``: three-seed
  replication result for the binary-label LightGBM champion
  (NK+LK 0.7291 ± 0.0028, 5/6 cells sig at 95%).
- Memory entry ``project_lb3_paired_ci_2026_05_18``: paired bootstrap
  CI table (N=10000, 6/6 cells strictly positive vs KNN baseline).
- Memory entry ``project_lm3_feature_importance_2026_05_18``: 9
  aspect-stable generalist features dominate; alignment and taxonomy score
  0 gain; attention features not in champion set.
- Memory entry ``project_neural_head_full_run_2026_05_24``: ESMC-300M
  10-epoch run, ~01:30 UTC ETA, internal Fmax 0.119 (cafaeval
  normalisation difference from published 0.7291).
- Memory entry ``project_multi_plm_v226_sweep_plan``: 24-dataset grid
  (FARM-EXP.13, 8 PLMs x K in {3, 5, 10}).
- Memory entry ``project_geokg_nogo_2026_05_17``: parallel precedent for
  deferring a competing modality on explicit Fmax-gap precondition.
- Memory entry ``dl-postponed-2026-05-25``: user decision record cited
  as authoritative trigger for this ADR.
- ADR D34 (selective rerank resurrection): LightGBM champion design
  decisions.
- ADR D35 (canonical 8-PLM embedding config IDs): PLM registry.
- ADR D36 (PLM axis explicit in dataset naming): dataset naming
  convention for the 24-dataset grid.
- ``~/Thesis2/protea-neural-head/``: gitignored neural-head scaffold
  (exploratory, not wired to any PROTEA registry).
- Slice catalog ``agent-farm/plans/farm-platform/PLAN.md``:
  FARM-EXP.13 (multi-PLM export) and F-DATA-PACK (dataset packaging
  loop).
