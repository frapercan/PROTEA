ADR-D48: Stage-1 standard embedding configs replace the rung-1 seeds
=====================================================================

:Status: Accepted
:Date: 2026-10-05
:Author: Francisco Miguel Pérez Canales
:Phase: thesis campaign, stage 1 (transversal PLM ablation)

Context: the registry described a campaign that no longer exists
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The eight ``EmbeddingConfig`` rows in the registry were seeded by three
migrations in July 2026 (``e7a1c4f9b2d6``, ``a1c9e4b7d2f8``, ``f2b8d1c6a94e``)
for the rung-1 ladder, which the experiment graph has since retired. They were
wrong for the campaign about to run, in four separate ways:

- The roster was chosen to fit an 8 GB RTX 4060. ESM-2 3B and ESM-2 150M were
  left out for that reason. The compute node has a 12 GB RTX 3060.
- Two rows were never comparable members of a PLM axis. ``esm2_8m`` was a
  mechanism cell, a smoke test of the pipeline. ``protst`` is a 512-d
  text-aligned projection whose backend honours only ``normalize``, so its
  recorded ``max_length=2048`` described nothing it did.
- ``max_length=2048`` ran ESM-2 and ESM-C beyond the roughly 1024-token context
  they were trained on.
- The roster contradicted ADR-D35, which names the eight PLMs of the campaign.

None of them had ever been used. On 2026-10-05 no row of ``sequence_embedding``,
``prediction_set``, ``reranker_model``, ``dataset`` or
``embedding_config.derived_from_embedding_config_id`` referenced any of them.
That window closes with the first embedding written, smoke tests included.

Decision
~~~~~~~~

**1. Stage 1 is a transversal ablation on one standard recipe.** Its purpose is
to choose two PLMs: a fast one for the research line and a heavy one for a later
run. All eight ADR-D35 PLMs run on the same recipe: last layer
(``layer_indices=[0]``), ``layer_agg=mean``, ``pooling=mean``,
``normalize=True``, ``normalize_residues=False``, ``max_length=1022``,
``use_chunking=False``, ``embedding_scale=1.0``.

.. list-table:: Stage-1 configs (ids derived by ``protea.core.embedding_identity``)
   :header-rows: 1
   :widths: 12 34 8 6 8 32

   * - key
     - checkpoint
     - backend
     - dim
     - CUDA dtype
     - embedding_config_id
   * - esm2_150m
     - facebook/esm2_t30_150M_UR50D
     - esm
     - 640
     - fp16
     - ``336a2bcb-d35e-50c5-ba7e-9ae35f1a1dcb``
   * - esm2_650m
     - facebook/esm2_t33_650M_UR50D
     - esm
     - 1280
     - fp16
     - ``9c4ea46b-c8c9-5cdb-af36-14df75605dd2``
   * - esm2_3b
     - facebook/esm2_t36_3B_UR50D
     - esm
     - 2560
     - fp16
     - ``6fef8557-ca97-55c4-b75a-e9fca7d8dd93``
   * - esmc_600m
     - esmc_600m
     - esm3c
     - 1152
     - fp16
     - ``38e079cd-0613-5ea7-b6dc-f70f0c419731``
   * - prot_t5
     - Rostlab/prot_t5_xl_half_uniref50-enc
     - t5
     - 1024
     - fp16
     - ``7d409774-ec37-5606-a90c-95646ce813ac``
   * - prostt5
     - Rostlab/ProstT5
     - t5
     - 1024
     - fp16
     - ``cd00a743-8215-5ce9-8ad3-2846d9f590db``
   * - ankh_base
     - ElnaggarLab/ankh-base
     - ankh
     - 768
     - bf16
     - ``e7ba37da-c18d-5261-b5b4-49a3093d9d4d``
   * - ankh_large
     - ElnaggarLab/ankh-large
     - ankh
     - 1536
     - bf16
     - ``2b27f22a-ed4c-50a8-9068-ec18be8d4a46``

``max_length`` counts tokens including special tokens on the esm, t5 and ankh
backends and residues on esm3c. The residues seen are therefore 1020 for ESM-2
and ProstT5 (``<AA2fold>`` prefix), 1021 for ProtT5 and Ankh, and 1022 for
ESM-C. This is recorded, not corrected: the difference touches only sequences
that are truncated anyway. ``param_count`` is seeded NULL and filled by
``count_backend_parameters``, which counts the module each backend executes.

**2. One migration retires and seeds** (``3cd5f76d282f``). An empty
``embedding_config`` table makes ``/annotate`` mint a default config, so the
DELETE and the INSERT share one transaction. A retired row that something
references turns the upgrade into a refusal: a cascade would be a
reconciliation, not a replacement. The downgrade restores the rung-1 rows
exactly, ``param_count`` and ``created_at`` included.
``tests/test_stage1_standard_configs.py`` ties the restated derivation to the
application's, pins the roster to ``apps/lafa_knn_8plm`` ``PLM_SPECS``, and runs
upgrade, downgrade and the guard against a live Postgres.

**3. Stage 1 embeds the whole store once, and the donor bank is an axis.**

- Population: every row of ``sequence`` at dispatch. The store is being
  rebuilt to a GAF-derived universe (see below), so the count is recorded at
  dispatch rather than here. Before the rebuild it held 528,545 unique
  sequences behind 617,103 proteins. The pass is dispatched with neither
  ``query_set_id`` nor ``accessions``, which is the code path that selects every
  sequence. It is written here as the decision so that it is not read as the
  defect it would be anywhere else. At dispatch, the count and the sha256 of the
  sorted sequence hashes are recorded, and a pass is closed only when its
  coverage equals that set.
- Why the whole store: embeddings are keyed by ``(sequence, config)`` and
  ``compute_embeddings`` skips what already exists, so one pass serves VALID,
  TEST, the TRAIN windows and SF-JEPA. A narrower population now would be a
  second pass later. It is also a superset of both donor banks below, so the
  bank question costs no embedding.
- A narrower population, when one is needed, travels as a ``QuerySet``, never
  as ``accessions``. An ``IN`` list binds one parameter per accession and
  Postgres caps a statement at 65,535, so a large ``accessions`` payload is
  accepted, queued and then fails at execution.
- **Donor bank, an axis with two levels**, applied in the ``predict_go_terms``
  payload. No extra embedding is needed:

  - *permissive*: every annotation in the cutoff release (GOA 220 for VALID),
    with ``donor_policy`` unset;
  - *experimental*: evidence in ``protea.core.evaluation._EXP_CODES`` (13 GO
    codes plus their 13 ECO equivalents), with ``donor_policy.evidence_codes``
    set to ``_EXP_CODES``.

  On the pre-rebuild store the two banks at GOA 220 were 556,468 and 85,989
  proteins. Their sizes on the rebuilt store are recorded with the run.

  Donors carrying a ``NOT`` qualifier are excluded by the loaders in both.
- **Prior, declared as a prior and not as a measurement.** The previous
  campaign is remembered as favouring the permissive bank in every panel its
  note lists, most strongly in NK. Its rows were destroyed on 2026-09-14, and the note
  carrying the figures is undated, so it is not citable here. The mechanism is
  plausible: restricting to experimental evidence keeps about one donor in six,
  and the ones it keeps are well-studied proteins, so the nearest donor sits farther away
  and transfers more terms. Stage 1 measures it, and its ``evaluation_result``
  ids become the citable source.

**4. The software regime of the compute node is part of the result.** It moves
stored coordinates without moving any id. ``protea-node-sync`` governs only the
seven git siblings of the declared coordinator. The node therefore pins the
numeric path by hand and records it:

- transformers 4.48.1 and tokenizers 0.21.4 (from the lock). In this version
  ESM and T5 have no SDPA or flash path, so attention is eager. Newer
  transformers routes them through SDPA, which moved 88% of fp16 coordinates in
  a measurement made for protea-backends#45. This governs seven of the eight
  PLMs.
- esm 3.2.1.post1 (from the lock) and torch 2.11.0+cu130, pinned explicitly:
  the lock has no CUDA build of torch at all. torch governs ESM-C, whose forward
  pass does not go through transformers. ``flash_attn`` is not installed.
- esm declares ``torchtext``. The lock's torchtext 0.18.0 has no wheel for the
  node's Python 3.14.4, and esm never imports it. This divergence is accepted.
- The server runs Python 3.12.13, so it cannot reproduce node vectors byte for
  byte even with identical packages.
- The declaration stays frozen for the whole of stage 1. A jump that carries a
  migration must set ``coordinator`` and ``schema-applied`` to the same new sha,
  after the server has applied it.
- **Regime fingerprint.** 16 fixed sequences, stratified by length and
  including the longest in the store (about 36k residues), are embedded by
  every stage-1 config one sequence per forward pass (batch 1, no padding).
  Shape, dtype and sha256 are recorded per vector. The fingerprint is computed
  twice in a row to establish that byte equality is attainable on this GPU in
  half precision; if it is not, the comparison falls back to a numeric
  threshold. It is then compared before the first pass and after the last.
  ``pip freeze`` is kept alongside. Production passes run at ``batch_size=1``
  for the same reason.

**5. The selection rule is fixed before anything is measured.**

The rule has two steps, read in order.

- **Step 1, bank.** Metric, estimand and uncertainty as in step 2. For each
  PLM, take the paired difference permissive minus experimental, then its mean
  over the eight PLMs. The experimental bank is chosen only if the 95% interval
  of that mean lies entirely below -0.02, that is, only if it is better by more
  than the margin. Otherwise the permissive bank, the prior, is kept. All
  sixteen PLM by bank cells are published, so an interaction is visible rather
  than averaged away.
- **Step 2, PLM**, within the bank chosen in step 1:

  - Metric: KNN-only ``f_micro_w`` on VALID-A (220 -> 227, each side under its
    native DAG, the declared frame), repeated on VALID-B (both sides under the
    t0 pivot). Both are rebuilt after the binding rule of PROTEA#976 and the
    universe rebuild, and their ids are recorded with the run.
  - Estimand: the flat mean of the nine category by aspect cells. That is how LAFA
    reports, and it keeps NK, the frontier of the project, from being outvoted by
    PK, which holds 79.9% of the VALID-A proteins.
  - Uncertainty: a paired bootstrap stratified by cell, resampling proteins
    within each cell (``compare_paired_panels`` machinery). No sigma constant
    from outside the record is used.
  - Equivalence: a PLM is equivalent to the best when the lower bound of the 95%
    interval of its difference exceeds -0.02. The 0.02 is a **practical margin**
    (``_TARGET_EFFECT`` in ``_graph_panels.py``), not a derived quantity.
    Projected from configuration-class sigmas, the minimum detectable effect of
    the flat mean is about 0.0045, and about 0.0136 at three times those sigmas.
    The variance of the aggregate is dominated by the small MFO cells (LK.MFO
    and NK.MFO, about 58%), so all nine per-cell intervals are published with
    it.
  - **heavy** = the highest ``f_micro_w``. **fast** = the cheapest model (GPU
    hours per pass, then dimensions) that is equivalent to the best; if that is
    the heavy one, the next model on the cost frontier.

Consequences
~~~~~~~~~~~~

- The registry matches ADR-D35 and the campaign. The ids of ADR-D35's May
  table are not revived: their recipes differed (ESM at 1024 tokens, an
  un-normalised ankh-base, a chunked esm2_3b). ADR-D35 keeps the roster; this
  record replaces the ids.
- Stage 1 costs one forward pass per sequence of the rebuilt store and PLM,
  once. Later windows and SF-JEPA reuse them, and the bank axis adds prediction
  sets, not embeddings.
- Every stage-1 number is read in one software regime, and that regime is
  written down where both machines read it.

What this record does not settle
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- The store it runs on. On 2026-10-05 the protein universe is being rebuilt
  from the GAF history rather than from a present-day UniProt query: the
  proteins with evidence in the 13 LAFA-regime codes in any release, any
  taxonomy, isoforms kept. A present-day query would bias the historical
  windows, because a protein that had evidence in 2018 and lost it would not
  appear. The GAFs are reloaded after the universe, because the foreign key
  silently drops every accession not in ``protein``. Every count in this
  record that predates the rebuild is labelled as such.
- The MFO ground truth. PROTEA#976 drops MFO from proteins whose direct F
  annotations are all ``GO:0005515`` (protein binding) or its descendants,
  which is about 31% of the proteins with any MFO in GOA 220. The VALID
  evaluation sets are rebuilt after it, and three of the nine cells the
  selection rule averages are MFO.
- PLM-independent neighbourhood strata. The neighbourhood axes of
  ``protea.core.strata`` are read from each run's own retrieval, so a protein
  can fall in different strata under different PLMs. Their donor evidence is
  also not aspect-specific. A sequence-search neighbourhood (MMseqs2, per
  aspect, against the bank at the cutoff) is the proposed fix.
- Three definitions of "experimental" coexist: ``evaluation._EXP_CODES`` (26),
  a duplicate in ``build_go_cooccurrence`` and a 6-code set in
  ``proteins_stats``.
- ``query_set`` carries no content hash, so a QuerySet is referable but not
  verifiable from inside the system. Any narrower population used later needs
  its sha recorded outside, as section 3 does for the whole store.

References
~~~~~~~~~~

- Migration ``alembic/versions/3cd5f76d282f_stage1_standard_configs_replace_rung1.py``
- ``tests/test_stage1_standard_configs.py``
- ADR-D35 (roster), ADR-D40 (temporal protocol), ADR-D46 (IA as a corpus
  artifact; the VALID IA is recomputed on the rebuilt corpus and recorded with
  the run)
- ``agent-farm/plans/DECLARED-REVISION.txt`` and
  ``agent-farm/scripts/services/protea-node-sync.sh``
