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

**3. The stage-1 population is fixed and published by sha.** It is the
experimental bank at the VALID cutoff together with the VALID deltas:

- bank: proteins with a non-``NOT`` annotation in GOA 220 (``ba9f57f7``) whose
  evidence code is in ``protea.core.evaluation._EXP_CODES`` (13 GO codes plus
  their 13 ECO equivalents). That is 85,989 proteins and 84,702 sequences.
- deltas: VALID-A ``fd0314d8`` (each side under its native DAG, the declared
  frame) and VALID-B ``43b6b9e7`` (both sides under the t0 pivot, the
  robustness check).
- union: **88,404 accessions, 87,081 unique sequences**, against 528,545 in
  the whole store. The canonical list (``sort -u`` in C locale, one accession
  per line, trailing newline) has sha256
  ``696496893bdc1450d9dd052026aec20b36ffcbe6b4762f57d584a4d9de375a39``. The
  same sha comes out with the 13 GO codes alone, because GOA 220 carries no
  annotation in ECO form.
- It travels as a ``QuerySet``, never as ``accessions``. An ``IN`` list binds
  one parameter per accession, and Postgres caps a statement at 65,535, so an
  ``accessions`` payload of this size is accepted, queued and then fails at
  execution. Without either field, ``compute_embeddings`` embeds the entire
  ``sequence`` table.
- The FASTA is written by ``export_evaluation_targets.format_fasta`` (WRAP 60,
  canonical residues, bare-accession headers), so its bytes are those of the
  evaluation targets by construction. FASTA sha256
  ``3e5c3e7db61b246f28f2f303020cd39584a8fa15779ad1fcdbc9bdf083e7b2f7``.
- ``predict_go_terms`` for stage 1 sets ``donor_policy.evidence_codes`` to
  ``_EXP_CODES``. Donors carrying a ``NOT`` qualifier are already excluded by
  the loaders.

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

- Metric: KNN-only ``f_micro_w`` on VALID-A (``fd0314d8``), repeated on VALID-B.
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
- Stage 1 costs about 87k forward passes per PLM, not 528k.
- Every stage-1 number is read in one software regime, and that regime is
  written down where both machines read it.

What this record does not settle
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- PLM-independent neighbourhood strata. The neighbourhood axes of
  ``protea.core.strata`` are read from each run's own retrieval, so a protein
  can fall in different strata under different PLMs. Their donor evidence is
  also not aspect-specific. A sequence-search neighbourhood (MMseqs2, per
  aspect, against the bank at the cutoff) is the proposed fix.
- Three definitions of "experimental" coexist: ``evaluation._EXP_CODES`` (26),
  a duplicate in ``build_go_cooccurrence`` and a 6-code set in
  ``proteins_stats``.
- ``query_set`` carries no content hash, so a QuerySet is referable but not
  verifiable from inside the system. The shas above make it checkable from
  outside.

References
~~~~~~~~~~

- Migration ``alembic/versions/3cd5f76d282f_stage1_standard_configs_replace_rung1.py``
- ``tests/test_stage1_standard_configs.py``
- ADR-D35 (roster), ADR-D40 (temporal protocol), ADR-D46 (IA as a corpus
  artifact; the VALID IA is ``4346e676``)
- ``agent-farm/plans/DECLARED-REVISION.txt`` and
  ``agent-farm/scripts/services/protea-node-sync.sh``
