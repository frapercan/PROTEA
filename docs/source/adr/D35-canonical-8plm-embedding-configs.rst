ADR-D35: Canonical 8-PLM embedding config IDs and orphan classification
========================================================================

:Status: Accepted
:Date: 2026-05-18

Context
-------

The LAFA KNN-8PLM ensemble (``apps/lafa_knn_8plm/plm_encoders.py``,
``PLM_SPECS`` tuple) defines eight protein language models as the
production ensemble. Each PLM must have a corresponding
``EmbeddingConfig`` row in the PROTEA database before embeddings can be
stored and retrieved.

On 2026-05-18 a bioinfo-quick dispatch agent sent eight
``compute_embeddings`` jobs without first auditing the existing
``EmbeddingConfig`` table. The table already contained 9 rows (11 after
the dispatch created 2 new ones), and most of the dispatched work was
redundant. The 8 jobs were immediately cancelled (all returned HTTP 200
``cancelled``; job IDs listed in the References section).

This ADR pins the canonical ``embedding_config_id`` for each of the 8
ensemble members, classifies the 3 non-canonical (orphan) rows, and
records the protocol divergences that require attention before any future
hydration dispatch.

Decision
--------

**Canonical 8-PLM ``embedding_config_id`` table** (as of 2026-05-18):

.. list-table:: Canonical 8-PLM embedding config mapping
   :header-rows: 1
   :widths: 14 38 14 14

   * - PLM key
     - HF / SDK checkpoint
     - embedding_config_id
     - embedding_count
   * - esm2_150m
     - facebook/esm2_t30_150M_UR50D
     - ``500a0c59-be09-424d-9d51-b7997629c95a``
     - 66 432
   * - esm2_650m
     - facebook/esm2_t33_650M_UR50D
     - ``c2e9dda3-e505-4170-b50d-435a451761ac``
     - 527 422
   * - esm2_3b
     - facebook/esm2_t36_3B_UR50D
     - ``55e43f1c-1a3b-4b1d-88c0-26b433f5f673``
     - 551 918
   * - prot_t5
     - Rostlab/prot_t5_xl_half_uniref50-enc
     - ``084943c6-fec1-441d-bdc5-63b0268ada1b``
     - 0
   * - prostt5
     - Rostlab/ProstT5
     - ``c0ae5b69-d6dc-41cf-a711-1739d3d2e170``
     - 527 424
   * - ankh_base
     - ElnaggarLab/ankh-base
     - ``08234f06-ba76-4d7d-aaec-ae601096b4fa``
     - 527 424
   * - ankh_large
     - ElnaggarLab/ankh-large
     - ``238f79b1-3068-4c6f-9013-5cc52b4f662b``
     - 527 424
   * - esmc_600m
     - esmc_600m (EvolutionaryScale SDK)
     - ``2bf1e753-022f-44b8-a131-9a90acb4024e``
     - 527 424

Match notes:

- **esm2_150m (500a0c59):** exact match. Newly created on 2026-05-18 by
  the dispatch agent. Has 66 432 embeddings (partial hydration completed
  before the cancel arrived). Needs a future targeted hydration to reach
  the full reference-set count (~527 k).
- **esm2_650m (c2e9dda3):** exact match. Full hydration present.
- **esm2_3b (55e43f1c):** exact match. Largest embedding_count (551 918,
  above the 527 k baseline due to the chunking-enabled config). Full
  hydration present.
- **prot_t5 (084943c6):** exact checkpoint match (half-precision encoder,
  ``Rostlab/prot_t5_xl_half_uniref50-enc``). Newly created on 2026-05-18.
  embedding_count = 0. Hydration job was cancelled; needs a fresh dispatch
  to populate.
- **prostt5 (c0ae5b69):** exact match. Full hydration present.
- **ankh_base (08234f06):** exact match (layer_indices=[0], normalize=false).
  Full hydration present.
- **ankh_large (238f79b1):** exact match. Full hydration present.
- **esmc_600m (2bf1e753):** exact match. Full hydration present.

**Protocol divergence flagged (prot_t5 vs orphan db4db5ed):** The old
full-precision ProtT5 config (``db4db5ed``, checkpoint
``Rostlab/prot_t5_xl_uniref50``, no ``_half_``) has 527 424 embeddings
and is classified as superseded (see orphan table below). The canonical
config ``084943c6`` uses the half-precision checkpoint and currently has
0 embeddings. A targeted ``compute_embeddings`` dispatch against
``084943c6`` is required before the 8-PLM ensemble can run end-to-end.

**Orphan / non-canonical rows:**

.. list-table:: Orphan EmbeddingConfig rows (non-canonical)
   :header-rows: 1
   :widths: 38 20 12 30

   * - embedding_config_id
     - model_name
     - embedding_count
     - classification
   * - ``db4db5ed-e34a-47af-a9ab-cc0f230b0a8c``
     - Rostlab/prot_t5_xl_uniref50 (full precision)
     - 527 424
     - **superseded** (full-precision ProtT5; canonical is half-precision
       ``084943c6``; retain for traceability)
   * - ``c2868c1a-0966-47a0-9941-2213fe6d22fa``
     - ElnaggarLab/ankh-base (layers 0,1,2; normalize_residues=true)
     - 527 432
     - **experimental** (multi-layer ablation config; not in 8-PLM protocol)
   * - ``c85d1afe-3f49-4ead-82d9-faaa6efe7a2c``
     - esmc_300m
     - 527 855
     - **baseline** (ESMC-300M single-PLM baseline; not in 8-PLM ensemble)

**Recommended follow-up actions** (deferred to a future user-led cleanup
slice, not executed in this ADR):

1. Label ``db4db5ed`` as ``superseded-prot-t5-full-precision`` in its
   description field. Do not delete; the 527 k embeddings are a valid
   reference for ablation comparisons.
2. Label ``c2868c1a`` as ``experimental-ankh-base-multilayer`` in its
   description field. Do not delete; the multi-layer ablation is a
   distinct research cell.
3. Label ``c85d1afe`` as ``baseline-esmc-300m`` in its description field.
   Do not delete; single-PLM baseline row is needed for the ESMC-300M
   vs ESMC-600M comparison in chapter 6.
4. Dispatch a fresh ``compute_embeddings`` job targeting config
   ``084943c6`` (prot_t5 half-precision) to bring its embedding_count
   from 0 to the full reference-set size. This is a prerequisite for
   any 8-PLM ensemble inference run.
5. Investigate why ``esm2_150m`` (``500a0c59``) has only 66 432 embeddings.
   A targeted top-up dispatch is needed to reach the full reference-set
   count before the config can be used in 8-PLM KNN queries.

Consequences
------------

**Positive**

- Canonical IDs are pinned in version control; future dispatch agents
  must audit against this table before creating new configs.
- Orphan rows are classified and labelled, eliminating ambiguity about
  which row to use for each PLM.
- The cancelled-job incident is documented; the GPU is freed immediately.
- Two newly created configs (``500a0c59``, ``084943c6``) are identified as
  the correct canonical rows, preventing future duplication.

**Negative**

- ``prot_t5`` (``084943c6``) has 0 embeddings. A hydration dispatch is
  required before the full 8-PLM ensemble can be used in production.
- ``esm2_150m`` (``500a0c59``) has a partial hydration (66 432 rows), also
  requiring a top-up before full production use.
- Two dispatch jobs remain partially incomplete despite the cancellation;
  workers that had already started processing may have written partial
  rows before detecting the cancel signal.

**Neutral**

- The dispatch agent pattern (dispatch without auditing first) must be
  updated: see recommended action to add a pre-dispatch audit step in
  the ``bioinfo-quick`` conductor flow.
- The 3 full-hydration canonical configs (prostt5, ankh_base, ankh_large,
  esmc_600m) plus esm2_650m and esm2_3b are production-ready today.

References
----------

- ``apps/lafa_knn_8plm/plm_encoders.py`` (``PLM_SPECS`` tuple, canonical
  8-PLM checkpoint registry).
- Cancelled job IDs (2026-05-18):

  - esm2_150m: ``05de343e-be81-4215-b79b-01dfb9cc394a``
  - esm2_650m: ``af48df6c-6ff7-4a1f-b2f7-9bace15ea4e6``
  - esm2_3b: ``76e24a5b-5be6-489a-8610-bebb634e02e3``
  - prot_t5 half: ``c6f5f812-91a2-4f94-9dd2-45db1f1408ad``
  - prostt5: ``387542b3-6579-4a1f-b700-1be7f2ae5b07``
  - ankh_base: ``ce5ada6f-4fd6-4f60-9417-e5794189c951``
  - ankh_large: ``23cba402-f6c1-4ad1-a3be-5830cffc8e11``
  - esmc_600m: ``74da4ab9-ed14-4007-b855-323a9fdd9ffa``

- Memory entry ``project_canonical_8plm_embedding_configs``
  (canonical ID table + orphan list, full record with protocol notes).
- ADR D09 (``009-cancellation-nack-before-dispatch.rst``) for the
  cancel endpoint behaviour relied on in Task A.
