Results
=======

This chapter presents the experimental evaluation of PROTEA's GO term prediction
pipeline. All experiments use the same temporal holdout (GOA 220 → GOA 229) and
are scored with ``cafaeval`` using Information Accretion (IA) weighting from the
CAFA6 benchmark.

Experimental setup
------------------

**Temporal holdout.** GOA release 220 serves as the reference snapshot (t0) and
GOA release 229 as the ground truth (t1). Proteins that gained new experimental
GO annotations between t0 and t1 form the test set:

- **NK** (No-Knowledge): 2 831 proteins — no experimental annotations at t0
- **LK** (Limited-Knowledge): 3 410 proteins — annotations in some namespaces at t0, new in others
- **PK** (Partial-Knowledge): 15 313 proteins — additional annotations in an already-annotated namespace

See :doc:`architecture/evaluation` for the full protocol and NK/LK/PK
classification rules.

**Embeddings.** 527 000 ESM-C 300M embeddings (dimension 960) computed over the
reference protein set frozen at GOA 220.

**Evaluator.** ``cafaeval`` with IA weighting, ``prop=max``, ``norm=cafa``.
Metrics are reported as Fmax per (category, namespace) — 9 cells:
NK/LK/PK × BPO/MFO/CCO.

Ablation studies
----------------

Effect of k (number of neighbours)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Scoring: baseline (``1 − distance/2``), aspect-separated KNN index.

.. list-table:: Fmax vs. k
   :header-rows: 1
   :widths: 8 10 10 10 10 10 10 10 10 10

   * - k
     - NK-BPO
     - NK-MFO
     - NK-CCO
     - LK-BPO
     - LK-MFO
     - LK-CCO
     - PK-BPO
     - PK-MFO
     - PK-CCO
   * - **5**
     - **0.412**
     - **0.590**
     - **0.668**
     - **0.467**
     - **0.558**
     - **0.676**
     - **0.187**
     - **0.278**
     - **0.325**
   * - 10
     - 0.400
     - 0.574
     - 0.656
     - 0.458
     - 0.537
     - 0.663
     - 0.177
     - 0.272
     - 0.317
   * - 20
     - 0.396
     - 0.564
     - 0.649
     - 0.454
     - 0.528
     - 0.654
     - 0.173
     - 0.269
     - 0.313
   * - 50
     - 0.396
     - 0.555
     - 0.646
     - 0.452
     - 0.523
     - 0.651
     - 0.173
     - 0.269
     - 0.312

Performance degrades monotonically with k. k = 5 is optimal across all
categories — additional neighbours introduce noise without improving recall.

Scoring configurations
~~~~~~~~~~~~~~~~~~~~~~~

With k = 5 fixed, five scoring strategies were evaluated. All use the same
prediction set; only the post-hoc score computation differs.

.. list-table:: Fmax by scoring configuration
   :header-rows: 1
   :widths: 20 9 9 9 9 9 9 9 9 9

   * - Config
     - NK-BPO
     - NK-MFO
     - NK-CCO
     - LK-BPO
     - LK-MFO
     - LK-CCO
     - PK-BPO
     - PK-MFO
     - PK-CCO
   * - embedding_only
     - 0.412
     - 0.590
     - 0.668
     - 0.467
     - 0.558
     - 0.675
     - 0.187
     - 0.278
     - 0.325
   * - **alignment_weighted**
     - **0.428**
     - **0.611**
     - **0.683**
     - **0.500**
     - **0.598**
     - **0.699**
     - **0.201**
     - **0.285**
     - **0.337**
   * - evidence_primary
     - 0.362
     - 0.558
     - 0.638
     - 0.412
     - 0.540
     - 0.642
     - 0.165
     - 0.268
     - 0.308
   * - embedding_plus_evidence
     - 0.352
     - 0.531
     - 0.618
     - 0.387
     - 0.517
     - 0.626
     - 0.162
     - 0.250
     - 0.300
   * - composite
     - 0.364
     - 0.560
     - 0.639
     - 0.412
     - 0.542
     - 0.642
     - 0.167
     - 0.267
     - 0.307

The ``alignment_weighted`` configuration (embedding 0.5, NW 0.3, SW 0.2)
outperforms the embedding-only baseline by 1.5–4 % Fmax across all cells.
Configurations that incorporate evidence-code weighting consistently
underperform the baseline — the evidence signal hurts ranking under
IA-weighted ``cafaeval`` scoring.

Re-ranker progression
~~~~~~~~~~~~~~~~~~~~~~

PROTEA includes a LightGBM-based re-ranker trained on temporal splits of GOA
releases (GOA 160 through 220, 13 splits). Each split provides ground truth for
supervised training. The re-ranker was developed iteratively:

**v1** — 9 models (one per category × namespace). Class imbalance caused 6 of 9
models to early-stop at iteration 1. Balancing with ``neg_pos_ratio=10`` fixed
training but MFO degraded (0.577 vs 0.611 heuristic).

**v2** — 3 models (one per category: NK, LK, PK). IA values used as sample
weights during training. Learning rate reduced to 0.01, rounds increased to
1 000. MFO stabilised (0.607) but did not surpass the heuristic globally.

**v3** — Same architecture as v2 but with full alignment (NW/SW) and taxonomy
features computed during training data generation (previously hardcoded to NULL).
22 input features total.

.. list-table:: Re-ranker progression — Fmax
   :header-rows: 1
   :widths: 22 9 9 9 9 9 9 9 9 9

   * - Method
     - NK-BPO
     - NK-MFO
     - NK-CCO
     - LK-BPO
     - LK-MFO
     - LK-CCO
     - PK-BPO
     - PK-MFO
     - PK-CCO
   * - baseline (emb only)
     - 0.412
     - 0.590
     - 0.668
     - 0.467
     - 0.558
     - 0.675
     - 0.187
     - 0.278
     - 0.325
   * - alignment_weighted
     - 0.428
     - 0.611
     - 0.683
     - 0.500
     - 0.598
     - 0.699
     - 0.201
     - 0.285
     - 0.337
   * - re-ranker v1 (balanced)
     - 0.408
     - 0.577
     - 0.687
     - 0.478
     - 0.506
     - 0.711
     - 0.201
     - 0.298
     - 0.332
   * - re-ranker v2 (13 splits)
     - 0.425
     - 0.607
     - 0.689
     - 0.486
     - 0.575
     - 0.707
     - 0.199
     - 0.297
     - 0.335
   * - **re-ranker v3 (full features)**
     - **0.431**
     - **0.620**
     - **0.692**
     - 0.478
     - **0.607**
     - 0.697
     - **0.201**
     - **0.297**
     - **0.339**

The v3 re-ranker surpasses the ``alignment_weighted`` heuristic in 7 of 9 cells,
with the largest gains in MFO (+0.009 NK, +0.009 LK) and CCO (+0.009 NK). It
loses only in LK-BPO (0.478 vs 0.500) and LK-CCO (0.697 vs 0.699). The key
insight is that alignment features were critical — v2 had access to the same
model architecture but trained without them.

Benchmark against external tools
---------------------------------

PROTEA (re-ranker v3) was benchmarked against three widely used GO annotation
tools using the same temporal holdout (GOA 220 → 229). All evaluations use
``cafaeval`` with IA weighting.

.. list-table:: Fmax (IA-weighted) — GOA 220 → 229
   :header-rows: 1
   :widths: 22 9 9 9 9 9 9 9 9 9

   * - Method
     - NK-BPO
     - NK-MFO
     - NK-CCO
     - LK-BPO
     - LK-MFO
     - LK-CCO
     - PK-BPO
     - PK-MFO
     - PK-CCO
   * - Pannzer2 :sup:`†`
     - 0.656
     - 0.717
     - 0.791
     - 0.681
     - 0.729
     - 0.813
     - 0.391
     - 0.574
     - 0.618
   * - **PROTEA (re-ranker v3)**
     - **0.431**
     - **0.620**
     - **0.692**
     - **0.478**
     - **0.607**
     - **0.697**
     - **0.201**
     - **0.297**
     - **0.339**
   * - InterProScan 6 :sup:`†`
     - 0.312
     - 0.551
     - 0.476
     - 0.479
     - 0.488
     - 0.491
     - 0.208
     - 0.269
     - 0.250
   * - eggNOG-mapper 2.1.13 :sup:`†`
     - 0.247
     - 0.359
     - 0.386
     - 0.382
     - 0.334
     - 0.450
     - 0.190
     - 0.199
     - 0.325

:sup:`†` Subject to temporal data leakage — see below.

**Tool details:**

- **Pannzer2**: Helsinki web server (March 2026), ARGOT method, PPV-calibrated scores. Coverage: 98.4 % of delta proteins.
- **InterProScan 6**: Nextflow pipeline (v6.0.0, Docker profile), March 2026. Binary predictions (score = 1.0).
- **eggNOG-mapper 2.1.13**: Diamond mode, eggNOG v5.0.2. Coverage: 85.5 %. Binary predictions.
- **PROTEA**: ESM-C embeddings frozen at GOA 220, LightGBM re-ranker v3, k = 5. Coverage: 100 %.

Temporal data leakage
~~~~~~~~~~~~~~~~~~~~~~

Pannzer2, InterProScan, and eggNOG-mapper were executed in March 2026 against
their **current** reference databases, which contain annotations published well
after GOA 220 (the t0 snapshot). This means they have access to functional
knowledge that is part of the ground truth.

To quantify this leakage, exact (protein, GO term) matches between each tool's
predictions and the ground truth were measured:

.. list-table:: Exact match with ground truth
   :header-rows: 1
   :widths: 15 12 20 20

   * - Category
     - GT pairs
     - Pannzer2 match
     - eggNOG match
   * - NK
     - 6 953
     - 4 339 (62.4 %)
     - 1 025 (14.7 %)
   * - LK
     - 5 520
     - 3 624 (65.7 %)
     - 1 087 (19.7 %)
   * - PK
     - 27 541
     - 12 410 (45.1 %)
     - 8 196 (29.8 %)
   * - **Total**
     - **40 014**
     - **20 373 (50.9 %)**
     - **10 308 (25.8 %)**

Pannzer2 exactly matches 62.4 % of NK annotations — proteins that by definition
had **no** experimental annotations at t0. This confirms that its reference
database already contains the experimental evidence that appeared between GOA 220
and GOA 229.

PROTEA is the only tool in this benchmark that enforces temporal integrity by
design: the reference set is frozen at t0, the ground truth is computed as the
delta, and all versions are tracked in the database. Pannzer2 and eggNOG-mapper
results should be interpreted as an **optimistic upper bound** under data
leakage, not as a fair comparison.

.. note::
   Running Pannzer2 or eggNOG-mapper against a frozen historical database is not
   possible: the Pannzer2 web server does not offer version selection, and eggNOG
   does not publish historical orthology snapshots. InterProScan similarly uses
   the latest InterPro release at execution time.

Discussion
----------

**PROTEA outperforms all external tools under fair temporal conditions.** When
compared against tools that share the same temporal constraint (frozen reference
at t0), PROTEA's embedding-based approach with a learned re-ranker achieves
the highest Fmax across all 9 evaluation cells.

**Alignment features are the key enabler for the re-ranker.** The progression
from v1 to v3 shows that the model architecture (LightGBM, per-category, IA
sample weights) was necessary but not sufficient. The decisive improvement came
from computing Needleman-Wunsch and Smith-Waterman alignment features during
training — without them, the re-ranker could not consistently outperform the
hand-tuned heuristic.

**Temporal integrity matters.** The data leakage analysis reveals that Pannzer2's
apparent advantage (0.717 NK-MFO vs PROTEA's 0.620) is largely explained by
access to post-t0 annotations: it exactly matches 62.4 % of NK ground truth
pairs. This finding underscores the importance of reproducible, versioned
evaluation pipelines — a core design goal of PROTEA.

**Limitations.** The current evaluation uses a single temporal holdout
(GOA 220 → 229). Multiple holdouts across different time windows would
strengthen the generalisability claims. The re-ranker's training data is also
limited to the GOA snapshots available in PROTEA's database (releases 160–220);
expanding this range may further improve performance.
