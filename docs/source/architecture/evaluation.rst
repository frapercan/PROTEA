CAFA Evaluation Protocol
========================

PROTEA implements the evaluation protocol used in the CAFA5 (Critical
Assessment of protein Function Annotation) challenge. This page explains the
protocol, the NK/LK/PK classification, and how to run an evaluation end-to-end
within PROTEA.

Background: the CAFA temporal holdout
--------------------------------------

CAFA evaluates protein function prediction by exploiting the growth of
experimental GO annotations over time:

- **t0** — an older annotation snapshot (the *reference* set). Methods may
  use these annotations as training signal.
- **t1** — a newer annotation snapshot (the *ground truth*). Proteins that
  gained new experimental GO annotations between t0 and t1 form the test set
  (the *delta*).

Only annotations with experimental evidence codes are considered
(EXP, IDA, IMP, IGI, IEP, IPI, and their ECO equivalents). Annotations with a
NOT qualifier — meaning the protein is *not* associated with that term — are
excluded, and their exclusion is propagated to all GO descendants through the
``is_a`` and ``part_of`` relationships.

NK / LK / PK classification
-----------------------------

A key feature of CAFA5 is that test proteins are not treated uniformly.
Classification is determined **per (protein, namespace)**, where namespace is
one of Molecular Function (MFO), Biological Process (BPO), or Cellular
Component (CCO).

**NK — No-Knowledge**
   The protein had **no** experimental annotations in **any** namespace at t0.
   All its new annotations across all namespaces form the NK ground truth.
   Evaluating NK targets tests a method's ability to make predictions from
   sequence alone, without any prior functional signal.

**LK — Limited-Knowledge**
   The protein had experimental annotations in **some** namespaces at t0, but
   **not** in namespace S. It gained new annotations in S at t1. Those new
   annotations in S are the LK ground truth for that (protein, S) pair.
   Evaluating LK tests transfer across namespaces.

**PK — Partial-Knowledge**
   The protein already had experimental annotations in namespace S at t0, and
   gained **additional** annotations in S at t1. Only the novel terms are
   ground truth; the old terms are collected in a ``pk_known_terms.tsv`` file
   and passed to ``cafaeval`` with the ``-known`` flag, which excludes them
   from scoring. This prevents credit for simply repeating prior annotations.

.. important::
   A single protein can be **LK in one namespace and PK in another
   simultaneously**. For example, a protein with MFO and BPO annotations at t0
   that gains new CCO and BPO annotations at t1 will be LK for CCO and PK for
   BPO.

Toy example
~~~~~~~~~~~

.. code-block:: text

   Protein P1 at t0:  MFO={GO:0003674}   BPO={}       CCO={}
   Protein P1 at t1:  MFO={GO:0003674}   BPO={GO:0008150}  CCO={GO:0005575}

   had_anything_old = True (had MFO)

   Namespace BPO: old_BPO={}  → LK (empty at t0, gained GO:0008150)
   Namespace CCO: old_CCO={}  → LK (empty at t0, gained GO:0005575)
   Namespace MFO: no new terms → not in test set for this namespace

   Protein P2 at t0:  BPO={GO:0006355}   (all others empty)
   Protein P2 at t1:  BPO={GO:0006355, GO:0045893}

   Namespace BPO: old_BPO={GO:0006355}  delta={GO:0045893}
     → PK ground truth = {GO:0045893}
     → pk_known = {GO:0006355}  (passed as -known)

   Protein P3 at t0:  (no annotations in any namespace)
   Protein P3 at t1:  MFO={GO:0003674}   BPO={GO:0008150}

   had_anything_old = False → NK
   NK ground truth = {GO:0003674, GO:0008150} (all new terms)

Evaluation flow in PROTEA
--------------------------

.. code-block:: text

   1. Load two GOA annotation sets (old = t0, new = t1).
   2. POST /annotations/evaluation-sets/generate
      → queues generate_evaluation_set job
      → computes delta and creates EvaluationSet row with stats
   3. Download delta-proteins.fasta (all NK+LK+PK sequences).
   4. POST /jobs  (compute_embeddings, query_set_id=...)
      → compute ESM-2 embeddings for delta proteins
   5. POST /embeddings/predict  (predict_go_terms, query_set_id=...)
      → run KNN GO transfer; creates PredictionSet
   6. POST /annotations/evaluation-sets/{id}/run
      → queues run_cafa_evaluation job
      → runs cafaeval for NK, LK, PK; creates EvaluationResult
   7. View results in the Evaluation UI or download artifacts.zip.

The ``cafaeval`` command equivalent (for manual inspection):

.. code-block:: bash

   python -m cafaeval go-basic.obo predictions/ ground_truth_NK.tsv -out_dir results/NK
   python -m cafaeval go-basic.obo predictions/ ground_truth_LK.tsv -out_dir results/LK
   python -m cafaeval go-basic.obo predictions/ ground_truth_PK.tsv \
     -known pk_known_terms.tsv -out_dir results/PK

Data model
----------

``EvaluationSet``
   Stores the (old\_annotation\_set\_id, new\_annotation\_set\_id) pair and a
   JSONB ``stats`` dict with delta/NK/LK/PK protein and annotation counts.
   Created by ``generate_evaluation_set``.

``EvaluationResult``
   Stores per-setting (NK/LK/PK) and per-namespace (MFO/BPO/CCO) metrics:
   Fmax, precision, recall, τ (threshold), and coverage. Created by
   ``run_cafa_evaluation``. Multiple ``EvaluationResult`` rows can exist per
   ``EvaluationSet``, one per (prediction\_set, run).

See :doc:`../reference/infrastructure` for the full ORM schema.

Benchmark: PROTEA vs external tools
-------------------------------------

PROTEA was benchmarked against two widely used function annotation tools
using the temporal holdout GOA 220 → GOA 229 (NK: 2831, LK: 3410,
PK: 15313 proteins). All evaluations use ``cafaeval`` with Information
Accretion (IA) weighting from the CAFA6 benchmark.

.. list-table:: Fmax (IA-weighted) — GOA 220 → 229
   :header-rows: 1
   :widths: 20 9 9 9 9 9 9 9 9 9

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

Temporal data leakage
~~~~~~~~~~~~~~~~~~~~~~

Both Pannzer2 and eggNOG-mapper were executed in March 2026 against their
**current** reference databases, which contain annotations published well
after GOA 220 (the t0 snapshot). This means they have access to functional
knowledge that is part of the ground truth.

To quantify this leakage, we measured exact (protein, GO term) matches
between each tool's predictions and the ground truth:

.. list-table:: Exact match with ground truth
   :header-rows: 1
   :widths: 15 12 20 20

   * - Category
     - GT pairs
     - Pannzer2 match
     - eggNOG match
   * - NK
     - 6,953
     - 4,339 (62.4%)
     - 1,025 (14.7%)
   * - LK
     - 5,520
     - 3,624 (65.7%)
     - 1,087 (19.7%)
   * - PK
     - 27,541
     - 12,410 (45.1%)
     - 8,196 (29.8%)
   * - **Total**
     - **40,014**
     - **20,373 (50.9%)**
     - **10,308 (25.8%)**

Pannzer2 exactly matches 62.4% of NK annotations — proteins that by
definition had **no** experimental annotations at t0. This confirms that
its reference database already contains the experimental evidence that
appeared between GOA 220 and GOA 229.

PROTEA is the only tool in this benchmark that enforces temporal integrity
by design: the reference set is frozen at t0, the ground truth is computed
as the delta, and all versions are tracked in the database. Pannzer2 and
eggNOG-mapper numbers should be interpreted as an **optimistic upper
bound** under data leakage, not as a fair comparison.

.. note::
   Running Pannzer2 or eggNOG-mapper against a frozen historical database
   is not possible: the Pannzer2 web server does not offer version
   selection, and eggNOG does not publish historical orthology snapshots.

Evaluating external tools
~~~~~~~~~~~~~~~~~~~~~~~~~~

External tools can be evaluated against the same ground truth using
``scripts/evaluate_external_tool.py``:

.. code-block:: bash

   poetry run python scripts/evaluate_external_tool.py \
       --evaluation-set-id <uuid> \
       --tool emapper \
       --input /path/to/annotations.emapper.annotations

   poetry run python scripts/evaluate_external_tool.py \
       --evaluation-set-id <uuid> \
       --tool pannzer2 \
       --input /path/to/anno.out

Supported formats: ``emapper``, ``pannzer2``, ``interproscan``, ``blast``.

Implementation reference
-------------------------

- Core logic: :mod:`protea.core.evaluation` — ``EvaluationData``,
  ``compute_evaluation_data``
- Operations: :mod:`protea.core.operations.generate_evaluation_set`,
  :mod:`protea.core.operations.run_cafa_evaluation`
- API router: ``protea/api/routers/annotations.py`` (download endpoints,
  generate and run routes)
