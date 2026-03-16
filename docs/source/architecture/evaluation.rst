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

Implementation reference
-------------------------

- Core logic: :mod:`protea.core.evaluation` — ``EvaluationData``,
  ``compute_evaluation_data``
- Operations: :mod:`protea.core.operations.generate_evaluation_set`,
  :mod:`protea.core.operations.run_cafa_evaluation`
- API router: ``protea/api/routers/annotations.py`` (download endpoints,
  generate and run routes)
