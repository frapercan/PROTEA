Reproduction guide (superseded, retained for provenance)
========================================================

.. admonition:: Superseded frame and dead endpoint, retained for provenance only (dated 2026-07-10)
   :class: danger

   This guide reproduces the abandoned pre-v227 Fmax board (GOA 220 to 229,
   ESM-C 300M, the lgbm_v1/v2/v3 progression), not the sealed result. It also
   drives ``POST /scoring/rerankers/train``, an endpoint that no longer
   exists: re-ranker training moved to the ``protea-reranker-lab`` sibling
   repository, and boosters are registered through
   ``POST /reranker-models/import``. The page is kept only so the older
   procedure remains traceable. For the current path see
   :doc:`/operate/reproduce-the-sealed-board`; for the sealed board see :doc:`/results`;
   for the protocol see :doc:`/architecture/evaluation`.


.. admonition:: Audience and scope
   :class: tip

   **Read this if:** you want to reproduce the full thesis evaluation
   end-to-end against the GOA 220 → 229 temporal holdout.

   **Read** :doc:`/appendix/howto_guides` **instead if:** you have one specific task to
   accomplish (load an ontology, upload a FASTA, predict GO terms for your
   own proteins, scale a worker). The how-to is recipe-style and stops at the
   step you need; this guide is a single ordered procedure that runs every
   experiment in sequence.

.. admonition:: Provisional expected values, pending final recompute
   :class: warning

   The expected Fmax values cited throughout this guide (baseline
   0.412 / 0.590 / 0.668, the +1.5-4 % ``alignment_weighted`` gain, the
   second and third re-ranker iteration targets) are the pre-2026-04-10
   numbers and will be refreshed for the Zenodo deposit. See
   :doc:`/results` for the full provisional notice and the reason
   behind the recompute.

.. admonition:: API drift across Stages 1.3 – 4
   :class: caution

   The curl recipes from **Step 1.3 onward** still use field names that
   the current Pydantic payloads no longer accept. Stage 1 up to and
   including Step 1.2 is correct (PR #138 fixed
   ``source_tag`` → ``source_version`` there); everything after needs
   manual translation before it can be re-run end-to-end. The
   *procedure* (the order of jobs, the conceptual inputs / outputs) is
   stable; only the field names drifted.

   Concrete renames operators must apply:

   .. list-table::
      :header-rows: 1
      :widths: 28 32 40

      * - Where
        - Doc says
        - Current payload uses
      * - Step 1.3 (``generate``) and the GET filter
        - ``"name": "goa_220_to_229"``, ``?name=…``
        - ``GenerateEvaluationSetPayload`` accepts no ``name``
          field; filter the GET response client-side with ``jq``.
      * - Step 1.4 (``compute_embeddings``)
        - ``"target": "all_sequences"``
        - ``ComputeEmbeddingsPayload`` accepts ``accessions`` (list)
          or ``query_set_id`` (UUID); ``null`` for both = embed all.
      * - Stage 2 (``predict_go_terms``)
        - ``query_annotation_set_id``, ``reference_annotation_set_id``
        - The payload has a single ``annotation_set_id`` (the reference
          set) plus ``query_set_id`` (a ``QuerySet`` UUID). The query
          *set* is no longer keyed off another ``AnnotationSet``.
      * - Stage 2 (``predict_go_terms``)
        - ``"k": 5``, ``"backend": "faiss"``, ``"index_type": …``,
          ``"name": "k5_aspect_sep"``
        - ``limit_per_entry``, ``search_backend``, ``faiss_index_type``;
          there is no ``name`` field on ``PredictGoTermsPayload``.
      * - Stages 3 and 4 (``run_cafa_evaluation``)
        - ``scoring_config_name``, ``reranker_name``
        - ``scoring_config_id`` (UUID) plus either flat
          ``reranker_id_{nk,lk,pk}`` UUIDs or the nested ``rerankers``
          mapping. See :class:`RunCafaEvaluationPayload` in
          :mod:`protea.core.operations.run_cafa_evaluation`.

   **Stage 4 specifically** also targets the retired
   ``POST /scoring/rerankers/train`` endpoint
   (``train_reranker`` / ``train_reranker_auto`` were unregistered in
   F0 / T0.6). LightGBM training has moved to the sibling repo
   `protea-reranker-lab <https://github.com/frapercan/protea-reranker-lab>`_,
   which consumes a frozen parquet dataset published by PROTEA via
   ``export_research_dataset`` and registers the booster through
   ``POST /reranker-models/import``. The four-step flow is documented
   in :ref:`Register a reranker from protea-reranker-lab
   <howto-register-reranker>`.

   The historical experiments below produced the
   ``lgbm_v1`` / ``lgbm_v2`` / ``lgbm_v3`` boosters that still back the
   numbers in :doc:`/results`; the *commands* must be translated
   against the contracts in :doc:`/architecture/operations` before
   they can be re-run. A staged rewrite of this guide is on the
   doc-writer roadmap.

This appendix documents the exact sequence of steps required to reproduce the
experimental results reported in :doc:`/results`. The target is a fresh
PROTEA installation against the GOA 220 → 229 temporal holdout, covering all
nine experiments: the ``k`` sweep, the aspect-separated KNN ablation, the five
heuristic scoring configurations, the three re-ranker iterations
(``lgbm_v1``, ``lgbm_v2``, ``lgbm_v3``), and the external benchmark against
eggNOG-mapper, Pannzer2, and InterProScan 6.

Every command is expressed against the public HTTP API. The API runs at
``http://127.0.0.1:8000`` after ``bash scripts/manage.sh start``; environment
variables such as ``API=http://127.0.0.1:8000`` are used for brevity.

.. contents::
   :local:
   :depth: 2

Infrastructure
--------------

The full experimental campaign used:

- **15 GOA snapshots** (releases 160 through 229) loaded as independent
  ``AnnotationSet`` rows, all referencing a single ``OntologySnapshot``.
- **GO ontology** release ``2026-01-23`` plus the CAFA6 Information Accretion
  file (``IA_cafa6.tsv``) for IA-weighted evaluation.
- **527 K ESM-C 300M embeddings** (embedding dimension 960, stored as
  ``pgvector`` ``VECTOR(960)``).
- **Evaluation set** computed from the GOA 220 → 229 delta. The delta contains
  2 831 NK, 3 410 LK, and 15 313 PK proteins.
- **Query set** consisting of the ~20 000 proteins present in the delta.
- **Evaluator**: ``cafaeval`` with IA weighting.

The key reference UUIDs from the original campaign are synthesised on the
results page (:doc:`/results`). Reproducing the experiments
on a new deployment regenerates these UUIDs; the shell variables below are
placeholders that the user fills in after each preparation step.

Stage 1: Prepare infrastructure
--------------------------------

Step 1.1: Load the GO ontology
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Queue a ``load_ontology_snapshot`` job. The OBO file is versioned by
``obo_version``; loading the same release twice is idempotent.

.. code-block:: bash

   curl -X POST $API/annotations/snapshots/load \
     -H "Content-Type: application/json" \
     -d '{
       "obo_url": "http://release.geneontology.org/2026-01-23/ontology/go.obo",
       "obo_version": "2026-01-23"
     }'

Poll ``GET /jobs/{id}`` until ``status == "SUCCEEDED"``, then capture the
snapshot ID:

.. code-block:: bash

   SNAPSHOT_ID=$(curl -s $API/annotations/snapshots \
     | jq -r '.[0].id')

Step 1.2: Load GOA annotation sets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For the temporal re-ranker training pipeline the campaign loads 15 releases
(``160`` through ``229``). For the minimum reproduction path only two are
required: ``220`` (the ``t0`` reference) and ``229`` (the ``t1`` ground truth).

.. code-block:: bash

   for REL in 160 165 170 175 180 185 190 195 200 205 210 215 220 225 229; do
     curl -X POST $API/annotations/sets/load-goa \
       -H "Content-Type: application/json" \
       -d "{
         \"gaf_url\": \"http://release.geneontology.org/2026-01-23/annotations/goa_uniprot_all.gaf.gz\",
         \"ontology_snapshot_id\": \"$SNAPSHOT_ID\",
         \"source_version\": \"goa_${REL}\"
       }"
   done

Each load emits ``ProteinGOAnnotation`` rows filtered against canonical
accessions already in the database, so ``insert_proteins`` for the UniProt
slice of interest must have been executed beforehand.

Record the two critical IDs once the jobs complete. The ``GET /annotations/sets``
endpoint accepts only a ``source`` filter (``goa`` or ``quickgo``); narrow
to a specific release with ``jq``:

.. code-block:: bash

   OLD_SET=$(curl -s "$API/annotations/sets?source=goa" \
     | jq -r '.[] | select(.source_version=="goa_220") | .id')
   NEW_SET=$(curl -s "$API/annotations/sets?source=goa" \
     | jq -r '.[] | select(.source_version=="goa_229") | .id')

Step 1.3: Generate the NK/LK/PK evaluation set
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   curl -X POST $API/annotations/evaluation-sets/generate \
     -H "Content-Type: application/json" \
     -d "{
       \"old_annotation_set_id\": \"$OLD_SET\",
       \"new_annotation_set_id\": \"$NEW_SET\",
       \"ontology_snapshot_id\": \"$SNAPSHOT_ID\",
       \"name\": \"goa_220_to_229\"
     }"

   EVAL_SET=$(curl -s "$API/annotations/evaluation-sets?name=goa_220_to_229" \
     | jq -r '.[0].id')

The operation implements the CAFA5 protocol described in
:doc:`/architecture/evaluation`: NOT-propagation through the GO DAG,
experimental evidence filtering, and per-namespace classification. The
summary counts stored on the ``EvaluationSet`` row should match the numbers
reported in Infrastructure above.

Step 1.4: Compute ESM-C reference embeddings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create the embedding config first (ESM-C 300M, mean-pooled, float16 storage):

.. code-block:: bash

   curl -X POST $API/embeddings/configs \
     -H "Content-Type: application/json" \
     -d '{
       "model_name": "esmc_300m",
       "pooling": "mean",
       "dtype": "float16"
     }'

   EMB_CONFIG=$(curl -s $API/embeddings/configs | jq -r '.[0].id')

Then enqueue the coordinator job. The coordinator is serialised on the
``protea.embeddings`` queue to prevent concurrent GPU model loads; batch and
write workers scale independently.

.. code-block:: bash

   curl -X POST $API/jobs \
     -H "Content-Type: application/json" \
     -d "{
       \"operation\": \"compute_embeddings\",
       \"payload\": {
         \"embedding_config_id\": \"$EMB_CONFIG\",
         \"target\": \"all_sequences\"
       }
     }"

The full reference set contains 527 K sequences; total wall-clock time is
approximately 6–8 hours on a single GPU. Monitor ``manage.sh status`` and the
``protea.embeddings.batch`` worker logs for progress.

Stage 2: Baseline KNN experiments
----------------------------------

Stage 2 reproduces experiments 1 and 2 from the results page
(:doc:`/results`): the ``k`` sweep and the ``aspect_separated_knn`` ablation.

Experiment 1: ``k`` sweep
~~~~~~~~~~~~~~~~~~~~~~~~~

Run one ``predict_go_terms`` job for each target ``k``. Feature-engineering
flags are left disabled at this stage; the scoring and re-ranker experiments
reuse a single enriched prediction set generated in Stage 3.

.. code-block:: bash

   for K in 5 10 20 50; do
     curl -X POST $API/embeddings/predict \
       -H "Content-Type: application/json" \
       -d "{
         \"embedding_config_id\": \"$EMB_CONFIG\",
         \"query_annotation_set_id\": \"$NEW_SET\",
         \"reference_annotation_set_id\": \"$OLD_SET\",
         \"ontology_snapshot_id\": \"$SNAPSHOT_ID\",
         \"k\": $K,
         \"aspect_separated_knn\": true,
         \"backend\": \"faiss\",
         \"index_type\": \"IVFFlat\",
         \"name\": \"k${K}_aspect_sep\"
       }"
   done

For each resulting prediction set, run CAFA evaluation against the evaluation
set:

.. code-block:: bash

   for PS in $(curl -s $API/embeddings/prediction-sets | jq -r '.[].id'); do
     curl -X POST $API/annotations/evaluation-sets/$EVAL_SET/run \
       -H "Content-Type: application/json" \
       -d "{\"prediction_set_id\": \"$PS\", \"scoring_config_name\": \"embedding_only\"}"
   done

The expected ``k = 5`` baseline Fmax (IA-weighted) is ``0.412 / 0.590 / 0.668``
for NK BPO/MFO/CCO and degrades monotonically for larger ``k``. Use ``k = 5``
for all downstream experiments.

Experiment 2: ``aspect_separated_knn``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Re-run prediction with ``aspect_separated_knn: false`` and compare against the
``k = 5`` result from Experiment 1:

.. code-block:: bash

   curl -X POST $API/embeddings/predict \
     -H "Content-Type: application/json" \
     -d "{
       \"embedding_config_id\": \"$EMB_CONFIG\",
       \"query_annotation_set_id\": \"$NEW_SET\",
       \"reference_annotation_set_id\": \"$OLD_SET\",
       \"ontology_snapshot_id\": \"$SNAPSHOT_ID\",
       \"k\": 5,
       \"aspect_separated_knn\": false,
       \"backend\": \"faiss\",
       \"index_type\": \"IVFFlat\",
       \"name\": \"k5_aspect_unified\"
     }"

Differences between the two variants are within ±0.011 Fmax across all nine
cells. The campaign retains ``aspect_separated_knn = true`` for uniform aspect
coverage.

Stage 3: Feature engineering and scoring
-----------------------------------------

Experiment 3: Heuristic scoring configurations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All scoring configurations operate on a single enriched prediction set that
includes alignment and taxonomy features. Generate it once:

.. code-block:: bash

   curl -X POST $API/embeddings/predict \
     -H "Content-Type: application/json" \
     -d "{
       \"embedding_config_id\": \"$EMB_CONFIG\",
       \"query_annotation_set_id\": \"$NEW_SET\",
       \"reference_annotation_set_id\": \"$OLD_SET\",
       \"ontology_snapshot_id\": \"$SNAPSHOT_ID\",
       \"k\": 5,
       \"aspect_separated_knn\": true,
       \"backend\": \"faiss\",
       \"index_type\": \"IVFFlat\",
       \"compute_alignments\": true,
       \"compute_taxonomy\": true,
       \"compute_reranker_features\": true,
       \"name\": \"k5_full_features\"
     }"

   PS_FULL=$(curl -s "$API/embeddings/prediction-sets?name=k5_full_features" \
     | jq -r '.[0].id')

The resulting prediction set populates the 20 numeric and 3 categorical
feature columns documented in :doc:`/reference/core` and is reused by
Stage 3, Stage 4, and Stage 5.

Seed the scoring configuration presets and evaluate each one:

.. code-block:: bash

   curl -X POST $API/scoring/configs/presets

   for CFG in embedding_only alignment_weighted evidence_primary \
              embedding_plus_evidence composite; do
     curl -X POST $API/annotations/evaluation-sets/$EVAL_SET/run \
       -H "Content-Type: application/json" \
       -d "{
         \"prediction_set_id\": \"$PS_FULL\",
         \"scoring_config_name\": \"$CFG\"
       }"
   done

The ``alignment_weighted`` preset is expected to dominate every cell,
improving the ``embedding_only`` baseline by +1.5 % to +4 % Fmax. Every scoring
configuration that mixes evidence-code weights degrades the baseline under
IA-weighted CAFA evaluation.

Stage 4: Re-ranker training
----------------------------

Experiment 4: Re-ranker iteration 1 (per-aspect LightGBM)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The first iteration (``lgbm_v1``) trains nine LightGBM binary
classifiers (one per NK/LK/PK ×
BPO/MFO/CCO cell) on 12 temporal splits (GOA 160 → 165, 165 → 170, …,
215 → 220). Train first without class balancing and then with
``neg_pos_ratio = 10`` to observe the effect on the BPO cells, which otherwise
early-stop after a single boosting iteration.

.. code-block:: bash

   curl -X POST $API/scoring/rerankers/train \
     -H "Content-Type: application/json" \
     -d "{
       \"name\": \"lgbm_v1_unbalanced\",
       \"strategy\": \"per_aspect\",
       \"splits\": [[160,165],[165,170],[170,175],[175,180],[180,185],
                    [185,190],[190,195],[195,200],[200,205],[205,210],
                    [210,215],[215,220]],
       \"test_split\": [220, 229],
       \"ontology_snapshot_id\": \"$SNAPSHOT_ID\",
       \"embedding_config_id\": \"$EMB_CONFIG\"
     }"

   curl -X POST $API/scoring/rerankers/train \
     -H "Content-Type: application/json" \
     -d "{
       \"name\": \"lgbm_v1_balanced\",
       \"strategy\": \"per_aspect\",
       \"neg_pos_ratio\": 10,
       \"splits\": [[160,165],[165,170],[170,175],[175,180],[180,185],
                    [185,190],[190,195],[195,200],[200,205],[205,210],
                    [210,215],[215,220]],
       \"test_split\": [220, 229],
       \"ontology_snapshot_id\": \"$SNAPSHOT_ID\",
       \"embedding_config_id\": \"$EMB_CONFIG\"
     }"

In the unbalanced run six of the nine models early-stop at iteration 1 due to
extreme class imbalance (≈0.17 % positives in BPO). The balanced run recovers
BPO (+0.124 AUC for LK-BPO) but does not outperform ``alignment_weighted``.

Experiment 5: Re-ranker iteration 2 (per-category + IA weighting)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The second iteration (``lgbm_v2``) collapses the nine per-aspect
models into three per-category models (NK, LK, PK) and passes the
per-term Information Accretion values as
``sample_weight`` to LightGBM, so that rare terms contribute more to the
training loss. Hyperparameters move to ``learning_rate = 0.01`` and
``num_boost_round = 1000`` with ``early_stopping_rounds = 50``.

.. code-block:: bash

   curl -X POST $API/scoring/rerankers/train \
     -H "Content-Type: application/json" \
     -d "{
       \"name\": \"lgbm_v2_full\",
       \"strategy\": \"per_category\",
       \"neg_pos_ratio\": 10,
       \"ia_weights\": true,
       \"learning_rate\": 0.01,
       \"num_boost_round\": 1000,
       \"early_stopping_rounds\": 50,
       \"splits\": [[160,165],[165,170],[170,175],[175,180],[180,185],
                    [185,190],[190,195],[195,200],[200,205],[205,210],
                    [210,215],[215,220]],
       \"test_split\": [220, 229],
       \"ontology_snapshot_id\": \"$SNAPSHOT_ID\",
       \"embedding_config_id\": \"$EMB_CONFIG\"
     }"

The expected ``lgbm_v2_full`` Fmax matches or exceeds the
``lgbm_v1`` numbers in every cell but does not yet overtake
``alignment_weighted`` globally.

Experiment 6: Re-ranker iteration 3 (full alignment and taxonomy features)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The third iteration (``lgbm_v3``) is identical to ``lgbm_v2`` except
that the training-data generator calls ``compute_alignment()`` and
``compute_taxonomy()`` for every (query, reference) pair in the
historical splits, so that the alignment and taxonomy feature columns
are populated throughout training (in ``lgbm_v1`` and ``lgbm_v2``
those columns were hardcoded to ``NULL`` because the features were
only computed at prediction time).

.. code-block:: bash

   curl -X POST $API/scoring/rerankers/train \
     -H "Content-Type: application/json" \
     -d "{
       \"name\": \"lgbm_v3_full\",
       \"strategy\": \"per_category\",
       \"neg_pos_ratio\": 10,
       \"ia_weights\": true,
       \"compute_alignments\": true,
       \"compute_taxonomy\": true,
       \"learning_rate\": 0.01,
       \"num_boost_round\": 1000,
       \"early_stopping_rounds\": 50,
       \"splits\": [[160,165],[165,170],[170,175],[175,180],[180,185],
                    [185,190],[190,195],[195,200],[200,205],[205,210],
                    [210,215],[215,220]],
       \"test_split\": [220, 229],
       \"ontology_snapshot_id\": \"$SNAPSHOT_ID\",
       \"embedding_config_id\": \"$EMB_CONFIG\"
     }"

Training wall-clock time is approximately 2 h 45 min on a single CPU machine.
The alignment overhead during training data generation is marginal (~15 min
over ``lgbm_v2``). Three models are produced: ``lgbm_v3_full-nk``,
``lgbm_v3_full-lk``, ``lgbm_v3_full-pk``.

Score and evaluate the enriched prediction set with each ``lgbm_v3`` model:

.. code-block:: bash

   for CAT in nk lk pk; do
     curl -X POST $API/annotations/evaluation-sets/$EVAL_SET/run \
       -H "Content-Type: application/json" \
       -d "{
         \"prediction_set_id\": \"$PS_FULL\",
         \"reranker_name\": \"lgbm_v3_full-$CAT\"
       }"
   done

Expected result: ``lgbm_v3`` outperforms the ``alignment_weighted``
heuristic in 7 of the 9 evaluation cells and is the best global
configuration reported in :doc:`/results`.

Stage 5: External tool benchmarks
----------------------------------

All external tools are evaluated on the same 20 281-protein delta as PROTEA
using ``scripts/evaluate_external_tool.py``. The script normalises each tool's
output into a CAFA-style ``predictions.tsv`` and runs ``cafaeval`` with IA
weights against the same evaluation set.

Experiment 7: eggNOG-mapper
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   docker run --rm -v $(pwd)/data:/data \
     quay.io/biocontainers/eggnog-mapper:2.1.13--pyhdfd78af_2 \
     emapper.py -i /data/query.fasta -o /data/eggnog_out \
       -m diamond --go_evidence experimental \
       --tax_scope auto --target_orthologs all --cpu 8

   poetry run python scripts/evaluate_external_tool.py \
     --tool eggnog \
     --predictions data/eggnog_out.emapper.annotations \
     --evaluation-set $EVAL_SET

The PROTEA reranker third iteration (``lgbm_v3``) is expected to
outperform eggNOG-mapper in 9 of 9 cells (differences up to +0.306
Fmax in NK-CCO).

Experiment 8: Pannzer2 and the data-leakage analysis
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pannzer2 is invoked via the Helsinki web server; results are downloaded as
HTML, parsed into a CAFA-style TSV, and scored the same way.

.. code-block:: bash

   poetry run python scripts/evaluate_external_tool.py \
     --tool pannzer2 \
     --predictions data/pannzer2_predictions.tsv \
     --evaluation-set $EVAL_SET

Pannzer2 posts the highest apparent Fmax in the benchmark (e.g. NK-MFO 0.717),
but its reference database was pulled in March 2026, after the GOA 229 cutoff
that defines the ground truth. The leakage measurement compares the
(protein, GO term) pairs in the ground truth against those in each tool's
predictions and reports the exact-match overlap per NK/LK/PK category. For
Pannzer2 this overlap reaches **62.4 %** of the NK ground truth, fully
explaining its apparent advantage over temporally strict methods. PROTEA is
the only tool in the benchmark that freezes its reference at ``t0``, so its
numbers are the only fair upper bound.

Experiment 9: InterProScan 6
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   docker run --rm -v $(pwd)/data:/data \
     interpro/interproscan:6.0.0 \
     interproscan.sh -i /data/query.fasta -f TSV -goterms \
       -d /data/interproscan_out

   poetry run python scripts/evaluate_external_tool.py \
     --tool interproscan \
     --predictions data/interproscan_out/query.fasta.tsv \
     --evaluation-set $EVAL_SET

The PROTEA reranker third iteration (``lgbm_v3``) is expected to
outperform InterProScan 6 in 8 of 9 cells.

Checklist
---------

The nine experiments above fully reproduce the figures and tables in
:doc:`/results`:

1. Experiment 1: ``k`` sweep (``k ∈ {5, 10, 20, 50}``)
2. Experiment 2: ``aspect_separated_knn`` ablation
3. Experiment 3: Heuristic scoring (five presets)
4. Experiment 4: Re-ranker ``lgbm_v1`` (per-aspect, unbalanced and balanced)
5. Experiment 5: Re-ranker ``lgbm_v2`` (per-category, IA-weighted)
6. Experiment 6: Re-ranker ``lgbm_v3`` (per-category, full alignment and
   taxonomy features), best global configuration
7. Experiment 7: eggNOG-mapper benchmark
8. Experiment 8: Pannzer2 benchmark plus data-leakage analysis
9. Experiment 9: InterProScan 6 benchmark

Every prediction set, evaluation result, and re-ranker model is persisted in
the database with a UUID that can be recorded alongside the thesis tables,
making each reported number traceable to a concrete job, a concrete payload,
and a concrete input snapshot.
