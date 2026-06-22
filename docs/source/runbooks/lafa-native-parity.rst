LAFA Native Parity (INT-8 prep)
===============================

This runbook specifies how to reproduce the LAFA 7401-target frame
natively on the platform (``frame="lafa"``) and how to check the native
score against the offline reference predictions, so the on-platform
``/benchmark`` number is trustworthy and leaderboard-comparable.

It is the **non-gated prep** for slice INT-8 (the native seal of the
full-system row on ``/benchmark``). The seal itself needs a
maintainer-authorized redeploy; that gate is spelled out at the end. Run
nothing here that restarts the stack or triggers a redeploy.

Two numbers, one frame
~~~~~~~~~~~~~~~~~~~~~~

- **Offline reference: f_micro_w 0.3911.** The sealed research-method
  champion, scored on the official LAFA 7401-target frame. Its prediction
  file is ``storage/fullgo_models/predictions_merged_7401.tsv`` (3-column
  ``Query_ID  GO_Term  Score`` TSV, the LAFA container output contract).
  The champion's *source* training files were deleted, so 0.3911 is the
  reference target, not a byte-reproducible build.
- **Native best (productized): f_micro_w 0.3745.** The full on-platform
  reranked run, ``EvaluationResult b21b187c`` (the S2 booster trio), UI
  verifiable on ``/benchmark`` (NK 0.4645 / LK 0.4526 / PK 0.2065).

The parity check measures the native pipeline against the offline
reference **on the same fixed 7401-target frame, with the same official
cafaeval recipe**, so any residual is attributable to feature-value
fidelity, not to the frame, harness, pool, or metric.

The converged-seed parity rule
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The offline 0.3911 is reached by **converged seed-averaging, not a lucky
seed**: the per-category boosters are an average over the converged seed
band (7 seeds for the champion), so the number is the central tendency of
the score distribution, not its best draw. The parity comparison MUST be
made against this converged band:

- Compare native vs offline at the **mean over the converged seed band**,
  never a single seed's draw. A native run that matches 0.3911 only on a
  hand-picked seed has not reached parity; it has found a favourable
  draw.
- A native booster trio that is *itself* a single-seed model is compared
  against the offline mean as a lower bound; closing the gap requires
  seed-averaging the native boosters to the same converged band before any
  residual is called real.
- This is the same de-risk discipline the beat-lafa-1 campaign uses: only
  a same-recipe, converged-band delta is meaningful.

The fixed 7401-target frame
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every parity artifact is pinned to the LAFA window; nothing here may fall
back to a different band, OBO, or IA file.

- **Query set:** the 7401 LAFA test targets (a ``QuerySet`` row;
  ``query_set_id`` in the predict payload).
- **Reference pool:** the t0-clean reference ``AnnotationSet``
  (``annotation_set_id``), cutoff ``2025-09-04`` (GOA Sep_2025), the same
  pool that backs the frozen reference embeddings.
- **Ontology snapshot:** the Sep_2025 ``OntologySnapshot``
  (``ontology_snapshot_id``); its ``obo_version`` pins the OBO cafaeval
  reads.
- **IA file:** the t0 ``IA.tsv``
  (``protea-lafa-knn/lafa_t0_Sep_2025/IA.tsv``); the IA-weighting of
  ``f_micro_w``.
- **TOI file:** LAFA's release-specific
  ``CAFA_forever/data/releases/Sep_2025_Mar_2026/groundtruth_terms_of_interest.txt``
  (38649 terms).
- **Ground truth:** the LAFA GT ``EvaluationSet`` (``evaluation_set_id``,
  e.g. ``34a634a8`` on the native checkout), with NK / LK / PK splits and
  the PK-known exclusion list.

The cafaeval recipe is the official LAFA one (see
``docs/EVAL_LAFA_PARITY.md``): ``prop=fill``, ``norm=cafa``,
``no_orphans``, ``th_step=0.01`` (default, the load-bearing parity fix),
``max_terms=None`` (unlimited), ``-ia`` t0 IA, ``-toi`` LAFA TOI, ``-known``
on PK only. Do **not** use the optimistic validation recipe (no-TOI /
no-PK-exclude) for the parity number; that recipe inflates and only its
same-recipe relative delta is meaningful.

What gets dispatched
~~~~~~~~~~~~~~~~~~~~

All dispatch is via ``POST /jobs`` with ``{operation, payload}`` (never
ad-hoc curl to an internal endpoint; ``POST /jobs`` needs a bearer JWT).
The full native parity run is three operations in sequence. **None of
these may run until the redeploy gate below is cleared**, because the
running deploy must carry the LAFA predict wiring
(``apply_self_prior`` / ``apply_association`` / ``apply_ia`` behind the
``compute_*`` flags) and the per-category booster trio must be registered
against the live feature schema.

1. **build_go_cooccurrence** (queue ``protea.jobs``) for the t0 reference
   set, so the ``association`` feature is populated:

   .. code-block:: json

      {
        "operation": "build_go_cooccurrence",
        "payload": {"annotation_set_id": "<t0-reference-set-uuid>"}
      }

   After it finishes, ``ANALYZE term_cooccurrence`` is required: the freshly
   COPY-loaded partition has no statistics, so the planner full-scans the
   partition per association read (minutes per read) until ``ANALYZE`` (a
   few seconds) flips it to the indexed path. (The durable fix is for
   ``build_go_cooccurrence`` to run ``ANALYZE`` at the end of its persist
   step; see the open follow-up.)

2. **predict_go_terms** (queue ``protea.predictions``) over the 7401 query
   set, against the t0 pool, with the LAFA feature families on. This writes
   a new ``PredictionSet`` (the native equivalent of the offline reference
   prediction). Payload:

   .. code-block:: json

      {
        "operation": "predict_go_terms",
        "payload": {
          "embedding_config_id": "<prott5-embedding-config-uuid>",
          "annotation_set_id": "<t0-reference-set-uuid>",
          "ontology_snapshot_id": "<sep2025-snapshot-uuid>",
          "query_set_id": "<lafa-7401-query-set-uuid>",
          "aspect_separated_knn": true,
          "compute_v6_features": true,
          "expand_votes_to_ancestors": true,
          "compute_self_prior": true,
          "compute_association": true,
          "compute_ia": true
        }
      }

   Notes on the flags:

   - ``compute_v6_features: true`` is required: the registered boosters are
     sealed under the v6-inclusive feature schema sha, so serving v6 off
     would feed the v6 block as NaN (train/serve skew, the cause of the
     earlier 0.315 collapse). Seal-sha and serve flags must agree.
   - ``compute_self_prior`` / ``compute_association`` / ``compute_ia`` turn
     on the LAFA per-category JSONB features the champion uses. They ride
     the ``GOPrediction.features`` blob, NOT the feature-schema sha, so they
     are post-KNN and do not change the schema fingerprint.
   - The reranker booster trio is applied via the prediction-time reranker
     binding once registered (the S2 trio
     ``198baf99`` / ``68f3232c`` / ``f0669e41`` reproduces the 0.3745
     headline; a converged-seed trio is what closes toward 0.3911).

3. **run_cafa_evaluation** (queue ``protea.evaluations``) of the new
   prediction set against the LAFA GT, stamped into the LAFA frame:

   .. code-block:: json

      {
        "operation": "run_cafa_evaluation",
        "payload": {
          "evaluation_set_id": "<lafa-gt-eval-set-uuid>",
          "prediction_set_id": "<new-native-prediction-set-uuid>",
          "rerankers": {
            "nk": {"bpo": "<nk-bpo-model>", "mfo": "<nk-mfo-model>", "cco": "<nk-cco-model>"},
            "lk": {"bpo": "<lk-bpo-model>", "mfo": "<lk-mfo-model>", "cco": "<lk-cco-model>"},
            "pk": {"bpo": "<pk-bpo-model>", "mfo": "<pk-mfo-model>", "cco": "<pk-cco-model>"}
          },
          "ia_file": "<t0-IA.tsv-path>",
          "toi_file": "<lafa-groundtruth_terms_of_interest.txt-path>",
          "th_step": 0.01,
          "max_terms": null,
          "frame": "lafa",
          "temporal_window": "FINAL_227_230",
          "leakage_role": "test"
        }
      }

   ``frame: "lafa"`` is what surfaces the LAFA chip and makes the row
   leaderboard-comparable on ``/benchmark``. Leaving it empty (the
   mirror-baseline mistake) drops the chip and the row reads as an unknown
   frame.

How the comparison is checked
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The parity assertion is a same-frame, same-recipe, converged-band
comparison:

1. **Sanity gate first.** Before trusting any delta, re-score the offline
   reference prediction (``predictions_merged_7401.tsv``) through the SAME
   ``run_cafa_evaluation`` payload (skip the predict step; register the TSV
   as a ``PredictionSet`` or score it directly). It MUST reproduce
   **0.3911** within cafaeval's 3-decimal rounding (epsilon about 5e-4). If
   it does not, the frame / IA / TOI / OBO inputs are misconfigured and no
   native delta is meaningful yet.

2. **Native run.** Dispatch the three operations above; read the resulting
   ``EvaluationResult.f_micro_w`` (overall and per NK / LK / PK) on
   ``/benchmark``.

3. **Assert.** The parity check asserts:

   - the offline reference reproduces 0.3911 on the platform recipe (the
     gate);
   - the native number is reported as the **mean over the converged seed
     band**, not a single draw;
   - the native-vs-offline residual is attributed per category (today
     PK is the bottleneck: native ~0.2065 vs the champion's ~0.215+), and
     any residual is feature-value fidelity, not frame / pool / harness /
     metric (all of which are pinned identical here);
   - native >= offline only counts as parity reached when both are at the
     converged band on the identical frame.

4. **Record.** Whatever the result, record it as an ``EvaluationResult``
   row with ``frame: "lafa"`` so it is self-describing and live on
   ``/benchmark`` (the platform is the single source of truth; no /tmp
   scripts).

The redeploy gate (maintainer ask)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The native seal (INT-8) is **blocked** on a maintainer-authorized
redeploy. The prep above is complete and safe to read / plan; the actual
dispatch needs the following, in order, and only the maintainer may
authorize the redeploy / restart:

1. **Resync the deploy to the LAFA predict wiring.** The full native run
   needs ``apply_self_prior`` / ``apply_association`` / ``apply_ia`` live
   behind ``compute_self_prior`` / ``compute_association`` /
   ``compute_ia`` in ``_post_knn_pipeline.py``, plus the contracts version
   that carries those payload fields (v0.7.0+). The running deploy must be
   the develop tip that carries this wiring; a redeploy onto a branch that
   lacks it would lose the LAFA path. **Why blocked:** redeploy / stack
   restart is maintainer-gated by the hard project constraint.

2. **Register the per-category booster trio** (``POST
   /reranker-models/import`` x3, NK / LK / PK) sealed under the live
   feature schema sha, with ``training.cell`` set per category so they do
   not all register as ``pk``. Confirm the import and the predict reader
   resolve the SAME artifact store backend (the import-to-MinIO /
   predict-reads-local mismatch is a known landmine; both must be minio or
   both local).

3. **Run build_go_cooccurrence + ANALYZE** for the t0 reference set (step 1
   above), so ``association`` is non-zero at serve.

4. **Dispatch predict + run_cafa_evaluation over the 7401 frame** (steps 2
   and 3 above), frame-stamped ``lafa``.

5. **Surface on /benchmark.** ``/benchmark`` is a read-only aggregator over
   ``EvaluationResult`` rows; the new frame-stamped row appears
   automatically once the eval succeeds. No redeploy is needed for the row
   to show, only for the predict path to be live.

In one line for the maintainer: **enable the ``compute_*`` LAFA feature
flags on a deploy that carries the LAFA predict wiring, register the
converged-seed booster trio against the live schema, run
build_go_cooccurrence + ANALYZE, then predict + run_cafa_evaluation over
the 7401 frame stamped ``lafa``; it is blocked only because the redeploy /
restart is maintainer-authorized.**

See also
~~~~~~~~

- :doc:`/adr/D44-continuous-lafa-container` for the continuous-deployment
  loop that keeps the deployed container + data at the current best.
- ``docs/EVAL_LAFA_PARITY.md`` for the cafaeval recipe and the th_step /
  toi parity mechanics.
- :doc:`/adr/D23-lafa-submission` for the container strategy and
  :doc:`/adr/D40-leakage-free-temporal-eval-protocol` for the window
  protocol.
