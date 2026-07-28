Clean Reproducible Evaluation Frame (R0.1)
==========================================

This runbook specifies how to build a CLEAN, fully reproducible
evaluation frame on the platform: an evaluation set generated job-backed
(never an orphan ``job_id=None`` artifact), pinned to asymmetric
cross-OBO snapshots plus a fixed IA and TOI, and scored so a baseline
dense-KNN reranked ``f_micro_w`` reproduces bit-identically across two
independent runs.

It is the unblocker for the roadmap-from-zero campaign. Without a frame
where the dense baseline reproduces bit-identically, no learned-vs-dense
or learned-vs-leaderboard delta is trustworthy. The driving lesson from
the prior campaign is the score archaeology pain: the ``0.3745`` champion
could not be reproduced because it was an external ``job_id=None``
artifact under an unrecoverable propagation frame. Every number must now
be born on-platform with full provenance on a frozen, versioned frame.

This is NOT a DB wipe. The expensive 527k x 8-PLM embeddings stay; the
fix is a job-backed eval frame plus provenance discipline, with the
Postgres backups as the safety net.

All dispatch is via ``POST /jobs`` with ``{operation, payload}`` and a
bearer JWT. Never use ad-hoc curl to an internal endpoint.

Provenance: every result carries its job id
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``BaseWorker`` injects the claimed job's id as ``_job_id`` into every
operation payload. Both eval operations now thread it onto the persisted
row (via :func:`protea.core.utils.job_id_from_payload`):

- ``generate_evaluation_set`` stamps it onto the ``EvaluationSet.job_id``;
- ``run_cafa_evaluation`` stamps it onto the ``EvaluationResult.job_id``.

So a row produced through ``POST /jobs`` is traceable back to the job
that produced it (and through the job, to the exact payload, OBO, IA, and
TOI). A row with ``job_id = None`` is an orphan artifact (the
archaeology trap) and must not be trusted as part of the frame. The
``/benchmark/matrix`` per-cell payload surfaces ``job_id`` so the
distinction is visible in the UI without a second request.

To audit the frame for orphans:

.. code-block:: sql

   SELECT id, created_at, frame, temporal_window
   FROM evaluation_result
   WHERE job_id IS NULL
   ORDER BY created_at DESC;

Any row here predates the provenance fix or was produced off-platform;
it is not part of the clean frame.

Pinning the frame: asymmetric cross-OBO + IA + TOI
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The frame pins three things, and a band-declared cell may never
free-float any of them (the ``protea.core.band_registry`` guard rejects
cross-band contamination at runtime and in CI):

- **OBO t0 / t1 (asymmetric, cross-OBO).** The t0 (old) and t1 (new)
  sides are propagated under EXPLICIT ontology snapshots, decoupled from
  each annotation set's stored ``ontology_snapshot_id``. This is the
  cross-OBO native-snapshot override (see ``docs/EVAL_LAFA_PARITY.md``
  and the phantom-gap audit): t0 propagates under its congruent OBO
  (``releases/2025-07-22`` for v227), NOT under a churned, too-new graph
  that would mark pre-window experimental annotations as new knowledge.
  Pass ``old_native_snapshot_id`` and ``new_native_snapshot_id`` on
  ``generate_evaluation_set``. They select the propagation DAG per side
  without touching annotation rows. ``None`` falls back to each set's
  stored snapshot (the symmetric default), which is what mis-framed the
  prior native delta; use the explicit asymmetric pins for the clean
  frame.
- **IA.** The Information Accretion artifact that weights ``f_micro_w``,
  passed as ``ia_file`` on ``run_cafa_evaluation`` (or resolved from the
  snapshot ``ia_url``). It must be the t0 IA for the band; the band
  registry refuses the uniform IC=1 fallback for a band-declared cell.
- **TOI.** The terms-of-interest file restricting which terms count
  toward precision and recall, passed as ``toi_file``. LAFA passes a
  release-specific ``groundtruth_terms_of_interest.txt`` (a strict subset
  of the full ontology and a strict superset of the GT term union). For
  strict parity pass that exact file; otherwise PROTEA derives the TOI
  from the pivot snapshot (a small, MFO-biased residual, documented in
  ``docs/EVAL_LAFA_PARITY.md``).

Step 1: generate the evaluation set (job-backed, cross-OBO)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: json

   {
     "operation": "generate_evaluation_set",
     "payload": {
       "old_annotation_set_id": "<t0-reference-set-uuid>",
       "new_annotation_set_id": "<t1-target-set-uuid>",
       "old_native_snapshot_id": "<t0-congruent-snapshot-uuid>",
       "new_native_snapshot_id": "<t1-snapshot-uuid>",
       "window_role": "test"
     }
   }

Notes:

- ``old_native_snapshot_id`` / ``new_native_snapshot_id`` are the
  asymmetric cross-OBO pins. With them set the operation takes the
  reconciled compute path (``compute_evaluation_data_reconciled``),
  resolving go-id text and loading each side's native DAG by snapshot
  id.
- A native-snapshot override changes the computed delta, so it is never
  silently served from (nor silently overwrites) the unique cached
  ``(old, new)`` set: if one already exists the operation errors and you
  must remove it before regenerating. This guards against a stale
  symmetric-frame set masquerading as the clean cross-OBO one.
- The resulting row carries ``EvaluationSet.job_id`` (the job that ran
  it). Record the ``evaluation_set_id`` from the result.

Step 2: run the baseline dense-KNN reranked evaluation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Dispatch ``run_cafa_evaluation`` against the eval set and the dense-KNN
``PredictionSet``, with the LAFA cafaeval recipe and the frame stamps:

.. code-block:: json

   {
     "operation": "run_cafa_evaluation",
     "payload": {
       "evaluation_set_id": "<eval-set-uuid-from-step-1>",
       "prediction_set_id": "<dense-knn-prediction-set-uuid>",
       "ia_file": "<t0-IA.tsv-path>",
       "toi_file": "<lafa-groundtruth_terms_of_interest.txt-path>",
       "th_step": 0.01,
       "max_terms": null,
       "band": "v227",
       "frame": "lafa",
       "temporal_window": "FINAL_227_230",
       "leakage_role": "test"
     }
   }

The cafaeval recipe is the official LAFA one (``prop=fill``,
``norm=cafa``, ``no_orphans``, ``th_step=0.01``, ``max_terms=None``,
``-ia`` t0 IA, ``-toi`` LAFA TOI, ``-known`` on PK only). ``th_step``
is the load-bearing parity knob: LAFA uses cafaeval's default ``0.01``;
a finer grid inflates ``f_micro_w`` (up to ``+0.014`` historically). Do
not pass the optimistic validation recipe (no-TOI / no-PK-exclude) for a
frame number. See ``docs/EVAL_LAFA_PARITY.md`` for the knob-by-knob
mapping.

``band: "v227"`` arms the phantom-gap guard: the run is rejected if the
resolved pivot snapshot or IA come from another band. ``frame: "lafa"``
surfaces the LAFA chip on ``/benchmark`` and makes the row
leaderboard-comparable.

Record the ``evaluation_result_id`` and the per-cell ``f_micro_w``.

Step 3: verify bit-identical reproduction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Dispatch step 2 a SECOND time, unchanged, against the same eval set and
the same prediction set. The two ``EvaluationResult`` rows must agree on
``f_micro_w`` per NK / LK / PK x BP / CC / MF cell to the bit (their
``job_id`` and ``id`` differ; the metric values do not).

Why this is deterministic: the cafaeval driver
(:func:`protea.core.operations._run_cafa_eval_driver._invoke_cafaeval_signal_safe`)
runs no RNG. LightGBM ``predict`` is deterministic; the dense base frame
is deduplicated with a stable ``mergesort`` on
``(protein_accession, go_id, distance)``; ground truth is written from
sorted iterables; the threshold grid is a fixed ``np.arange``. The
synthetic-ontology and opt-in real-leaderboard parity tests in
``tests/test_lafa_frame_parity.py`` pin that PROTEA's driver scores in
the exact same frame as the LAFA scorer and that ``th_step`` is
load-bearing.

To compare two result rows:

.. code-block:: sql

   SELECT id, job_id, results
   FROM evaluation_result
   WHERE evaluation_set_id = '<eval-set-uuid>'
     AND prediction_set_id = '<dense-knn-prediction-set-uuid>'
   ORDER BY created_at DESC
   LIMIT 2;

Extract ``results -> '<category>' -> '<aspect>' -> 'f_micro_w'`` for both
rows; every cell must be equal. A mismatch means a non-pinned input
leaked (a different OBO, IA, TOI, or prediction set), and the frame is
not clean yet.

/benchmark and manual dispatch agree
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``/benchmark`` is read-only: it aggregates persisted ``EvaluationResult``
rows; it does not run eval. So a number on ``/benchmark`` agrees with a
manual ``POST /jobs`` dispatch exactly when the manual dispatch used the
recipe above. The matrix surfaces ``job_id``, ``frame``,
``temporal_window``, ``leakage_role``, and ``arms_enabled`` per cell, so
an operator can confirm a benchmarked cell is job-backed and frame-stamped
(not an orphan) without a second request. A cell with a ``null`` job-id
chip is not part of the clean frame and must be re-run job-backed before
it is trusted.

Related
~~~~~~~

- ``docs/EVAL_LAFA_PARITY.md`` for the cafaeval recipe and the
  ``th_step`` / ``max_terms`` / ``toi`` knob mapping.
- ``docs/BAND_REGISTRY.md`` and ``protea.core.band_registry`` for the
  per-band OBO + IA pinning and the phantom-gap guard.
- ``runbooks/lafa-native-parity`` for the champion-trio parity check on
  the fixed 7401-target frame (this runbook is the clean-frame
  foundation that one builds on).
