ADR-D34: Selective rerank resurrection, recompute not archaeology
=================================================================

:Status: Accepted
:Date: 2026-05-16 (proposed), 2026-05-17 (accepted with multi-seed numbers), 2026-05-18 (closure ratified by lab LR.4 + paired CI from LB.3)

.. note::

   **Terminology.** Earlier drafts called the pre-fix dataset
   "leakage-contaminated". That framing is technically inaccurate: the
   CAFA temporal partition (NK / LK / PK in
   :doc:`/architecture/evaluation`) is mathematically clean; PK simply
   records that the protein had experimental annotations in some
   namespace at t0, which is a legitimate evaluation split, not a leak.
   The actual incident was a **dataset-construction replication
   artefact** in the early ``export_research_dataset`` pipeline that
   made ``anc2vec_query_known_count`` act as a row-bucket identifier;
   see the *Replication artefact in the anc2vec_query feature family*
   entry in :doc:`/insights` for the corrected description. Wording in
   this ADR has been updated accordingly, except for frozen artefact
   identifiers (e.g. the lab axis value
   ``feat=v6_features+lineage-leakfree`` and the memory keys
   ``project_lb2_leakage_fixed_champion`` and
   ``project_anc2vec_leakage_mechanism``) where the literal string is
   load-bearing.

Context
-------

The reranker lab maintains validation bands for bench-v1-K5 across v220,
v226, and v230 lineage. The LAFA submission uses the v226-v230 band. The
historical "selective rerank at K=10" champion record (avg cafaeval
0.4562) existed only as a memory-only entry. That record predated the
range distinction and was not generated with explicit ``eval_set_name``
tracking.

Lab memory showed the legacy record as non-reproducible (pre-replication-fix) and of
unknown range provenance. The lab summary file under the replication-fixed
bench runs directory does
not contain 0.4562 for ``bench-v1-K5-v226-lineage-prostt5`` or any other current
validation band. The record was therefore not reproducible or
comparable to current champion runs.

On 2026-05-05 the anc2vec_query_known_count replication-artefact finding (memory
``project_anc2vec_leakage_mechanism``) confirmed that any pre-fix Fmax is
inflated and must be discarded; the only correct path is to recompute
on the replication-fixed feature set against the current bench.

On 2026-05-17 the LB.2 multi-seed sweep landed (lab branch
``task/bioinfo-quick-1778972872-6d9a``, commit ``77c3b33``):
6 NK+LK cells (nk-mfo, nk-bpo, nk-cco, lk-mfo, lk-bpo, lk-cco) trained
for 3 seeds each (42, 7, 137) on the replication-fixed bench-v1-K5-v226-lineage-prostt5 configuration
(no anc2vec, no PCA features; lambdarank; LR=0.05, leaves=63,
num_boost_round=10000, early_stop=100). The 9-cell selective policy
applies the reranker on NK+LK cells and falls back to KNN baseline on
PK cells, where lineage features induce a DAG-closure shortcut that
overfits.

Decision
--------

1. **Recompute, not archaeology.** When historical records conflict
   with, or cannot be reproduced on, current validation data,
   recompute on the current bench rather than reverse-engineer the
   old configuration.

2. **Selective rerank on NK+LK; KNN baseline fallback on PK.** This
   is the live PROTEA inference policy for ``bench-v1-K5-v226-lineage-prostt5``
   pending the next champion sweep.

3. **Configuration:** replication-fixed ``bench-v1-K5-v226-lineage-prostt5`` bundle
   (the ``v6_features`` bundle with lineage features enabled, minus all
   ``anc2vec_*`` and ``emb_pca_*`` columns), per-cell lambdarank
   LightGBM booster.

4. **Champion numbers (multi-seed, 2026-05-17):**

   .. list-table:: per-cell cafaeval Fmax, mean over seeds 42 / 7 / 137
      :header-rows: 1
      :widths: 12 14 14 14 14 14 18

      * - cell
        - s42
        - s7
        - s137
        - mean
        - 95% CI half
        - baseline (KNN)
      * - nk-mfo
        - 0.7112
        - 0.7041
        - 0.7041
        - 0.7065
        - 0.0036
        - 0.6447
      * - nk-bpo
        - 0.5599
        - 0.5571
        - 0.5618
        - 0.5596
        - 0.0024
        - 0.5333
      * - nk-cco
        - 0.7733
        - 0.7830
        - 0.7758
        - 0.7774
        - 0.0048
        - 0.7000
      * - lk-mfo
        - 0.6877
        - 0.6786
        - 0.6757
        - 0.6806
        - 0.0060
        - 0.5816
      * - lk-bpo
        - 0.6472
        - 0.6421
        - 0.6485
        - 0.6460
        - 0.0032
        - 0.5844
      * - lk-cco
        - 0.7434
        - 0.7252
        - 0.7417
        - 0.7367
        - 0.0091
        - 0.7053

   Aggregate numbers:

   - 6-cell NK+LK reranker avg: **0.6845**.
   - 9-cell selective avg (6 NK+LK rerank + 3 PK baseline fallback):
     **0.6215 ± 0.0014** (95% CI half-width on 9-cell mean, derived
     from a 10000-iteration bootstrap of the 3-seed mean per cell).
   - PK baseline values (used in fallback): pk-mfo 0.483, pk-bpo
     0.403, pk-cco 0.601.

5. **Supersedes the legacy memory-only 0.4562 record.** The legacy
   record (memory key ``project_v18_selective_rerank``) is retained
   for audit and explicitly marked as superseded. It is not
   comparable to the new champion: different feature set, different
   range, non-reproducible (pre-replication-fix).

6. **Deployment.** The replication-fixed bench-v1-K5-v226-lineage-prostt5 config becomes the PROTEA
   inference default for ``bench-v1-K5-v226-lineage-prostt5`` on NK+LK cells.
   Older ``RerankerModel`` rows from pre-replication-fix sweeps are
   considered stale; they remain in the registry for traceability but
   should not be selected for new inference jobs.

7. **FARM-EXP.10 closure.** The slice scope changed from "reconstruct
   axis tuple from RerankerModel table" to "re-train with current
   policy on current bench". This ADR records the multi-seed
   acceptance.

8. **Lab LR.4 closure (2026-05-18).** Lab PR
   ``protea-reranker-lab#21`` (``lab(LR.4): replication-artefact-free
   re-run of the historical selective-rerank policy``) formalises the
   supersession on the lab side: it lands
   ``scripts/lr4_v18_selective.py`` (regenerator reading
   ``runs/lb2_multiseed/cis.json`` or falling back to documented
   canonical numbers), ``experiments/lr4/v18_selective_delta.csv``
   (canonical acceptance artefact: per-cell selective rerank table
   plus aggregate delta row), and an ``EXPERIMENTS.md`` LR.4 closure
   section. The CSV reports the same-bench all-baseline reference
   (the pre-fix 0.4562 had no per-cell breakdown on file, so the lift
   is reported against the new bench baseline; the publishable
   selective-rerank lift on bench-v1-K5-v226-lineage-prostt5 is +0.0397
   over the same-bench KNN baseline). The legacy record is also
   marked superseded in the lab champions appendix.

9. **Paired CI evidence (LB.3, 2026-05-18).** Lab PR
   ``protea-reranker-lab#19`` (``lab(LB.3): per-cell paired CI on
   replication-fixed champion``) supplies the per-cell paired confidence
   intervals for the recomputed run. All 6 NK+LK cells are
   significant at the 95% level (6 of 6); see memory
   ``project_lb3_paired_ci_2026_05_18``. This is the statistical
   evidence supporting the deployment policy in points 2 and 6
   above.

Consequences
------------

**Positive**

- Eliminates the need to reverse-engineer unknown historical configs.
- Produces a valid, reproducible champion record with full range
  traceability (``eval_set_name`` pinned).
- Establishes a scalable pattern for future legacy-record conflicts:
  recompute on the current bench.
- All 6 NK+LK cells show strictly positive lift across all 3 seeds
  (max CI half-width 0.0091 on lk-cco). The selective policy is
  robust to seed variation.

**Negative**

- The legacy 0.4562 record is explicitly marked as not comparable to
  current champions. Any narrative claiming continuity with the old
  cell is incorrect.
- Requires regeneration of the cell, not mere documentation of an
  existing artefact.
- The catalog cell ``axis.features`` value for this replication-fixed
  bundle is not yet a named entry in the FARM-EXP.2 transversal catalog
  (see ``project_farm_exp_2_placeholder_digests``); a follow-up
  slice will add the replication-fixed bundle as a first-class axis
  value once the digest backfill clears.

**Neutral**

- Memory record ``project_v18_selective_rerank`` documents the
  historical value and its supersession. Future maintainers can
  cross-reference if needed.
- The 3 PK cells continue to ride the KNN baseline. A future slice
  (tracked separately) explores known-terms overlap features as a
  PK-specific lift signal.

References
----------

- Lab champion declaration: ``EXPERIMENTS.md`` in
  ``frapercan/protea-reranker-lab`` (FARM-EXP.10 champion section,
  LB.2 multi-seed sweep section, LR.4 closure section).
- LB.2 multi-seed sweep commit:
  ``protea-reranker-lab`` branch
  ``feat/FARM-EXP.10-transversal-champion`` (cherry-pick of
  ``77c3b33`` from ``task/bioinfo-quick-1778972872-6d9a``).
- Lab LR.4 closure PR: ``protea-reranker-lab#21`` (merged
  2026-05-18, commit ``1c17f75``); ships
  ``scripts/lr4_v18_selective.py``,
  ``experiments/lr4/v18_selective_delta.csv``, and the LR.4
  ``EXPERIMENTS.md`` closure section.
- Lab LB.3 paired CI PR: ``protea-reranker-lab#19`` (merged
  2026-05-18); per-cell paired confidence intervals (6 of 6 NK+LK
  cells significant at the 95% level).
- Memory entry ``project_lb2_leakage_fixed_champion`` (publishable
  numbers, 0.6215 plus or minus 0.0014 selective avg).
- Memory entry ``project_lb3_paired_ci_2026_05_18`` (paired CI
  evidence, 6 of 6 NK+LK cells significant).
- Memory entry ``project_anc2vec_leakage_mechanism`` (root cause for
  the supersession of 0.4562).
- Memory entry ``project_v18_selective_rerank`` (legacy champion,
  marked historical-only and superseded 2026-05-18).
- Memory entry ``feedback_no_archaeology_recompute`` (policy
  decision; canonical wording of the recompute-not-archaeology
  rule).
- Memory entry ``reference_lab_validation_ranges`` (v220 / v226 /
  v230 distinction).
- Memory entry ``project_farm_exp_2_placeholder_digests`` (catalog
  shortid tentativeness; relevant to the replication-fixed bundle's
  pending first-class axis registration).
- FARM-EXP.10 slice definition in
  ``agent-farm/plans/farm-platform/PLAN.md``.
- Lab summary file under the replication-fixed bench runs directory
  (current bench results).

Open follow-ups (deferred work, tracked separately):

- ``runs/transversal/<shortid>/`` placement of per-seed
  FARM-EXP.3-format ``run.json`` records is **deferred to the
  writer slice (FARM-EXP.5 and later)**, which is the slice that
  emits the ``axis`` block plus the ``fmax_samples`` array consumed
  by ``scripts/update_champions.py``. Until that slice lands, the
  champion declaration lives in the lab ``champions.md`` manual
  appendix (delimited by ``MANUAL_ENTRIES_BEGIN`` /
  ``MANUAL_ENTRIES_END`` markers) so that re-renders do not strip
  it. See lab PR ``protea-reranker-lab#15`` (FARM-EXP.10
  formalisation) and the LR.4 PR for the appendix-survival test
  cases.
- The replication-fixed bundle (the
  ``feat=v6_features+lineage-leakfree`` axis value: the base
  ``v6_features+lineage`` bundle minus ``anc2vec_*`` and
  ``emb_pca_*``) was added as a first-class entry in the
  FARM-EXP.2 transversal catalog via FARM-EXP.10b (lab PR
  ``protea-reranker-lab#16``); the digest-backfill slice that
  clears the placeholder shortids documented in
  ``project_farm_exp_2_placeholder_digests`` is the remaining
  prerequisite for stable axis-tuple identifiers.
