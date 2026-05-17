ADR-D34: Selective rerank resurrection, recompute not archaeology
=================================================================

:Status: Accepted
:Date: 2026-05-16 (proposed), 2026-05-17 (accepted with multi-seed numbers)

Context
-------

The reranker lab maintains validation bands for bench-v1-K5 across v220,
v226, and v230 lineage. The LAFA submission uses the v226-v230 band. The
historical "selective rerank at K=10" champion record (avg cafaeval
0.4562) existed only as a memory-only entry. That record predated the
range distinction and was not generated with explicit ``eval_set_name``
tracking.

Lab memory showed the legacy record as leakage-contaminated and of
unknown range provenance. The lab summary file under the leakage-fixed
bench runs directory does
not contain 0.4562 for ``bench-v1-K5-v226-lineage`` or any other current
validation band. The record was therefore not reproducible or
comparable to current champion runs.

On 2026-05-05 the anc2vec count-leakage finding (memory
``project_anc2vec_count_leakage``) confirmed that any pre-fix Fmax is
inflated and must be discarded; the only correct path is to recompute
on the leakage-fixed feature set against the current bench.

On 2026-05-17 the LB.2 multi-seed sweep landed (lab branch
``task/bioinfo-quick-1778972872-6d9a``, commit ``77c3b33``):
6 NK+LK cells (nk-mfo, nk-bpo, nk-cco, lk-mfo, lk-bpo, lk-cco) trained
for 3 seeds each (42, 7, 137) on the leakage-fixed bench-v1-K5-v226-lineage configuration
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
   is the live PROTEA inference policy for ``bench-v1-K5-v226-lineage``
   pending the next champion sweep.

3. **Configuration:** leakage-fixed ``bench-v1-K5-v226-lineage`` bundle
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
   range, leakage-contaminated.

6. **Deployment.** The leakage-fixed bench-v1-K5-v226-lineage config becomes the PROTEA
   inference default for ``bench-v1-K5-v226-lineage`` on NK+LK cells.
   Older ``RerankerModel`` rows from pre-leakage-fix sweeps are
   considered stale; they remain in the registry for traceability but
   should not be selected for new inference jobs.

7. **FARM-EXP.10 closure.** The slice scope changed from "reconstruct
   axis tuple from RerankerModel table" to "re-train with current
   policy on current bench". This ADR records the multi-seed
   acceptance.

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
- The catalog cell ``axis.features`` value for this leakage-fixed
  bundle is not yet a named entry in the FARM-EXP.2 transversal catalog
  (see ``project_farm_exp_2_placeholder_digests``); a follow-up
  slice will add the leakage-fixed bundle as a first-class axis
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
  LB.2 multi-seed sweep section).
- LB.2 multi-seed sweep commit:
  ``protea-reranker-lab`` branch
  ``feat/FARM-EXP.10-transversal-champion`` (cherry-pick of
  ``77c3b33`` from ``task/bioinfo-quick-1778972872-6d9a``).
- Memory entry ``project_lb2_leakage_fixed_champion`` (publishable
  numbers).
- Memory entry ``project_anc2vec_count_leakage`` (root cause for
  the supersession of 0.4562).
- Memory entry ``project_v18_selective_rerank`` (legacy champion,
  marked superseded).
- Memory entry ``feedback_no_archaeology_recompute`` (policy
  decision).
- Memory entry ``reference_lab_validation_ranges`` (v220 / v226 /
  v230 distinction).
- Memory entry ``project_farm_exp_2_placeholder_digests`` (catalog
  shortid tentativeness).
- FARM-EXP.10 slice definition.
- Lab summary file under the leakage-fixed bench runs directory
  (current bench results).
