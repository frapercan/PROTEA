# LAFA evaluation parity

This note documents how PROTEA's `run_cafa_evaluation` operation is made
to measure the same thing as the LAFA (CAFA_forever) scoring pipeline, so
that the same prediction scored on either side yields the same headline
metric within rounding. It also lists the metrics that are now obsolete and
should be de-emphasized in the front-end.

## Headline metric

Both pipelines use the same `cafaeval` fork binary and the same headline
metric: `f_micro_w`, the IA-weighted micro-averaged F-measure, taken per
namespace (BPO / CCO / MFO) at the threshold that maximizes it. LAFA reads
it from column 31 of `evaluation_best_f_micro_w.tsv`; PROTEA reads it from
`dfs_best["f_micro_w"]` in `parse_results`. The metric choice was already
correct on both sides. The gap was in the invocation, not the metric.

## The mechanism (flags, ground truth, TOI, known, IA)

LAFA invokes cafaeval like this (from
`CAFA_forever/modules/local/evaluation.nf` and
`protea-lafa-knn/score_and_inject_windows.sh`):

```
cafaeval <OBO> <PREDS_DIR> <groundtruth_{NK,LK,PK}.tsv> \
  -ia <IA.tsv> -out_dir <...> \
  -toi <groundtruth_terms_of_interest.txt> \
  -prop fill -norm cafa -threads 4 -no_orphans
# PK additionally: -known <groundtruth_PK_known.tsv>
# No -max_terms flag (cafaeval default = unlimited).
# No -th_step flag (cafaeval default = 0.01).
```

PROTEA now calls `cafaeval.evaluation.cafa_eval` with the matching
arguments:

| cafaeval knob | LAFA | PROTEA (after fix) | notes |
| - | - | - | - |
| `prop` | `fill` | `fill` | already matched |
| `norm` | `cafa` | `cafa` | already matched |
| `no_orphans` | on | on | already matched |
| `ia` | t0 `IA.tsv` | payload `ia_file` | must be the same IA artifact |
| `exclude` / `-known` | PK only | PK only (`pk_known`) | already matched |
| `toi` | release `groundtruth_terms_of_interest.txt` | payload `toi_file`, else snapshot terms | see below |
| `th_step` | `0.01` (default) | `0.01` (was `0.001`) | dominant gap |
| `max_terms` | unlimited (default) | `None` (was `500`) | inert for KNN-style predictions |

### `th_step` (the dominant gap)

cafaeval sweeps the score threshold `tau` over `np.arange(th_step, 1,
th_step)` and reports the metric at its best tau. PROTEA previously used
`th_step=0.001`, a 10x finer grid than LAFA's default `0.01`. A finer grid
optimizes over more candidate thresholds, so it reports a systematically
higher `f_micro_w`. On the validated prediction the inflation reached
`+0.0144` (LK / BPO). The fix sets the default to `0.01`, payload-controlled
via `th_step`.

### `max_terms` (inert here, but aligned)

PROTEA previously capped predictions at the top 500 terms per protein per
namespace (`max_terms=500`); LAFA passes no cap. For PROTEA-KNN style
predictions no protein/namespace ever exceeds 500 terms, so the cap was
inert and removing it changed nothing on the validated prediction. It was
still removed (`max_terms=None` by default, payload-controlled) so the two
pipelines are mechanically identical and no future dense prediction is
silently truncated.

### `toi` (terms of interest)

`toi` restricts which terms count toward precision and recall. LAFA passes
a release-specific `groundtruth_terms_of_interest.txt`. That file is a
strict subset of the full ontology (about 38.6k of 48.2k terms on the
validated window) and a strict superset of the union of ground-truth terms;
it is the t0 propagated annotation universe (the terms for which information
accretion is defined). It is not reconstructable from the pivot ontology
snapshot or the IA file alone, and it is not the union of ground-truth
terms (using the gt union inflates the score, because it drops false
positives from the precision denominator).

PROTEA's default `toi` is every GO term in the pivot ontology snapshot.
Against LAFA's `toi` this leaves a small residual, concentrated in MFO
(about `-0.004` on the validated prediction). To get strict parity, pass
LAFA's exact `groundtruth_terms_of_interest.txt` through the new payload
field `toi_file`. When `toi_file` is omitted PROTEA keeps the snapshot-
derived behavior (documented as a small, MFO-biased over/under count
relative to LAFA).

## Parity validation

Validated offline (no PROTEA stack) by calling `cafa_eval` directly on a
prediction LAFA had already scored:

- Prediction: `protea-lafa-knn/predictions_7401.tsv` (LAFA method
  `PROTEA-KNN`, file `protea-knn-v1.tsv`).
- Window: `Sep_2025_Mar_2026` (v227 LAFA band).
- Inputs (identical to LAFA): OBO + IA from `lafa_t0_Sep_2025`, ground
  truth + TOI + PK-known from
  `CAFA_forever/data/releases/Sep_2025_Mar_2026`.
- LAFA reference: `results_{NK,LK,PK}/evaluation_best_f_micro_w.tsv`
  (reported to 3 decimals).

`f_micro_w` per tier and aspect:

| tier / ns | LAFA ref | PROTEA before (th_step=0.001, max_terms=500) | delta | PROTEA after (th_step=0.01, LAFA TOI) | delta |
| - | - | - | - | - | - |
| NK / BPO | 0.263 | 0.2641 | +0.0011 | 0.2626 | -0.0004 |
| NK / CCO | 0.407 | 0.4108 | +0.0038 | 0.4067 | -0.0003 |
| NK / MFO | 0.579 | 0.5864 | +0.0074 | 0.5790 | +0.0000 |
| LK / BPO | 0.284 | 0.2984 | +0.0144 | 0.2843 | +0.0003 |
| LK / CCO | 0.332 | 0.3363 | +0.0043 | 0.3324 | +0.0004 |
| LK / MFO | 0.489 | 0.4949 | +0.0059 | 0.4893 | +0.0003 |
| PK / BPO | 0.075 | 0.0761 | +0.0011 | 0.0753 | +0.0003 |
| PK / CCO | 0.184 | 0.1852 | +0.0012 | 0.1840 | +0.0000 |
| PK / MFO | 0.191 | 0.1908 | -0.0002 | 0.1906 | -0.0004 |

After the fix every delta is below `1e-3`, within LAFA's own 3-decimal
rounding (epsilon about `5e-4`). With the `th_step` fix but PROTEA's default
snapshot-derived TOI (no `toi_file`), the residual is at most `0.0039`,
concentrated in MFO; passing LAFA's exact TOI closes it.

The `th_step` change is the load-bearing fix for parity. The `max_terms`
change is a no-op on these predictions and a safety alignment. The `toi`
field is required only for the last sub-`1e-3` MFO residual.

## How to run a LAFA-comparable evaluation

In the `run_cafa_evaluation` payload:

- leave `th_step` and `max_terms` at their defaults (`0.01` and `None`),
- pass `ia_file` pointing at the same IA artifact LAFA used,
- pass `toi_file` pointing at LAFA's release
  `groundtruth_terms_of_interest.txt` for strict MFO parity.

## Obsolete metrics (front-end guidance)

The front-end should label these as deprecated and de-emphasize them, with
a short "why these numbers" note:

- Unweighted `fmax` / `f_micro` (and `precision` / `recall` at the
  unweighted-optimal threshold). These weight every GO term equally, so
  common, easy, high-frequency terms dominate and the score is inflated and
  not comparable to LAFA. They are superseded by the IA-weighted
  `f_micro_w` (and `fmax_w`), which is the headline LAFA metric.
- v226-band numbers. The benchmark was historically cut at GOA v226; the
  deployed LAFA band is v227 to v230. v226 numbers are superseded by v227
  LAFA-band numbers and should be flagged as a different (stale) evaluation
  window, not compared head to head with LAFA results.

Front-end note suggestion: "Headline metric is IA-weighted f_micro_w on the
v227 LAFA band, matching LAFA's scorer. Unweighted fmax / f_micro and
v226-band numbers are kept for history but are not LAFA-comparable."
