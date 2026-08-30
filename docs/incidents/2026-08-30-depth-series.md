# The depth series, and the five results that were not one

Two series of five evaluations were run on prediction set `9995651a` on
2026-08-30, at sequence depths 30, 20, 10, 5 and 2. The first is not a series.
Both are sealed under the same frame digest `f-1c245d41f26ff70c3b0a9247`, and
nothing in the record distinguishes them, which is why this document exists.

## The one that is not a series

Run 02:44 to 02:46. Result ids `67fe2fa0`, `28621824`, `9de60449`, `53d68960`,
`d0705d22`.

Every one returned the same numbers: NK.BPO 0.1486, NK.MFO 0.2902, NK.CCO
0.2760, PK.BPO 0.0582, at all five depths. The depth-2 frame holds 247,482 rows
and the depth-30 frame holds 2,441,584, an order of magnitude apart, so the
identity was the finding.

The cut was accepted, validated and dropped. Neither of the two places that
build a `WritePredictionsContext` passed `max_sequence_rank` through, so the
SELECT's `sequence_rank <= n` clause was unreachable, and
`assert_depth_was_applied` returned early whenever `max_k_position` was null,
which a run counting depth in sequences always leaves so. Fixed in PR #904.

**Four of the five carry a depth label they did not score.** They cannot be
told apart from the corrected series by the seal, they carry
`leakage_role='select'`, and there is no supported way to mark them.

## The one that is

Run 04:06 to 04:08, on `73acf81` with #904 merged. Result ids `095b81eb`,
`437b08a8`, `9f508370`, `3e1a47d8`, `aab5bfc6`.

| depth | NK.BPO | NK.MFO | NK.CCO | LK.BPO | PK.BPO |
|-------|--------|--------|--------|--------|--------|
| 30    | 0.1462 | 0.2870 | 0.2721 | 0.1725 | 0.0579 |
| 20    | 0.1546 | 0.3126 | 0.2815 | 0.1825 | 0.0599 |
| 10    | 0.1731 | 0.3310 | 0.3065 | 0.2043 | 0.0640 |
| 5     | 0.1848 | 0.3652 | 0.3268 | 0.2287 | 0.0706 |
| 2     | 0.2154 | 0.4104 | 0.3488 | 0.2437 | 0.0789 |

Monotone in all five cells with no exception: shallower is better, and the
winner is at the edge of what was measured.

**It is not a precision for recall trade.** Coverage holds at 1.000, 1.000,
1.000, 0.9987 and 0.996. In NK.BPO, depth 2 beats depth 30 on precision
(0.2602 against 0.2094) *and* on recall (0.3427 against 0.3139), and
`coverage_at_tau` rises from 0.6667 to 0.7992, so more proteins receive a
prediction above the threshold rather than fewer.

The cohort is identical at every depth (NK 3,754 per-protein rows, LK 2,978,
PK 12,303), so the only thing that moves is candidate depth.

`n_proteins` is not monotone (1006, 928, 1047, 991, 1206). It is a count at the
optimum threshold, which moves, so it is not a cohort size and a correlation
with it has no reading.

## What it means, and what it does not

It reproduces on the leak-free window, with the corrected donor policy,
sequence-based self-exclusion and depth counted in sequences, a law measured
before the 2026-08-27 wipe: depth is monotone, deeper is worse, and the winner
is always the edge, so there is no interior optimum.

The optimum therefore lies below 2, in the territory of K=1, which is a
different regime rather than another level of this axis: one donor per
(protein, aspect), announcing itself by two measured routes. The series does
not cross into it on purpose.

These five cannot share an axis with the 71 sealed results of the previous
campaign. Four axes moved between them, one of which changed meaning without
changing any recorded field.
