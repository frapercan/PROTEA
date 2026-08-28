// What each stored metric actually is, in one place.
//
// Every evaluation cell already carries twelve figures, and the surface used to
// show one of them as a bare number. Two independent axes decide what a figure
// means, and mixing them silently is how a table becomes uncomparable with
// itself:
//
//   averaging   MACRO averages the per-protein score, so every protein counts
//               once whatever its number of terms. MICRO sums tp / fp / fn over
//               the whole population first, so a protein with fifty terms
//               weighs fifty times a protein with one.
//
//   weighting   PLAIN counts every GO term alike. IA-WEIGHTED weights each term
//               by its information accretion, so a rare, specific term counts
//               more than a common one.
//
// THE TWO BOARDS DO NOT USE THE SAME AVERAGE, and for a long time this file said
// they did. Verified 2026-08-28:
//
//   CAFA   Fmax, protein-centric. Precision and recall are averaged OVER
//          PROTEINS at each threshold and the F of those averages is maximised.
//          cafaeval computes it under normalization='cafa', which is its default
//          and the one this project uses, so `fmax` and `fmax_w` in every stored
//          row already ARE the CAFA metric.
//   LAFA   f_micro_w, the IA-weighted micro. Read directly from LAFA's own
//          output file, evaluation_best_f_micro_w.tsv column 31, in
//          docs/EVAL_LAFA_PARITY.md.
//
// So a number is comparable to ONE board, never to both at once, and which one
// depends on which average it carries. On this campaign's own data the choice is
// not cosmetic: micro and macro name a different best depth in 27 of 72 series,
// and depth declines monotonically in 60 of 72 series under micro against 38 of
// 72 under macro. A metric that changes the answer is a condition of the result,
// not a convention of reading.
//
// Both axes are real and both are useful, which is why all four combinations
// are kept and named rather than one being picked for the reader.

/**
 * The metric the surface leads with, matching the API's own choice.
 *
   * f_micro_w because it is LAFA's headline, and LAFA is the board this
   * campaign submits to. It is NOT CAFA's: CAFA scores protein-centric Fmax,
   * which is `fmax_w`, stored alongside in the same row. Named as a CHOICE
   * because on this campaign's own data it changes which depth wins in 27 of
   * 72 series. Kept here so the front end and protea/api/metrics.py cannot
   * drift into disagreeing about the default.
 */
export const PRIMARY_METRIC = "f_micro_w";

export type Averaging = "macro" | "micro";
export type Weighting = "plain" | "ia";

export type MetricSpec = {
  key: string;
  label: string;
  averaging: Averaging | null;
  weighting: Weighting;
  /** One sentence a reader can act on, not a restatement of the name. */
  meaning: string;
};

/**
 * Every metric an evaluation cell carries.
 *
 * `fmax` and `fmax_w` are maximised over the decision threshold; the others are
 * reported at that same threshold, which is why `tau` travels with them.
 */
export const METRICS: MetricSpec[] = [
  {
    key: "f_micro_w",
    label: "F micro, IA-weighted",
    averaging: "micro",
    weighting: "ia",
    meaning:
      "This project's headline, and LAFA's. Sums the confusion matrix over the whole population, weighting each GO term by how specific it is, so a protein with fifty gained terms weighs fifty times one with a single term. Not CAFA's metric: that is fmax_w, in the same row.",
  },
  {
    key: "fmax_w",
    label: "F-max macro, IA-weighted",
    averaging: "macro",
    weighting: "ia",
    meaning:
      "CAFA's metric. The best per-protein F over the threshold sweep, with rare terms counting more. Every protein counts once, so a protein with one annotation weighs as much as one with fifty. This is the number to quote against a CAFA leaderboard, not f_micro_w.",
  },
  {
    key: "f_micro",
    label: "F micro, unweighted",
    averaging: "micro",
    weighting: "plain",
    meaning:
      "Micro-averaged F treating every GO term alike. Higher than the IA-weighted figure whenever the easy predictions are the common, unspecific terms.",
  },
  {
    key: "fmax",
    label: "F-max macro, unweighted",
    averaging: "macro",
    weighting: "plain",
    meaning:
      "The classic CAFA F-max: per-protein F averaged over proteins, maximised over the threshold, with every term counting the same.",
  },
  {
    key: "precision_w",
    label: "precision, IA-weighted",
    averaging: null,
    weighting: "ia",
    meaning:
      "Of the information predicted, how much was right, at the reported threshold.",
  },
  {
    key: "recall_w",
    label: "recall, IA-weighted",
    averaging: null,
    weighting: "ia",
    meaning:
      "Of the information that should have been predicted, how much was found.",
  },
  {
    key: "precision",
    label: "precision, unweighted",
    averaging: null,
    weighting: "plain",
    meaning:
      "Of the terms predicted, the share that were correct, at the reported threshold.",
  },
  {
    key: "recall",
    label: "recall, unweighted",
    averaging: null,
    weighting: "plain",
    meaning:
      "Of the terms that should have been predicted, the share that were found.",
  },
  {
    key: "coverage_w",
    label: "coverage, IA-weighted",
    averaging: null,
    weighting: "ia",
    meaning:
      "The share of evaluable proteins the method predicted anything for. A high score over low coverage describes a method that answered only the easy cases.",
  },
  {
    key: "coverage",
    label: "coverage",
    averaging: null,
    weighting: "plain",
    meaning:
      "The share of evaluable proteins the method predicted anything for.",
  },
];

const BY_KEY = new Map(METRICS.map((m) => [m.key, m]));

export function metricSpec(key: string): MetricSpec | undefined {
  return BY_KEY.get(key);
}

/**
 * The label to print beside a number, never the bare key.
 *
 * Falls back to the key itself so a metric added upstream renders as its own
 * name rather than as blank or as somebody else's label.
 */
export function metricLabel(key: string): string {
  return BY_KEY.get(key)?.label ?? key;
}

/** Full sentence for a tooltip: what it is, then what it means. */
export function metricTooltip(key: string): string {
  const spec = BY_KEY.get(key);
  if (!spec) return key;
  const axes = [
    spec.averaging ? `${spec.averaging}-averaged` : null,
    spec.weighting === "ia" ? "IA-weighted" : "unweighted",
  ]
    .filter(Boolean)
    .join(", ");
  return `${spec.key} (${axes}). ${spec.meaning}`;
}

/**
 * True when the cell reports a figure that is NOT IA-weighted.
 *
 * Worth flagging on the surface: an evaluation run without an
 * information-accretion set does not compute the weighted variants at all, so
 * the cell silently falls back to the plain figure and stops being comparable
 * to the leaderboards, with nothing in the number to say so.
 */
export function isUnweighted(key: string): boolean {
  return BY_KEY.get(key)?.weighting === "plain";
}

/** Metrics grouped for a picker: the four F scores first, then the parts. */
export function metricGroups(): { title: string; keys: string[] }[] {
  return [
    { title: "F scores", keys: ["f_micro_w", "fmax_w", "f_micro", "fmax"] },
    {
      title: "precision and recall",
      keys: ["precision_w", "recall_w", "precision", "recall"],
    },
    { title: "coverage", keys: ["coverage_w", "coverage"] },
  ];
}

/**
 * The value of one metric on a row, with the reason it may be absent.
 *
 * Absent is a real answer here: an evaluation run without an
 * information-accretion set does not compute the weighted variants at all, so
 * `f_micro_w` is missing rather than zero. Returning null keeps the surface
 * able to say "not computed for this run" instead of drawing a zero that reads
 * as a terrible score.
 */
export function metricValue(
  row: Record<string, unknown>,
  key: string,
): number | null {
  const raw = row[key];
  return typeof raw === "number" && Number.isFinite(raw) ? raw : null;
}

/** Rank rows by a metric, putting rows that lack it last rather than at zero. */
export function byMetricDesc(key: string) {
  return (a: Record<string, unknown>, b: Record<string, unknown>): number => {
    const av = metricValue(a, key);
    const bv = metricValue(b, key);
    if (av === null && bv === null) return 0;
    if (av === null) return 1;
    if (bv === null) return -1;
    return bv - av;
  };
}
