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
//               more than a common one. This is the CAFA / LAFA convention and
//               the only variant comparable to those leaderboards.
//
// Both axes are real and both are useful, which is why all four combinations
// are kept and named rather than one being picked for the reader.

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
      "The headline metric of this project and of the CAFA / LAFA leaderboards. Sums the confusion matrix over the whole population, weighting each GO term by how specific it is.",
  },
  {
    key: "fmax_w",
    label: "F-max macro, IA-weighted",
    averaging: "macro",
    weighting: "ia",
    meaning:
      "The best per-protein F over the threshold sweep, with rare terms counting more. Every protein counts once, so a protein with one annotation weighs as much as one with fifty.",
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
