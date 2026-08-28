// Who wins inside one stratum, decided apart from how it draws.
//
// The strata panel answers "how did this arm do across strata". This is the
// transpose, and it is the question the data says matters. Measured on rung
// 1, spread across the eight representations per cell:
//
//   by identity band   0.1316 twilight, 0.0388 distant, 0.0293 close, 0.0155 near-identical
//   by length band     0.0323, 0.0388, 0.1336 (two cells)
//
// Homology is monotone over a factor of 8.5. Length ties, and the largest
// spread inside each length band is the twilight maximum, so what length
// appears to carry is homology's variation scattered. A reader given only
// the global table sees 0.0188 and concludes the representation barely
// matters, which is true where a near-identical donor exists and false
// where one does not.

export type StratumRow = {
  evaluation_result_id: string;
  model: string;
  display_name: string;
  k: number;
  // What the arm did downstream of the retrieval. Optional because an older
  // API build does not send them, and a surface that assumed otherwise would
  // print "undefined" where it means "this build cannot say". Eight arms of
  // this campaign share a prediction set and differ only here, so a table
  // naming an arm by its model alone shows eight identical labels at eight
  // different scores.
  scoring_name?: string | null;
  donor_policy?: string | null;
  metric?: string | null;
  n_proteins: number;
  f_micro_w: number;
  precision_w?: number;
  recall_w?: number;
  reportable?: boolean;
  category?: string;
  aspect?: string;
  length?: string;
  homology?: string;
};

export type StratumCompare = {
  evaluation_set_id: string;
  setting: string;
  where: Record<string, string>;
  arms_total: number;
  arms_with_strata: number;
  rows: StratumRow[];
};

/** Identity bands in the order they mean, floor first. */
export const HOMOLOGY_BANDS = ["none", "<=30", "30-60", "60-90", ">90"] as const;

/**
 * Arms ranked inside a stratum, best first.
 *
 * Withheld cells are dropped rather than ranked: a cell below the
 * population floor has a number and not a measurement, and putting it in a
 * ranking is how it gets quoted.
 */
export function rankArms(rows: StratumRow[]): StratumRow[] {
  return rows
    .filter((r) => r.reportable !== false)
    .slice()
    .sort((a, b) => b.f_micro_w - a.f_micro_w);
}

export type Spread = {
  best: StratumRow;
  worst: StratumRow;
  value: number;
  /** Arms the spread is over. Two is not a spread worth printing. */
  arms: number;
};

/**
 * How far apart the arms are inside a stratum.
 *
 * Null under three arms. With two, best minus worst is just the difference
 * between two numbers, and calling it a spread invites comparing it to one
 * computed over eight.
 */
export function spreadOf(rows: StratumRow[], minArms = 3): Spread | null {
  const ranked = rankArms(rows);
  if (ranked.length < minArms) return null;
  const best = ranked[0];
  const worst = ranked[ranked.length - 1];
  return {
    best,
    worst,
    value: best.f_micro_w - worst.f_micro_w,
    arms: ranked.length,
  };
}

/**
 * Rows for one stratum out of a whole-setting fetch.
 *
 * The endpoint can pin the axes server-side, but pinning them there costs a
 * request per stratum and the reader moves between strata constantly. One
 * fetch, filtered here.
 */
export function atStratum(
  rows: StratumRow[],
  where: Partial<Record<"category" | "aspect" | "length" | "homology", string>>,
): StratumRow[] {
  const pins = Object.entries(where).filter(([, v]) => v != null) as [
    keyof StratumRow,
    string,
  ][];
  return rows.filter((r) => pins.every(([axis, v]) => String(r[axis] ?? "") === v));
}

/** Values present on an axis, in their own order where they have one. */
export function bandsPresent(rows: StratumRow[], axis: keyof StratumRow): string[] {
  const seen = new Set(rows.map((r) => String(r[axis] ?? "")).filter(Boolean));
  if (axis === "homology") return HOMOLOGY_BANDS.filter((b) => seen.has(b));
  return [...seen].sort();
}

/**
 * The sentence the panel exists to let a reader write.
 *
 * Deliberately not a verdict. It states the spread and the population and
 * leaves the reading to the reader, because the same number means
 * different things at 550 proteins and at 32.
 */
export function spreadSentence(band: string, s: Spread | null): string {
  if (!s) return "too few arms here to compare";
  const pop = s.best.n_proteins;
  return `${s.arms} arms span ${s.value.toFixed(4)} in ${band}, over ${pop} proteins`;
}
