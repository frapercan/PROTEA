// How many proteins a benchmark score was computed over, and when that count
// makes the score incomparable to the ones beside it.
//
// WHY THIS EXISTS. The strata surface already refuses to print a cell without
// its population, on the grounds that "a table that prints only the cells that
// survived looks identical to a table that covered everything" (see
// `lib/strata.ts`). The benchmark matrix carries the same field and never drew
// it. On 2026-08-23 a study selected arms from this data and one of them had
// been scored on 106 proteins where its neighbours had roughly 680. It ranked
// like any other row, and the resulting anomaly was mistaken for a bug in the
// scorer before the count was looked at.
//
// A caveat that has to travel with the number. `n_proteins` is the count of
// proteins carrying a prediction AT THE THRESHOLD WHERE THE METRIC MAXIMISED,
// not the size of the evaluated cohort. It moves with the operating point as
// well as with coverage: the platform records a 0.98 -> 0.99 step in that
// threshold moving the count by 17 per cent across 32 runs whose cohort was
// identical (see `protea/core/operations/_run_cafa_artifacts.py`). So a low
// count is a reason to look, never on its own a verdict, and nothing here
// withholds a row or reorders one. It marks, and the reader decides.

/** The minimum share of its neighbours' median a row's count may hold before
 *  the row is marked. Half is deliberately loose: it clears the ordinary
 *  threshold-driven wobble the note above describes, and still catches the
 *  106-against-680 case that motivated this by a wide margin. */
export const POPULATION_SHARE_FLOOR = 0.5;

type WithPopulation = { n_proteins?: number | null };

/** Median of the counts present, or null when no row carries one. */
export function medianPopulation(rows: WithPopulation[]): number | null {
  const counts = rows
    .map((r) => r.n_proteins)
    .filter((n): n is number => typeof n === "number" && Number.isFinite(n) && n > 0)
    .sort((a, b) => a - b);
  if (counts.length === 0) return null;
  const mid = Math.floor(counts.length / 2);
  return counts.length % 2 === 1
    ? counts[mid]
    : (counts[mid - 1] + counts[mid]) / 2;
}

export type PopulationNote = {
  /** The row's own count, or null when the row does not carry one. */
  count: number | null;
  /** The median across the rows it is being compared against. */
  median: number | null;
  /** True when the count is present and far enough below the median that the
   *  row is not comparable to its neighbours on face value. */
  underpopulated: boolean;
  /** Ready-to-render sentence, or null when there is nothing to say. */
  label: string | null;
};

/**
 * What to say about one row's population, in the context of the rows it sits
 * beside.
 *
 * A single row has no neighbours to be out of step with, so it reports its
 * count and never marks. A row with no count at all reports that absence
 * rather than inventing a number, because "not recorded" and "small" are
 * different facts and only one of them is a reason to distrust the score.
 */
export function populationNote(
  row: WithPopulation,
  peers: WithPopulation[],
): PopulationNote {
  const count =
    typeof row.n_proteins === "number" && Number.isFinite(row.n_proteins)
      ? row.n_proteins
      : null;
  const median = medianPopulation(peers);

  if (count == null) {
    return { count: null, median, underpopulated: false, label: "population not recorded" };
  }

  const comparable = peers.length > 1 && median != null && median > 0;
  const underpopulated = comparable && count < median * POPULATION_SHARE_FLOOR;

  const base = `over ${count.toLocaleString()} proteins`;
  return {
    count,
    median,
    underpopulated,
    label: underpopulated
      ? `${base}, against a median of ${median!.toLocaleString()} here`
      : base,
  };
}
