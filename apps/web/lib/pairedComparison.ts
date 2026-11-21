// The paired comparisons of a window's results, as the window page shows them.
//
// compare_paired_panels writes no row of its own; its job is its record, and the
// API reads the latest whole-panel comparison of each pair of the window's
// results from those jobs. Per panel it keeps the difference of f_micro_w (the
// arm under test minus the baseline), its interval, the interval's method, the
// verdict and the number of proteins both arms were paired on.

export type PanelComparison = {
  delta: number | null;
  ci_low: number | null;
  ci_high: number | null;
  interval_method: string | null;
  verdict: string | null;
  status: string | null;
  n_paired: number | null;
};

export type PairedComparison = {
  job_id: string;
  evaluation_result_id: string;
  baseline_evaluation_result_id: string;
  finished_at: string | null;
  method_axis: string[] | null;
  operating_point: string | null;
  weighting: string | null;
  n_resamples: number | null;
  seed: number | null;
  confidence: number | null;
  panels: Record<string, PanelComparison>;
};

/** What a result's evaluated set is: its model, the scoring it read, its method. */
export type Arm = { model: string | null; scoring: string | null; method: string | null };

/** A set's arm in a few words: the model's own name and the scoring, or the method. */
export function armLabel(arm: Arm | null | undefined): string | null {
  if (!arm) return null;
  const model = arm.model ? arm.model.split("/").pop() ?? arm.model : null;
  const parts = [model ?? arm.method, arm.scoring].filter((p): p is string => !!p);
  return parts.length ? parts.join(" · ") : null;
}

/** The baselines the comparisons were run against, the most used first. */
export function baselinesOf(comparisons: PairedComparison[]): string[] {
  const counts = new Map<string, number>();
  for (const c of comparisons) {
    counts.set(c.baseline_evaluation_result_id, (counts.get(c.baseline_evaluation_result_id) ?? 0) + 1);
  }
  return [...counts.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])).map(([id]) => id);
}

const signed = (v: number) => `${v >= 0 ? "+" : "−"}${Math.abs(v).toFixed(3)}`;

/** The difference and its interval, as a cell prints them: "+0.012 [+0.004, +0.020]". */
export function deltaText(p: PanelComparison): string {
  if (p.delta == null) return "—";
  const interval = p.ci_low != null && p.ci_high != null ? ` [${signed(p.ci_low)}, ${signed(p.ci_high)}]` : "";
  return `${signed(p.delta)}${interval}`;
}

/** Which way a panel came out: the arm higher, the baseline higher, or neither resolved. */
export function verdictTone(p: PanelComparison): "higher" | "lower" | "unresolved" {
  if (p.verdict === "a_greater") return "higher";
  if (p.verdict === "b_greater") return "lower";
  return "unresolved";
}

/** Everything a cell's tooltip says about one panel of one comparison. */
export function panelNote(p: PanelComparison, c: PairedComparison): string {
  const level = c.confidence != null ? `${Math.round(c.confidence * 100)} %` : "";
  const method = p.interval_method === "bca" ? "BCa" : (p.interval_method ?? "");
  const parts = [`f_micro_w of the arm minus the baseline: ${deltaText(p)}`];
  if (level || method) parts.push(`${level} ${method} interval`.trim());
  if (p.n_paired != null) parts.push(`${p.n_paired.toLocaleString("en-US")} paired proteins`);
  parts.push(`verdict: ${p.verdict ?? "none"}${p.status && p.status !== "ok" ? ` (${p.status})` : ""}`);
  if (c.n_resamples != null) parts.push(`${c.n_resamples.toLocaleString("en-US")} resamples, seed ${c.seed}`);
  if (c.operating_point) parts.push(`operating point ${c.operating_point.replaceAll("_", " ")}`);
  if (c.method_axis) parts.push(c.method_axis.length ? `varies ${c.method_axis.join(", ")}` : "method held still");
  return parts.join(" · ");
}
