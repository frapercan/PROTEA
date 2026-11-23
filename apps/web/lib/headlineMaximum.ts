// Where a cell's headline was read on the threshold grid, and whether that is an edge.
//
// cafaeval reports each F at the threshold where its curve peaks, on a grid of
// step 0.01. When the curve peaks at the last threshold that kept a prediction,
// the reported value is that edge and not a maximum: the maximum may lie beyond
// the grid. The evaluator records, per cell and per F variant, the threshold of
// the maximum and whether it sits at the top or bottom edge (`maxima`, ADR-D55
// section 6), and a maximum at an edge blocks a verdict.
//
// The cell's own `tau` key is NOT the headline's threshold. It belongs to the
// unweighted Fmax, and the two differ: on 211->227, LK CCO reads 0.97 there and
// 0.99 for f_micro_w.

export type Maximum = { tau: number; edge: "top" | "bottom" | null };

type WithMaxima = { maxima?: Record<string, Maximum | undefined> };

/** Where the headline f_micro_w was read, or null for a result that records no maxima. */
export function headlineMaximum(cell: WithMaxima): Maximum | null {
  const m = cell.maxima?.f_micro_w;
  return m && typeof m.tau === "number" ? m : null;
}

/** The mark beside the figure: one at either edge, none for an interior maximum. */
export function edgeMark(m: Maximum | null): string {
  if (m?.edge === "top") return "▴";
  if (m?.edge === "bottom") return "▾";
  return "";
}

/** What a tooltip says about the threshold the headline was read at. */
export function maximumNote(m: Maximum): string {
  const at = `f_micro_w reported at threshold ${m.tau.toFixed(2)}`;
  if (m.edge === "top") {
    return `${at}, the highest threshold that kept a prediction: the curve peaks at the edge of the grid, so the figure is an edge and not a maximum, and no verdict is read from it (ADR-D55)`;
  }
  if (m.edge === "bottom") {
    return `${at}, the lowest threshold of the grid: the curve peaks at its edge, so the figure is an edge and not a maximum, and no verdict is read from it (ADR-D55)`;
  }
  return `${at}, an interior maximum`;
}
