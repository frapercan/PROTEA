// Resolvers for the per-stratum cells an evaluation produces.
//
// The API serves one cell per stratum with the population it was pooled over,
// and a `reportable` flag saying whether that population cleared the floor.
// Everything here is pure so the panel stays presentation-only.
//
// The rule these encode: a cell is never shown without its population, and a
// withheld cell is never silently dropped. A table that prints only the cells
// that survived looks identical to a table that covered everything, and the
// difference is the whole point of stratifying.

export type StratumCell = {
  n_proteins: number;
  precision_w: number;
  recall_w: number;
  f_micro_w: number;
  reportable: boolean;
  // Axis columns are dynamic: whichever axes the run crossed.
  [axis: string]: string | number | boolean;
};

export type StrataResponse = {
  evaluation_result_id: string;
  axes: string[];
  settings: Record<string, StratumCell[]>;
};

/** Human labels for the axis values, so a column reads as prose. */
const AXIS_LABELS: Record<string, Record<string, string>> = {
  category: {
    NK: "no knowledge",
    LK: "limited knowledge",
    PK: "prior knowledge",
  },
  aspect: {
    P: "biological process",
    F: "molecular function",
    C: "cellular component",
  },
  homology: {
    none: "no donor",
    "<=30": "twilight (<=30%)",
    "30-60": "distant (30-60%)",
    "60-90": "close (60-90%)",
    ">90": "near-identical (>90%)",
  },
  donor_evidence: {
    exp: "experimental",
    other: "other evidence",
    none: "no donor",
  },
};

export function axisLabel(axis: string, value: string): string {
  return AXIS_LABELS[axis]?.[value] ?? value;
}

/** The axis a reader scans first, when the run crossed more than one. */
export function primaryAxis(axes: string[]): string | null {
  const preferred = [
    "homology",
    "length",
    "taxonomy",
    "donor_evidence",
    "aspect",
    "category",
  ];
  return preferred.find((a) => axes.includes(a)) ?? axes[0] ?? null;
}

export function cellKey(cell: StratumCell, axes: string[]): string {
  return axes.map((a) => String(cell[a] ?? "")).join(" / ");
}

/**
 * Cells in report order, withheld ones last.
 *
 * Sorted rather than filtered: the withheld ones stay visible and marked,
 * because their absence is itself a result about coverage.
 */
export function inReportOrder(
  cells: StratumCell[],
  axes: string[],
): StratumCell[] {
  return [...cells].sort((a, b) => {
    if (a.reportable !== b.reportable) return a.reportable ? -1 : 1;
    return cellKey(a, axes).localeCompare(cellKey(b, axes));
  });
}

/** How much of the population a view is not showing, as a share in [0,1]. */
export function withheldShare(cells: StratumCell[]): number {
  const total = cells.reduce((s, c) => s + c.n_proteins, 0);
  if (total === 0) return 0;
  const withheld = cells
    .filter((c) => !c.reportable)
    .reduce((s, c) => s + c.n_proteins, 0);
  return withheld / total;
}

/**
 * Colour ramp for a score, relative to the reportable cells beside it.
 *
 * Relative rather than absolute because f_micro_w over one stratum is not
 * comparable to a headline number: biological process at low homology reads
 * around 0.11 while molecular function at mid homology reads 0.46, and a fixed
 * scale would paint the whole BP column as failure.
 */
export function scoreShade(value: number, cells: StratumCell[]): string {
  const scores = cells.filter((c) => c.reportable).map((c) => c.f_micro_w);
  if (scores.length < 2) return "bg-slate-50";
  const lo = Math.min(...scores);
  const hi = Math.max(...scores);
  if (hi === lo) return "bg-slate-50";
  const t = (value - lo) / (hi - lo);
  if (t >= 0.8) return "bg-emerald-100";
  if (t >= 0.6) return "bg-emerald-50";
  if (t >= 0.4) return "bg-amber-50";
  if (t >= 0.2) return "bg-orange-50";
  return "bg-rose-50";
}

export function formatScore(value: number): string {
  return value.toFixed(4);
}
