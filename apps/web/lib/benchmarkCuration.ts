// Pure, reversible presentation curation for the benchmark matrix.
//
// The benchmark is recomputed continuously; rather than delete anything,
// the default /benchmark view hides clutter and a single "Show all" toggle
// brings every row back. None of these helpers mutate or drop DB data: they
// filter the in-memory matrix response the page already holds.
//
// Two row-level curation rules drive the default view (slice F-METHOD-
// EVAL-SURFACE provenance):
//
//   1. probe rows           : leakage_role === "probe" are diagnostic probes
//                             (ADR D40), never a headline number.
//   2. superseded internal  : frame === "internal" rows for a cell that also
//                             has a current LAFA-frame row are legacy; the
//                             LAFA-frame number is the one that ships.
//
// Both rules are no-ops while the provenance columns are null (the recompute
// is mid-flight), so the default view degrades gracefully to "show all" until
// LAFA-frame results land.

import type { BenchmarkBestCell, BenchmarkRow, PerTaskAggregate } from "./api";

/** Eval-set ids that are live or canonical and must never be treated as junk.
 *  Matched by uuid prefix so the short labels the UI already renders line up.
 *  817c6b9f = current SELECT 220->227, dd9f16ed = FINAL v227->v230,
 *  213e6ab6 = v226->v227, 2db7931a = v220->v226, 3b6f8064 = LAFA prod band. */
export const PRESERVED_EVAL_SET_PREFIXES: readonly string[] = [
  "817c6b9f",
  "dd9f16ed",
  "213e6ab6",
  "2db7931a",
  "3b6f8064",
];

export function isPreservedEvalSet(id: string): boolean {
  const lower = id.toLowerCase();
  return PRESERVED_EVAL_SET_PREFIXES.some((p) => lower.startsWith(p));
}

export function isProbeRole(role?: string | null): boolean {
  return role === "probe";
}

function cellGroupKey(embeddingConfigId: string, category: string, aspect: string): string {
  return `${embeddingConfigId}|${category}|${aspect}`;
}

/** (embedding|category|aspect) cells that carry at least one current
 *  LAFA-frame row. Internal-frame rows for those cells are superseded. */
export function lafaCellKeys(rows: BenchmarkRow[]): Set<string> {
  const out = new Set<string>();
  for (const r of rows) {
    if (r.frame === "lafa") {
      out.add(cellGroupKey(r.embedding_config_id, r.category, r.aspect));
    }
  }
  return out;
}

export function isSupersededInternal(
  prov: { frame?: string | null; embedding_config_id: string; category: string; aspect: string },
  lafaKeys: Set<string>,
): boolean {
  return (
    prov.frame === "internal" &&
    lafaKeys.has(cellGroupKey(prov.embedding_config_id, prov.category, prov.aspect))
  );
}

export type RowCuration = {
  rows: BenchmarkRow[];
  hiddenProbe: number;
  hiddenLegacy: number;
  hiddenTotal: number;
};

/** Default-view row curation. ``showAll`` short-circuits to the identity so
 *  the toggle is a true reveal-everything escape hatch. */
export function curateRows(rows: BenchmarkRow[], showAll: boolean): RowCuration {
  if (showAll) {
    return { rows, hiddenProbe: 0, hiddenLegacy: 0, hiddenTotal: 0 };
  }
  const lafaKeys = lafaCellKeys(rows);
  const kept: BenchmarkRow[] = [];
  let hiddenProbe = 0;
  let hiddenLegacy = 0;
  for (const r of rows) {
    if (isProbeRole(r.leakage_role)) {
      hiddenProbe += 1;
      continue;
    }
    if (isSupersededInternal(r, lafaKeys)) {
      hiddenLegacy += 1;
      continue;
    }
    kept.push(r);
  }
  return { rows: kept, hiddenProbe, hiddenLegacy, hiddenTotal: hiddenProbe + hiddenLegacy };
}

/** Eval-set ids that still have at least one row after curation. Used to keep
 *  the selector free of probe / test-only sets in the default view while never
 *  removing them from the underlying data. */
export function evalSetIdsWithRows(rows: BenchmarkRow[]): Set<string> {
  return new Set(rows.map((r) => r.evaluation_set_id));
}

/** Recompute the in-selection best-cell leaderboard from a curated row set so
 *  the leaderboard never points at a row the table no longer shows. Mirrors the
 *  backend ``_make_leaderboard`` (max IA-weighted ``primary`` per cell). */
export function bestPerCellFromRows(
  rows: BenchmarkRow[],
  categories: readonly string[],
  aspects: readonly string[],
): BenchmarkBestCell[] {
  const winners = new Map<string, BenchmarkRow>();
  for (const r of rows) {
    const key = `${r.category}|${r.aspect}`;
    const cur = winners.get(key);
    if (cur === undefined || r.primary > cur.primary) winners.set(key, r);
  }
  const out: BenchmarkBestCell[] = [];
  for (const cat of categories) {
    for (const asp of aspects) {
      const r = winners.get(`${cat}|${asp}`);
      if (r === undefined) continue;
      out.push({
        category: r.category,
        aspect: r.aspect,
        primary: r.primary,
        primary_metric: r.primary_metric,
        f_micro_w: r.f_micro_w,
        precision_w: r.precision_w,
        recall_w: r.recall_w,
        fmax: r.fmax,
        precision: r.precision,
        recall: r.recall,
        coverage: r.coverage,
        embedding_config_id: r.embedding_config_id,
        k: r.k,
        stage: r.stage,
        evaluation_result_id: r.evaluation_result_id,
        evaluation_set_id: r.evaluation_set_id,
        frame: r.frame,
        temporal_window: r.temporal_window,
        arms_enabled: r.arms_enabled,
        leakage_role: r.leakage_role,
      });
    }
  }
  return out;
}

/** Recompute the per-task mean / 95% CI headline from a curated row set.
 *  Mirrors the backend ``per_task_aggregate`` exactly: sample sd (n-1) with a
 *  normal-approx ``1.96 * sd / sqrt(n)`` half-width, rounded to 4 dp. */
export function perTaskFromRows(
  rows: BenchmarkRow[],
  categories: readonly string[],
  aspects: readonly string[],
): PerTaskAggregate[] {
  const grouped = new Map<string, number[]>();
  for (const r of rows) {
    const key = `${r.category}|${r.aspect}`;
    const bucket = grouped.get(key);
    if (bucket === undefined) grouped.set(key, [r.primary]);
    else bucket.push(r.primary);
  }
  const round4 = (n: number): number => Math.round(n * 10000) / 10000;
  const out: PerTaskAggregate[] = [];
  for (const cat of categories) {
    for (const asp of aspects) {
      const vals = grouped.get(`${cat}|${asp}`);
      if (vals === undefined || vals.length === 0) continue;
      const n = vals.length;
      const mean = vals.reduce((s, v) => s + v, 0) / n;
      let ci = 0;
      if (n > 1) {
        const variance = vals.reduce((s, v) => s + (v - mean) ** 2, 0) / (n - 1);
        ci = (1.96 * Math.sqrt(variance)) / Math.sqrt(n);
      }
      out.push({
        category: cat,
        aspect: asp,
        metric: "f_micro_w",
        mean: round4(mean),
        ci95: round4(ci),
        max: round4(Math.max(...vals)),
        min: round4(Math.min(...vals)),
        n_models: n,
      });
    }
  }
  return out;
}

/** Drop probe / superseded-internal entries from a best-cell leaderboard
 *  (e.g. the filter-agnostic global champions) in the default view. */
export function curateBestCells(
  cells: BenchmarkBestCell[],
  showAll: boolean,
  lafaKeys: Set<string>,
): BenchmarkBestCell[] {
  if (showAll) return cells;
  return cells.filter(
    (c) => !isProbeRole(c.leakage_role) && !isSupersededInternal(c, lafaKeys),
  );
}
