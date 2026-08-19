// Pure helpers for the benchmark surface: formatting, indexing and defaults.
//
// Split out of the page, which had grown to 1,393 lines with a 1,133-line
// component inside it. None of this needs React, and keeping it here means the
// rounding rules and the cell-key convention can be tested directly rather than
// through a rendered table.

import type {
  BenchmarkBestCell,
  BenchmarkRow,
  BenchmarkStage,
} from "@/lib/api";

export function formatParams(n: number | null): string {
  if (n == null) return "";
  if (n >= 1_000_000_000) {
    const v = n / 1_000_000_000;
    return v >= 10 ? `${Math.round(v)}B` : `${v.toFixed(1)}B`;
  }
  if (n >= 1_000_000) return `${Math.round(n / 1_000_000)}M`;
  return `${n}`;
}

export function formatProteins(n: number | undefined): string {
  if (n == null) return "";
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

export function cellKey(eid: string, cat: string, asp: string): string {
  return `${eid}|${cat}|${asp}`;
}

/** Format the optional fmax_std as a compact "± 0.003" suffix. Returns
 *  an empty string when the std is missing, so callers can append
 *  unconditionally without conditional whitespace. */
export function formatStd(std: number | null | undefined): string {
  if (std == null || !Number.isFinite(std)) return "";
  return `± ${std.toFixed(3)}`;
}

/** True when the row carries both bootstrap bounds. The CI band only
 *  renders when both are present and form a valid interval. */
export function hasCiBand(row: BenchmarkRow): boolean {
  return (
    row.fmax_ci_low != null &&
    row.fmax_ci_high != null &&
    Number.isFinite(row.fmax_ci_low) &&
    Number.isFinite(row.fmax_ci_high) &&
    row.fmax_ci_high >= row.fmax_ci_low
  );
}

/** Index rows by (embedding, cat, asp) for O(1) cell lookup. The matrix
 *  endpoint already dedupes to a single best row per tuple. */
export function indexRows(rows: BenchmarkRow[]): Map<string, BenchmarkRow> {
  const out = new Map<string, BenchmarkRow>();
  for (const r of rows) {
    out.set(cellKey(r.embedding_config_id, r.category, r.aspect), r);
  }
  return out;
}

/** Index the leaderboard by (cat, asp) so the table can highlight winners. */
export function indexBestPerCell(
  cells: BenchmarkBestCell[],
): Map<string, BenchmarkBestCell> {
  const out = new Map<string, BenchmarkBestCell>();
  for (const c of cells) {
    out.set(`${c.category}|${c.aspect}`, c);
  }
  return out;
}

export function stageLabel(stages: BenchmarkStage[], name: string): string {
  return stages.find((s) => s.name === name)?.label ?? name;
}

/** Pick the initial stage once the catalog is loaded. Backend already
 *  returns stages sorted by YAML preferred_default_stages, so the first
 *  entry IS the preferred one if it has data. */
export function pickDefaultStage(stages: BenchmarkStage[]): string | null {
  return stages.length > 0 ? stages[0].name : null;
}

/** CSV export of the currently filtered rows — one line per cell. */
