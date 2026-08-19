// What the benchmark page shows, decided apart from how it draws it.
//
// This block used to sit inside a 1,133-line component as forty lines of
// intermediate consts. Every one of them encodes a decision about what a
// reader sees, and two of them are fallbacks that exist to stop the page going
// blank. Neither could be tested where it was.

import type { BenchmarkEvalSet, BenchmarkRow, BenchmarkStage } from "@/lib/api";

export type LineageChip = {
  key: string;
  label: string;
  cats: string[];
  tooltip: string;
};

/**
 * Evaluation sets worth offering in the selector.
 *
 * Sets with no curated rows are hidden by default: they are probe and
 * test-only runs, and listing them invites a reader to select one and find an
 * empty page. The currently selected set is always kept, so the dropdown can
 * never lose its own value, and `showAll` is the escape hatch.
 */
export function visibleEvalSets(
  all: BenchmarkEvalSet[],
  withRows: Set<string>,
  selectedId: string,
  showAll: boolean,
): BenchmarkEvalSet[] {
  if (showAll) return all;
  return all.filter((es) => withRows.has(es.id) || es.id === selectedId);
}

export type LineageResolution = {
  categories: string[];
  /** True when the chip matched nothing and the unfiltered list is in use. */
  fellBack: boolean;
};

/**
 * Categories after the lineage chip, with a fallback that keeps the page alive.
 *
 * A chip that matches no category would leave every category-keyed panel
 * empty, which reads as "the benchmark has no data" rather than as "this
 * filter excludes everything here". It happens on a benchmark holding only NK
 * rows when the reader picks "PK only". The fallback is reported rather than
 * silent so the surface can say why it did not do what was asked.
 */
export function resolveLineage(
  allCategories: string[],
  chip: LineageChip | undefined,
): LineageResolution {
  const wanted = new Set(chip?.cats ?? []);
  const categories = allCategories.filter((c) => wanted.has(c));
  const fellBack = categories.length === 0 && allCategories.length > 0;
  return { categories: fellBack ? allCategories : categories, fellBack };
}

/** Rows whose category survived the lineage filter. */
export function rowsInCategories(
  rows: BenchmarkRow[],
  categories: string[],
): BenchmarkRow[] {
  const keep = new Set(categories);
  return rows.filter((r) => keep.has(r.category));
}

/**
 * The evaluation set to describe in the banner, or null when there is no one
 * set to describe.
 *
 * "all" with a single set behind it still names that set, because a reader
 * looking at one set's numbers should see whose they are. "all" with several
 * returns null: naming one of them would be a lie about what is on screen.
 */
export function activeEvalSet(
  list: BenchmarkEvalSet[],
  selectedId: string,
): BenchmarkEvalSet | null {
  if (selectedId !== "all")
    return list.find((e) => e.id === selectedId) ?? null;
  return list.length === 1 ? list[0] : null;
}

/**
 * Which empty state to show.
 *
 * "curated-to-empty" is the case worth separating: the raw matrix has rows and
 * curation hid all of them, so the honest message points at the Show all
 * escape hatch instead of implying the benchmark itself is empty.
 */
export function emptyKind(
  curatedRows: BenchmarkRow[],
  rawRowCount: number,
): "none" | "empty" | "curated-to-empty" {
  if (curatedRows.length > 0) return "none";
  return rawRowCount > 0 ? "curated-to-empty" : "empty";
}

/** Catalogue value when it has one, otherwise whatever the matrix carried. */
export function preferCatalog<T>(catalog: T[], fromMatrix: T[]): T[] {
  return catalog.length > 0 ? catalog : fromMatrix;
}

/** Stage list and the label for the current one, resolved together. */
export function stageContext(
  catalogStages: BenchmarkStage[],
  matrixStages: BenchmarkStage[],
  current: string | null,
): { stages: BenchmarkStage[]; label: string } {
  const stages = preferCatalog(catalogStages, matrixStages);
  const found = stages.find((s) => s.name === current);
  return { stages, label: found?.label ?? current ?? "" };
}
