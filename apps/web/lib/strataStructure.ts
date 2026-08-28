// The four axes the record actually measured, read off /v1/strata/compare.
//
// The nine panels are a crossing of two axes. The stratifier crossed four,
// and the two it added are not the same kind of thing:
//
//   LENGTH is a property of the sequence. A protein falls in the same band
//   whichever arm scored it, so a decision taken per length band is a
//   decision about the protein.
//
//   HOMOLOGY is computed from the donors the run retrieved. Change the arm
//   and the donors change, so the band is a property of the ARM and not of
//   the protein. A decision taken per homology band would need the band
//   before it knows which arm to send the protein to, which is circular.
//
// That distinction is not asserted here, it is measured: `compositionDrift`
// asks how far a panel's composition on an axis moves when the arm moves,
// and the two axes answer it differently by an order of magnitude.
//
// Everything is pure except `fetchCrossings`, so the shaping can be tested
// without a DOM and the component stays presentation-only.

import { baseUrl } from "@/lib/api";
import type { ContrastFloor } from "@/lib/graph";

/** The four axes, in the order the stratifier crossed them. */
export type Axis = "category" | "aspect" | "length" | "homology";

/** One cell of one arm, exactly as the endpoint sends it. */
export type CompareRow = {
  evaluation_result_id: string;
  model: string;
  display_name: string;
  k: number;
  category?: string;
  aspect?: string;
  length?: string;
  homology?: string;
  n_proteins: number;
  precision_w?: number;
  recall_w?: number;
  f_micro_w?: number;
  reportable?: boolean;
};

export type ComparePayload = {
  evaluation_set_id: string;
  setting: string;
  where: Record<string, string>;
  arms_total: number;
  arms_with_strata: number;
  rows: CompareRow[];
};

/**
 * What came back for one knowledge setting.
 *
 * `absent` is kept apart from `error` because they call for different acts:
 * nobody stratified these arms and the operation has to be run, versus the
 * endpoint failed and somebody has to look at the API.
 */
export type SettingLoad = {
  setting: string;
  state: "ok" | "absent" | "error";
  payload: ComparePayload | null;
  message: string | null;
};

/**
 * The three settings, requested one at a time because they are three
 * populations. The endpoint serves one per call by design: a table that
 * mixed them would not be comparable down a column.
 */
export const SETTINGS = ["NK", "LK", "PK"] as const;

/** Aspect codes as the strata artefact spells them, and as the panels do. */
export const PANEL_OF_ASPECT: Record<string, string> = {
  P: "BPO",
  F: "MFO",
  C: "CCO",
};

/** The three aspects in the order the nine-panel grid reads them. */
export const ASPECT_CODES = ["P", "F", "C"];

/** Length bands by residue count, which is not their alphabetical order. */
export const LENGTH_BANDS = ["<=512", "512-1024", "1024-2048", ">2048"];

/** Identity bands ascending, "none" first because it is the floor. */
export const HOMOLOGY_BANDS = ["none", "<=30", "30-60", "60-90", ">90"];

export function bandsOf(axis: Axis): string[] {
  if (axis === "length") return LENGTH_BANDS;
  if (axis === "homology") return HOMOLOGY_BANDS;
  return [];
}

/**
 * Fetch the three settings at once, keeping each one's outcome separate.
 *
 * Settled independently rather than as one promise: a setting nobody
 * stratified must not take the other two off the page, and which one failed
 * is itself a reportable fact.
 */
export async function fetchCrossings(
  evaluationSetId: string,
  signal?: AbortSignal,
): Promise<SettingLoad[]> {
  const calls = SETTINGS.map(async (setting): Promise<SettingLoad> => {
    const url = `${baseUrl()}/strata/compare/${evaluationSetId}?setting=${setting}`;
    let res: Response;
    try {
      res = await fetch(url, { cache: "no-store", signal });
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      return { setting, state: "error", payload: null, message };
    }
    if (res.status === 404) {
      return { setting, state: "absent", payload: null, message: null };
    }
    if (!res.ok) {
      return { setting, state: "error", payload: null, message: `HTTP ${res.status}` };
    }
    try {
      const payload = (await res.json()) as ComparePayload;
      return { setting, state: "ok", payload, message: null };
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      return { setting, state: "error", payload: null, message };
    }
  });
  return Promise.all(calls);
}

/** Every row that arrived, whatever setting carried it. */
export function allRows(loads: SettingLoad[]): CompareRow[] {
  return loads.flatMap((l) => l.payload?.rows ?? []);
}

/** Arms this comparison covers, against the arms the set holds. */
export type ArmCoverage = {
  setting: string;
  total: number;
  withStrata: number;
  state: SettingLoad["state"];
};

export function armCoverage(loads: SettingLoad[]): ArmCoverage[] {
  return loads.map((l) => ({
    setting: l.setting,
    total: l.payload?.arms_total ?? 0,
    withStrata: l.payload?.arms_with_strata ?? 0,
    state: l.state,
  }));
}

/** True when any setting reports fewer stratified arms than it holds. */
export function armsIncomplete(coverage: ArmCoverage[]): boolean {
  return coverage.some((c) => c.state === "ok" && c.withStrata < c.total);
}

/**
 * One cell of a crossing, and how far the arms disagree about its size.
 *
 * `low` and `high` are never averaged into one number. Where they differ,
 * the population is a property of the arm rather than of the cell, and a
 * single figure would hide exactly the fact this section is about.
 */
export type CellPopulation = {
  /** Values on the axes asked for, in that order. */
  coords: string[];
  low: number;
  high: number;
  /** Arms that carry the cell at all. */
  arms: number;
};

function coordsOf(row: CompareRow, axes: Axis[]): string[] | null {
  const out: string[] = [];
  for (const axis of axes) {
    const v = row[axis];
    if (v == null || v === "") return null;
    out.push(String(v));
  }
  return out;
}

/**
 * Populations of a crossing, summed WITHIN one arm and never across arms.
 *
 * Summing inside an arm is licensed because the bands of an axis partition
 * that arm's placed population: a protein sits in exactly one length band
 * and exactly one identity band. Nothing here ever adds two panels
 * together, and there is no function in this module that could.
 */
export function crossing(rows: CompareRow[], axes: Axis[]): CellPopulation[] {
  const byCell = new Map<string, { coords: string[]; arms: Map<string, number> }>();
  for (const row of rows) {
    const coords = coordsOf(row, axes);
    if (!coords) continue;
    const key = coords.join(" ");
    const cell = byCell.get(key) ?? { coords, arms: new Map<string, number>() };
    const arm = row.evaluation_result_id;
    cell.arms.set(arm, (cell.arms.get(arm) ?? 0) + row.n_proteins);
    byCell.set(key, cell);
  }
  return [...byCell.values()].map(({ coords, arms }) => {
    const values = [...arms.values()];
    return {
      coords,
      low: values.length ? Math.min(...values) : 0,
      high: values.length ? Math.max(...values) : 0,
      arms: arms.size,
    };
  });
}

/** Cells of a crossing, addressable by their coordinates. */
export function indexCells(cells: CellPopulation[]): Map<string, CellPopulation> {
  return new Map(cells.map((c) => [c.coords.join(" "), c]));
}

export function cellKey(...coords: string[]): string {
  return coords.join(" ");
}

/**
 * The strictest floor this population clears, as an index into `floors`.
 *
 * `-1` means it clears none. Floors arrive ascending, so the index is also
 * the strength of the claim the cell can carry.
 *
 * Read against the LOW population rather than the high one: a cell that
 * clears the floor under one arm and not under another has not cleared it,
 * because the comparison a floor prices is between arms.
 */
export function floorRank(population: number, floors: ContrastFloor[]): number {
  let rank = -1;
  floors.forEach((f, i) => {
    if (population >= f.population) rank = i;
  });
  return rank;
}

/**
 * How far a panel's composition on one axis moves when the arm moves.
 *
 * Measured as a SHARE of each arm's own placed population, not as a count.
 * Arms place different numbers of proteins, so two arms can disagree about
 * every count while agreeing exactly about the shape, and it is the shape
 * that says whether the axis belongs to the protein or to the arm.
 *
 * Null when a single arm carries everything: one arm cannot disagree.
 */
export function compositionDrift(rows: CompareRow[], axis: Axis): number | null {
  // arm -> panel -> band -> population, and arm -> panel -> placed total.
  const per = new Map<string, Map<string, Map<string, number>>>();
  const totals = new Map<string, Map<string, number>>();
  for (const row of rows) {
    const panel = coordsOf(row, ["category", "aspect"]);
    const band = row[axis];
    if (!panel || band == null || band === "") continue;
    const pk = panel.join(" ");
    const arm = row.evaluation_result_id;
    const byPanel = per.get(arm) ?? new Map<string, Map<string, number>>();
    const byBand = byPanel.get(pk) ?? new Map<string, number>();
    byBand.set(String(band), (byBand.get(String(band)) ?? 0) + row.n_proteins);
    byPanel.set(pk, byBand);
    per.set(arm, byPanel);
    const armTotals = totals.get(arm) ?? new Map<string, number>();
    armTotals.set(pk, (armTotals.get(pk) ?? 0) + row.n_proteins);
    totals.set(arm, armTotals);
  }
  if (per.size < 2) return null;

  const shares = new Map<string, number[]>();
  for (const [arm, byPanel] of per) {
    for (const [pk, byBand] of byPanel) {
      const total = totals.get(arm)?.get(pk) ?? 0;
      if (total <= 0) continue;
      for (const [band, n] of byBand) {
        const key = `${pk} ${band}`;
        const list = shares.get(key) ?? [];
        list.push(n / total);
        shares.set(key, list);
      }
    }
  }
  let worst = 0;
  for (const list of shares.values()) {
    if (list.length < 2) continue;
    worst = Math.max(worst, Math.max(...list) - Math.min(...list));
  }
  return worst;
}

/** A panel: a knowledge category crossed with an aspect. */
export type PanelKey = { category: string; aspect: string };

/**
 * The panels a crossing touched, in the record's own order.
 *
 * A panel the endpoint sent that the canonical order does not name is
 * appended rather than dropped: hiding it would make the section lie about
 * what was crossed.
 */
export function panelsPresent(
  rows: CompareRow[],
  categories: string[],
  aspects: string[],
): PanelKey[] {
  const seen = new Set(
    rows
      .map((r) => (r.category && r.aspect ? `${r.category} ${r.aspect}` : ""))
      .filter(Boolean),
  );
  const out: PanelKey[] = [];
  for (const category of categories) {
    for (const aspect of aspects) {
      if (seen.has(`${category} ${aspect}`)) out.push({ category, aspect });
    }
  }
  for (const key of seen) {
    const [category, aspect] = key.split(" ");
    if (!out.some((p) => p.category === category && p.aspect === aspect)) {
      out.push({ category, aspect });
    }
  }
  return out;
}

/** What the triple crossing leaves behind, panel by panel. */
export type PanelTriple = {
  category: string;
  aspect: string;
  /** Cells with any population at all, keyed by length then homology. */
  cells: Map<string, CellPopulation>;
  withPopulation: number;
  /** Cells clearing the strictest floor the record declares. */
  clearing: number;
};

export function tripleByPanel(
  rows: CompareRow[],
  panels: PanelKey[],
  floors: ContrastFloor[],
): PanelTriple[] {
  const strictest = floors.length - 1;
  const cells = crossing(rows, ["category", "aspect", "length", "homology"]);
  return panels.map(({ category, aspect }) => {
    const mine = cells.filter(
      (c) => c.coords[0] === category && c.coords[1] === aspect,
    );
    return {
      category,
      aspect,
      cells: new Map(mine.map((c) => [cellKey(c.coords[2], c.coords[3]), c])),
      withPopulation: mine.length,
      clearing:
        strictest < 0
          ? 0
          : mine.filter((c) => floorRank(c.low, floors) >= strictest).length,
    };
  });
}

/** The accounting the triple crossing comes to, over every panel. */
export type TripleVerdict = {
  cells: number;
  clearing: number;
  panels: number;
  panelsWithNone: number;
  /** Knowledge categories that hold at least one clearing cell. */
  categoriesClearing: string[];
};

export function tripleVerdict(panels: PanelTriple[]): TripleVerdict {
  const categories: string[] = [];
  for (const p of panels) {
    if (p.clearing > 0 && !categories.includes(p.category)) {
      categories.push(p.category);
    }
  }
  return {
    cells: panels.reduce((s, p) => s + p.withPopulation, 0),
    clearing: panels.reduce((s, p) => s + p.clearing, 0),
    panels: panels.length,
    panelsWithNone: panels.filter((p) => p.clearing === 0).length,
    categoriesClearing: categories,
  };
}

/**
 * How many routing regions an axis yields inside one panel.
 *
 * A panel with fewer than two clearing cells does not admit the split at
 * all: one region is the panel itself under another name, and a partition
 * of one is not a decision. Such a panel inherits its parent rather than
 * pretending to refine it.
 */
export function routingRegions(
  cells: CellPopulation[],
  floors: ContrastFloor[],
): number {
  const strictest = floors.length - 1;
  if (strictest < 0) return 0;
  return cells.filter((c) => floorRank(c.low, floors) >= strictest).length;
}
