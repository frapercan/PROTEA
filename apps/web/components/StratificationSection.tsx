"use client";

/**
 * The fine stratification, under the nine panels it refines.
 *
 * WHY IT IS HERE. The nine panels are the coarsest crossing the record
 * carries, and the section above draws them as if they were the partition.
 * The stratifier crossed four axes, not two, and the finer cells say
 * something the nine cannot: that refining the partition destroys it. The
 * triple crossing leaves a handful of cells able to decide anything, and
 * they sit in the one region the arithmetic already calls inert.
 *
 * THE ONE THING IT MUST SHOW WITHOUT BEING READ. Nine small grids, one per
 * panel, each square a cell of length by identity. A filled square is a
 * cell that could decide between two arms. Six of the nine grids come out
 * empty. No sentence is needed to see that.
 *
 * REPORTING IS NOT ROUTING, AND THE TWO AXES ARE NOT ALIKE. Length is a
 * property of the sequence: the same protein falls in the same band under
 * every arm. Identity to the nearest donor is computed from the donors the
 * run retrieved, so it moves when the arm moves, and a decision that needed
 * it would need to know the answer before choosing. The two matrices are
 * drawn with identical geometry and differ in exactly one column, so the
 * difference is where the eye lands. It is not asserted either: the drift
 * figure under each matrix is measured off the same rows.
 *
 * IT NEVER POOLS THE PANELS. There is no cell anywhere that adds two
 * panels together. Populations are summed only along the bands of one axis
 * inside one panel and one arm, which is licensed because those bands
 * partition that arm's placed population. Counting how many CELLS clear a
 * floor is a statement about the partition, not a score built out of nine.
 */

import { useCallback, useEffect, useState } from "react";
import { useLocale, useTranslations } from "next-intl";
import { Ban, RefreshCw, Ruler, Waypoints } from "lucide-react";

import { Skeleton } from "@/components/Skeleton";
import {
  PANEL_CATEGORIES,
  type ContrastFloor,
  type ContrastFloors,
  type GraphPanel,
} from "@/lib/graph";
import {
  ASPECT_CODES,
  allRows,
  armCoverage,
  armsIncomplete,
  bandsOf,
  cellKey,
  compositionDrift,
  crossing,
  fetchCrossings,
  floorRank,
  indexCells,
  panelsPresent,
  routingRegions,
  tripleByPanel,
  tripleVerdict,
  PANEL_OF_ASPECT,
  type Axis,
  type CellPopulation,
  type CompareRow,
  type PanelKey,
  type SettingLoad,
} from "@/lib/strataStructure";

// ── How a cell wears its floor ───────────────────────────────────────────
//
// Same four redundant channels the node rows use: a shape, a fill, a glyph
// and, in the legend, the word. A reader who resolves none of the hues can
// still separate a double border from a hatch from a heavy left bar.

/** amber-100 hatching, drawn rather than tinted so it survives greyscale. */
const HATCH: React.CSSProperties = {
  backgroundImage:
    "repeating-linear-gradient(135deg, #fffbeb 0 3px, #fde68a 3px 6px)",
};

type FloorState = "top" | "mid" | "thin" | "absent";

const FLOOR_BOX: Record<FloorState, string> = {
  top: "rounded-md border-2 border-emerald-600 bg-emerald-50 text-emerald-900",
  mid: "rounded-md border border-amber-500 text-amber-900",
  thin: "rounded-none border border-rose-300 border-l-4 border-l-rose-500 bg-rose-50 text-rose-900",
  absent: "rounded-md border border-dashed border-slate-300 text-slate-400",
};

/** The same four states as a single square, for the nine small grids. */
const FLOOR_SQUARE: Record<FloorState, string> = {
  top: "bg-emerald-600 border border-emerald-700",
  mid: "border border-amber-500",
  thin: "border border-rose-300 bg-rose-50",
  absent: "border border-dashed border-slate-200",
};

/**
 * Which floor a cell clears, named for drawing.
 *
 * `top` is the strictest class the record declares, whatever it is called
 * and however many classes there are; `mid` is any weaker one it cleared.
 * With no floors served at all every cell that has a population reads
 * `mid`, because the honest report is that the population is known and the
 * floor is not.
 */
function floorState(
  cell: CellPopulation | undefined,
  floors: ContrastFloor[],
): FloorState {
  if (!cell || cell.low <= 0) return "absent";
  if (floors.length === 0) return "mid";
  const rank = floorRank(cell.low, floors);
  if (rank === floors.length - 1) return "top";
  if (rank >= 0) return "mid";
  return "thin";
}

// ── Small shared pieces ──────────────────────────────────────────────────

function usePopText() {
  const locale = useLocale();
  return useCallback(
    (cell: CellPopulation | undefined): string => {
      if (!cell || cell.low <= 0) return "∅";
      const low = cell.low.toLocaleString(locale);
      if (cell.low === cell.high) return low;
      return `${low}–${cell.high.toLocaleString(locale)}`;
    },
    [locale],
  );
}

function panelLabel(panel: PanelKey): string {
  return `${panel.category} · ${PANEL_OF_ASPECT[panel.aspect] ?? panel.aspect}`;
}

// ── The floors, said once ────────────────────────────────────────────────

/**
 * What a cell is being marked against.
 *
 * Printed before any marked cell, because "too thin" is meaningless until
 * a reader knows too thin for what, and the two classes here differ by a
 * factor of two and a half.
 */
function FloorLegend({ floors }: { floors: ContrastFloors | null }) {
  const t = useTranslations("graph.strat");
  const locale = useLocale();
  if (!floors || floors.classes.length === 0) {
    return (
      <p
        className="rounded-lg border border-amber-300 bg-amber-50 px-4 py-2.5 text-xs leading-relaxed text-amber-900"
        role="status"
      >
        {t("floorsAbsent")}
      </p>
    );
  }
  const label: Record<string, string> = {
    reporting: t("classReporting"),
    routing: t("classRouting"),
  };
  const top = floors.classes.length - 1;
  return (
    <div className="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h3 className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">
          {t("floorsHeading")}
        </h3>
        <p className="text-xs text-slate-600">
          {t("floorsTarget", { effect: floors.target_effect.toFixed(4) })}
        </p>
      </div>
      <dl className="mt-2 grid grid-cols-1 gap-x-6 gap-y-2 sm:grid-cols-2">
        {floors.classes.map((f, i) => (
          <div key={f.key} className="flex items-start gap-2">
            <dt className="shrink-0 pt-0.5">
              <span
                className={`inline-flex items-baseline gap-1.5 px-2 py-0.5 text-xs font-medium ${
                  FLOOR_BOX[i === top ? "top" : "mid"]
                }`}
                style={i === top ? undefined : HATCH}
              >
                <span className="font-mono font-semibold">
                  {f.population.toLocaleString(locale)}
                </span>
                {label[f.key] ?? f.key}
              </span>
            </dt>
            <dd className="text-xs leading-relaxed text-slate-600">{f.contrast}</dd>
          </div>
        ))}
      </dl>
      <p className="mt-2 border-t border-slate-200 pt-2 text-xs leading-relaxed text-slate-600">
        {t("floorsRead")}
      </p>
    </div>
  );
}

// ── How much of the comparison is actually there ─────────────────────────

/**
 * Arms stratified against arms held, per setting.
 *
 * The endpoint reports the pair for a reason. A crossing read across half
 * the arms and quoted as if it covered them all is the exact error this
 * page exists not to make, so the pair is on screen before the first cell.
 */
//: The three knowledge categories a stratification is read over.
const SETTING_COUNT = 3;

function ArmCoverageStrip({ loads }: { loads: SettingLoad[] }) {
  const t = useTranslations("graph.strat");
  const locale = useLocale();
  const coverage = armCoverage(loads);
  const short = armsIncomplete(coverage);
  return (
    <div data-testid="strata-arms">
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-[11px] uppercase tracking-wider text-slate-500">
          {t("armsHeading")}
        </span>
        {coverage.map((c) => {
          const incomplete = c.state === "ok" && c.withStrata < c.total;
          const failed = c.state !== "ok";
          return (
            <span
              key={c.setting}
              className={`inline-flex items-baseline gap-1.5 px-2 py-0.5 text-xs ${
                failed
                  ? "rounded-md border border-dashed border-slate-300 text-slate-500"
                  : incomplete
                    ? "rounded-none border border-amber-400 border-l-4 border-l-amber-600 bg-amber-50 text-amber-900"
                    : "rounded-md border border-slate-200 bg-white text-slate-700"
              }`}
              title={c.state === "absent" ? t("settingAbsent") : undefined}
            >
              <span className="font-mono font-semibold">{c.setting}</span>
              {c.state === "ok" ? (
                <span className="font-mono">
                  {c.withStrata.toLocaleString(locale)}
                  <span className="text-slate-400">
                    {" / "}
                    {c.total.toLocaleString(locale)}
                  </span>
                </span>
              ) : (
                <span>{c.state === "absent" ? t("stateAbsent") : t("stateError")}</span>
              )}
            </span>
          );
        })}
      </div>
      {short && (
        <p
          className="mt-1.5 rounded-md border border-amber-200 bg-amber-50/70 px-3 py-1.5 text-xs leading-relaxed text-amber-900"
          role="status"
        >
          {t("armsShort")}
        </p>
      )}
    </div>
  );
}

// ── One axis against the nine panels ─────────────────────────────────────

/**
 * A panel by band matrix, with the routing verdict as its last column.
 *
 * The two axes get the SAME geometry on purpose. They differ in one
 * column and in whether their cells carry one population or two, and both
 * of those differences are the finding rather than a styling choice: a
 * cell that reads `627-740` is a cell whose size depends on which arm is
 * asked, which is precisely what disqualifies the axis from routing.
 */
function AxisMatrix({
  axis,
  rows,
  panels,
  units,
  floors,
  routable,
}: {
  axis: Axis;
  rows: CompareRow[];
  panels: PanelKey[];
  units: Map<string, number | null>;
  floors: ContrastFloor[];
  routable: boolean;
}) {
  const t = useTranslations("graph.strat");
  const locale = useLocale();
  const popText = usePopText();

  const bands = bandsOf(axis);
  const cells = crossing(rows, ["category", "aspect", axis]);
  const index = indexCells(cells);
  const placed = indexCells(crossing(rows, ["category", "aspect"]));
  const seen = new Set(cells.map((c) => c.coords[2]));
  // Bands the endpoint sent that this build does not order are appended
  // rather than dropped, for the same reason an unknown panel is.
  const columns = [
    ...bands.filter((b) => seen.has(b)),
    ...[...seen].filter((b) => !bands.includes(b)).sort(),
  ];
  const drift = compositionDrift(rows, axis);

  return (
    <div className="rounded-lg border border-slate-200 bg-white" data-testid={`strata-axis-${axis}`}>
      <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1 border-b border-slate-100 px-4 py-2.5">
        <div className="flex flex-wrap items-baseline gap-2">
          <h3 className="text-sm font-semibold text-slate-900">{t(`axis.${axis}`)}</h3>
          {routable ? (
            <span className="inline-flex items-center gap-1 rounded-md border-2 border-emerald-600 bg-emerald-50 px-2 py-0.5 text-[11px] font-medium text-emerald-900">
              <Ruler className="h-3 w-3 shrink-0" aria-hidden />
              {t("canRoute")}
            </span>
          ) : (
            <span
              className="inline-flex items-center gap-1 rounded-none border border-rose-400 border-l-4 border-l-rose-600 bg-rose-50 px-2 py-0.5 text-[11px] font-medium text-rose-900"
              title={t("cannotRouteWhy")}
            >
              <Ban className="h-3 w-3 shrink-0" aria-hidden />
              {t("cannotRoute")}
            </span>
          )}
        </div>
        <p className="max-w-xl text-xs leading-relaxed text-slate-600">
          {t(`axisWhat.${axis}`)}
        </p>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full min-w-[44rem] text-sm">
          <thead className="bg-slate-50 text-xs uppercase tracking-wider text-slate-500">
            <tr>
              <th scope="col" className="px-3 py-2 text-left font-normal">
                {t("colPanel")}
              </th>
              {columns.map((band) => (
                <th key={band} scope="col" className="px-2 py-2 text-right font-normal">
                  <span className="font-mono normal-case tracking-normal">{band}</span>
                </th>
              ))}
              <th
                scope="col"
                className="border-l border-slate-200 px-3 py-2 text-right font-normal"
              >
                {t("colRegions")}
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {panels.map((panel) => {
              const pk = cellKey(panel.category, panel.aspect);
              const mine = cells.filter(
                (c) => c.coords[0] === panel.category && c.coords[1] === panel.aspect,
              );
              const regions = routingRegions(mine, floors);
              const unitCount = units.get(pk) ?? null;
              const placedHere = placed.get(pk);
              return (
                <tr key={pk} className="align-top">
                  <th scope="row" className="px-3 py-2 text-left font-normal">
                    <span className="font-mono text-xs font-semibold text-slate-900">
                      {panelLabel(panel)}
                    </span>
                    <span className="mt-0.5 block font-mono text-[10px] text-slate-400">
                      {t("placedOf", {
                        placed: popText(placedHere),
                        units: unitCount == null ? "∅" : unitCount.toLocaleString(locale),
                      })}
                    </span>
                  </th>
                  {columns.map((band) => {
                    const cell = index.get(cellKey(panel.category, panel.aspect, band));
                    const state = floorState(cell, floors);
                    return (
                      <td key={band} className="px-1.5 py-1.5 text-right">
                        <span
                          className={`inline-block min-w-[4.5rem] px-1.5 py-0.5 text-right font-mono text-xs ${FLOOR_BOX[state]} ${
                            state === "top" ? "font-semibold" : ""
                          }`}
                          style={state === "mid" ? HATCH : undefined}
                          title={t(`floorState.${state}`)}
                        >
                          {popText(cell)}
                        </span>
                      </td>
                    );
                  })}
                  <td className="border-l border-slate-200 px-3 py-2 text-right">
                    {floors.length === 0 ? (
                      // No floor served, so no count. Printing 0 or
                      // "inherits panel" here would be a verdict this build
                      // has no basis for, and a zero is the one reading that
                      // looks like a measurement.
                      <span
                        className="font-mono text-xs text-slate-400"
                        title={t("gridNoFloors")}
                      >
                        {t("regionsUnknown")}
                      </span>
                    ) : routable ? (
                      <span className="font-mono text-xs text-slate-900">
                        {regions >= 2 ? (
                          regions.toLocaleString(locale)
                        ) : (
                          <span className="text-slate-400">{t("inheritsPanel")}</span>
                        )}
                      </span>
                    ) : (
                      <span
                        className="inline-flex items-center justify-end gap-1 font-mono text-xs text-rose-800"
                        title={t("cannotRouteWhy")}
                      >
                        <Ban className="h-3 w-3 shrink-0" aria-hidden />
                        <span className="line-through decoration-rose-500 decoration-2">
                          {regions.toLocaleString(locale)}
                        </span>
                      </span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <p className="border-t border-slate-100 px-4 py-2 text-xs leading-relaxed text-slate-600">
        {drift == null
          ? t("driftSingleArm")
          : t("drift", { pct: (drift * 100).toFixed(2) })}
      </p>
    </div>
  );
}

/**
 * What a square means, once, above the nine grids that use them.
 *
 * A grid this small carries no labels of its own, so the reader needs the
 * four states and the two axes before the first grid rather than in a
 * tooltip they would have to go looking for.
 */
function SquareLegend({
  floors,
  rows,
  cols,
}: {
  floors: ContrastFloor[];
  rows: string[];
  cols: string[];
}) {
  const t = useTranslations("graph.strat");
  const states: FloorState[] = ["top", "mid", "thin", "absent"];
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
      <span className="font-mono text-[10px] text-slate-500">
        {t("gridAxes")}
      </span>
      {states.map((state) => (
        <span key={state} className="flex items-center gap-1.5">
          <span
            className={`h-3 w-5 shrink-0 ${FLOOR_SQUARE[state]}`}
            style={state === "mid" ? HATCH : undefined}
            aria-hidden
          />
          <span className="text-[11px] text-slate-600">{t(`floorState.${state}`)}</span>
        </span>
      ))}
      <span className="ml-auto font-mono text-[10px] text-slate-400">
        {t("gridShape", { rows: rows.length, cols: cols.length })}
      </span>
      {floors.length === 0 && (
        <span className="text-[11px] text-amber-800">{t("gridNoFloors")}</span>
      )}
    </div>
  );
}

// ── The triple crossing, as nine small grids ─────────────────────────────

/**
 * Every cell of length by identity, one grid per panel.
 *
 * The full crossing runs to more cells than anybody reads, and reading is
 * not what it is for. Drawn this small the only question a grid answers is
 * how many of its squares are filled, and that is the question.
 */
function TripleGrid({
  rows,
  panels,
  floors,
  answered,
}: {
  rows: CompareRow[];
  panels: PanelKey[];
  floors: ContrastFloor[];
  /** How many of the three knowledge categories answered. */
  answered: number;
}) {
  const t = useTranslations("graph.strat");
  const locale = useLocale();
  const popText = usePopText();

  const byPanel = tripleByPanel(rows, panels, floors);
  const verdict = tripleVerdict(byPanel);
  // How many of the three knowledge categories actually answered. The verdict
  // below is the one sentence this section exists to be read without reading,
  // and computed over two categories it still reads as complete: the
  // denominator drops from nine panels to six and nothing says so. A conclusion
  // that changes its own denominator in silence is worse than one that fails,
  // because a failure is noticed.
  const truncated = answered > 0 && answered < SETTING_COUNT;

  // ONE geometry for all nine, taken from the whole crossing rather than
  // from each panel. A panel drawn with fewer columns because a band never
  // occurred in it reads as a smaller grid, and the reader compares grid
  // sizes instead of fill. Every grid gets every band the record used, and
  // a band this panel never saw is a dashed square like any other absence.
  const seenLength = new Set<string>();
  const seenHomology = new Set<string>();
  for (const panel of byPanel) {
    for (const key of panel.cells.keys()) {
      const [l, h] = key.split(" ");
      seenLength.add(l);
      seenHomology.add(h);
    }
  }
  const known = { length: bandsOf("length"), homology: bandsOf("homology") };
  const inOrder = (seen: Set<string>, order: string[]) => [
    ...order.filter((b) => seen.has(b)),
    ...[...seen].filter((b) => !order.includes(b)).sort(),
  ];
  const cols = inOrder(seenHomology, known.homology);
  const rowBands = inOrder(seenLength, known.length);

  return (
    <div className="space-y-3" data-testid="strata-triple">
      <SquareLegend floors={floors} rows={rowBands} cols={cols} />
      <div className="overflow-x-auto">
        <div className="grid min-w-[46rem] gap-2 sm:grid-cols-3">
          {byPanel.map((panel) => {
            const unjudged = floors.length === 0;
            const none = !unjudged && panel.clearing === 0;
            return (
              <div
                key={cellKey(panel.category, panel.aspect)}
                className={`rounded-lg border p-3 ${
                  none ? "border-rose-200 bg-rose-50/40" : "border-slate-200 bg-white"
                }`}
              >
                <div className="flex items-baseline justify-between gap-2">
                  <span className="font-mono text-xs font-semibold text-slate-900">
                    {panelLabel(panel)}
                  </span>
                  <span
                    className={`font-mono text-xs ${
                      unjudged
                        ? "text-slate-400"
                        : none
                          ? "font-semibold text-rose-800"
                          : "text-slate-900"
                    }`}
                    title={unjudged ? t("gridNoFloors") : undefined}
                  >
                    {unjudged
                      ? t("regionsUnknown")
                      : panel.clearing.toLocaleString(locale)}
                    <span className="text-slate-400">
                      {" / "}
                      {panel.withPopulation.toLocaleString(locale)}
                    </span>
                  </span>
                </div>
                <div className="mt-2 flex flex-col gap-0.5">
                  {rowBands.map((length) => (
                    <div key={length} className="flex gap-0.5">
                      {cols.map((homology) => {
                        const cell = panel.cells.get(cellKey(length, homology));
                        const state = floorState(cell, floors);
                        return (
                          <span
                            key={homology}
                            className={`h-3.5 w-full ${FLOOR_SQUARE[state]}`}
                            style={state === "mid" ? HATCH : undefined}
                            title={`${length} · ${homology} · ${popText(cell)} · ${t(
                              `floorState.${state}`,
                            )}`}
                          />
                        );
                      })}
                    </div>
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      <p
        className={`rounded-lg border px-4 py-3 text-sm leading-relaxed ${
          floors.length === 0 || truncated
            ? "border-amber-300 bg-amber-50 text-amber-900"
            : verdict.clearing === 0 || verdict.panelsWithNone > 0
              ? "border-rose-300 bg-rose-50 text-rose-900"
              : "border-slate-200 bg-white text-slate-700"
        }`}
        role="status"
        data-testid="strata-verdict"
      >
        {floors.length === 0 ? (
          t("tripleUnjudgeable", { cells: verdict.cells.toLocaleString(locale) })
        ) : (
          <>
        {t("tripleVerdict", {
          clearing: verdict.clearing.toLocaleString(locale),
          cells: verdict.cells.toLocaleString(locale),
          none: verdict.panelsWithNone.toLocaleString(locale),
          panels: verdict.panels.toLocaleString(locale),
        })}
        {verdict.categoriesClearing.length > 0 && (
          <>
            {" "}
            {t("tripleWhere", { categories: verdict.categoriesClearing.join(", ") })}
          </>
        )}
        {truncated && (
          <>
            {" "}
            <strong>
              {t("tripleTruncated", {
                answered: answered.toLocaleString(locale),
                total: SETTING_COUNT.toLocaleString(locale),
              })}
            </strong>
          </>
        )}
          </>
        )}
      </p>
    </div>
  );
}

// ── The full crossing, for the reader who wants the rows ─────────────────

function FullCrossing({
  rows,
  panels,
  floors,
}: {
  rows: CompareRow[];
  panels: PanelKey[];
  floors: ContrastFloor[];
}) {
  const t = useTranslations("graph.strat");
  const locale = useLocale();
  const popText = usePopText();
  const cells = crossing(rows, ["category", "aspect", "length", "homology"]);
  const order = new Map(
    panels.map((p, i) => [cellKey(p.category, p.aspect), i] as const),
  );
  const lengths = bandsOf("length");
  const homologies = bandsOf("homology");
  const rank = (list: string[], v: string) => {
    const i = list.indexOf(v);
    return i === -1 ? list.length : i;
  };
  const sorted = [...cells].sort((a, b) => {
    const pa = order.get(cellKey(a.coords[0], a.coords[1])) ?? 99;
    const pb = order.get(cellKey(b.coords[0], b.coords[1])) ?? 99;
    if (pa !== pb) return pa - pb;
    const la = rank(lengths, a.coords[2]);
    const lb = rank(lengths, b.coords[2]);
    if (la !== lb) return la - lb;
    return rank(homologies, a.coords[3]) - rank(homologies, b.coords[3]);
  });

  return (
    <details className="rounded-lg border border-slate-200 bg-white">
      <summary className="cursor-pointer px-4 py-2.5 text-xs text-slate-700">
        {t("fullCrossing", { cells: sorted.length.toLocaleString(locale) })}
      </summary>
      <div className="max-h-96 overflow-auto border-t border-slate-100">
        <table className="w-full min-w-[42rem] text-sm">
          <thead className="sticky top-0 bg-slate-50 text-xs uppercase tracking-wider text-slate-500">
            <tr>
              <th scope="col" className="px-3 py-2 text-left font-normal">
                {t("colPanel")}
              </th>
              <th scope="col" className="px-3 py-2 text-left font-normal">
                {t("axis.length")}
              </th>
              <th scope="col" className="px-3 py-2 text-left font-normal">
                {t("axis.homology")}
              </th>
              <th scope="col" className="px-3 py-2 text-right font-normal">
                {t("colPopulation")}
              </th>
              <th scope="col" className="px-3 py-2 text-right font-normal">
                {t("colArms")}
              </th>
              <th scope="col" className="px-3 py-2 text-left font-normal">
                {t("colFloor")}
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {sorted.map((cell) => {
              const state = floorState(cell, floors);
              return (
                <tr key={cell.coords.join(" ")} className="hover:bg-slate-50/60">
                  <td className="px-3 py-1.5 font-mono text-xs text-slate-900">
                    {panelLabel({ category: cell.coords[0], aspect: cell.coords[1] })}
                  </td>
                  <td className="px-3 py-1.5 font-mono text-xs text-slate-700">
                    {cell.coords[2]}
                  </td>
                  <td className="px-3 py-1.5 font-mono text-xs text-slate-700">
                    {cell.coords[3]}
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono text-xs text-slate-900">
                    {popText(cell)}
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono text-xs text-slate-500">
                    {cell.arms.toLocaleString(locale)}
                  </td>
                  <td className="px-3 py-1.5">
                    <span
                      className={`inline-block px-1.5 py-0.5 text-[11px] ${FLOOR_BOX[state]}`}
                      style={state === "mid" ? HATCH : undefined}
                    >
                      {t(`floorState.${state}`)}
                    </span>
                  </td>
                </tr>
              );
            })}
            {sorted.length === 0 && (
              <tr>
                <td colSpan={6} className="px-4 py-6 text-center text-sm text-slate-600">
                  {t("noCells")}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </details>
  );
}

// ── The section ──────────────────────────────────────────────────────────

export function StratificationSection({
  evaluationSetId,
  floors,
  panels,
}: {
  /** The frame's set. Null when the frame names none, which is a state. */
  evaluationSetId: string | null;
  floors?: ContrastFloors | null;
  /** The nine panels, for their populations only. Never scored here. */
  panels: GraphPanel[];
}) {
  const t = useTranslations("graph.strat");
  // The answer carries the question it answers. Loading is DERIVED from the
  // pair rather than stored: an effect that set a flag on its way in would
  // cascade a render on every mount, and an answer that did not name its
  // evaluation set could be drawn under a different one after a change of
  // frame. The effect touches state only when its promise settles.
  const [answer, setAnswer] = useState<{
    forId: string;
    loads: SettingLoad[];
  } | null>(null);
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    if (!evaluationSetId) return;
    const controller = new AbortController();
    let live = true;
    fetchCrossings(evaluationSetId, controller.signal)
      .then((loads) => {
        if (live) setAnswer({ forId: evaluationSetId, loads });
      })
      .catch(() => {
        // fetchCrossings resolves every setting, so this only fires on an
        // abort. Nothing to report: the effect that aborted is gone.
      });
    return () => {
      live = false;
      controller.abort();
    };
  }, [evaluationSetId, nonce]);

  /** Refetch from an event, where clearing the old answer is allowed. */
  const reload = useCallback(() => {
    setAnswer(null);
    setNonce((v) => v + 1);
  }, []);

  const loads = answer && answer.forId === evaluationSetId ? answer.loads : null;

  const heading = (
    <div className="flex flex-wrap items-baseline justify-between gap-2">
      <h2 className="flex items-baseline gap-2 text-lg font-semibold tracking-tight text-slate-900">
        <Waypoints className="h-4 w-4 shrink-0 self-center text-slate-400" aria-hidden />
        {t("heading")}
      </h2>
      <p className="max-w-xl text-xs leading-relaxed text-slate-500">{t("hint")}</p>
    </div>
  );

  if (!evaluationSetId) {
    return (
      <section className="space-y-3" data-testid="graph-strata">
        {heading}
        <p className="rounded-lg border border-slate-200 bg-white px-4 py-6 text-center text-sm text-slate-600">
          {t("noEvaluationSet")}
        </p>
      </section>
    );
  }

  if (loads === null) {
    return (
      <section className="space-y-3" data-testid="graph-strata" aria-busy="true">
        {heading}
        <Skeleton className="h-8 w-1/3" />
        <Skeleton className="h-48 w-full" />
      </section>
    );
  }

  const rows = allRows(loads);
  const classes = floors?.classes ?? [];
  const present = panelsPresent(rows, PANEL_CATEGORIES, ASPECT_CODES);
  const units = new Map<string, number | null>(
    present.map((p) => [
      cellKey(p.category, p.aspect),
      panels.find(
        (g) =>
          g.category === p.category &&
          g.aspect === (PANEL_OF_ASPECT[p.aspect] ?? p.aspect),
      )?.units ?? null,
    ]),
  );
  const failed = loads.filter((l) => l.state === "error");
  // How many categories actually answered. The verdict downstream is read as a
  // conclusion about all nine panels, and over two categories it silently
  // becomes one about six.
  const answeredCount = loads.filter((l) => l.state === "ok").length;

  return (
    <section className="space-y-3" data-testid="graph-strata">
      {heading}

      <ArmCoverageStrip loads={loads} />

      {failed.length > 0 && (
        <p
          className="rounded-lg border border-rose-200 bg-rose-50 px-4 py-2 text-xs leading-relaxed text-rose-900"
          role="alert"
        >
          {t("settingsFailed", {
            settings: failed.map((f) => f.setting).join(", "),
            reason: failed.map((f) => f.message ?? "?").join("; "),
          })}
          <button
            type="button"
            onClick={reload}
            className="ml-2 inline-flex items-center gap-1 rounded border border-rose-300 bg-white px-2 py-0.5 font-medium text-rose-800 hover:bg-rose-100"
          >
            <RefreshCw className="h-3 w-3" aria-hidden />
            {t("retry")}
          </button>
        </p>
      )}

      {rows.length === 0 ? (
        // Three ways to hold no cell, and they are not the same fact. Nobody
        // stratified these arms, or the endpoint failed and nothing is known
        // either way, or every setting answered and answered with nothing.
        // One message for all three would assert the first whenever the
        // second happened, which is the one reading that sends somebody to
        // run an operation that has already run.
        <p className="rounded-lg border border-slate-200 bg-white px-4 py-6 text-center text-sm text-slate-600">
          {failed.length > 0
            ? t("nothingReadable")
            : loads.every((l) => l.state === "absent")
              ? t("nothingStratified")
              : t("noCellsReturned")}
        </p>
      ) : (
        <>
          <FloorLegend floors={floors ?? null} />

          {/* The two axes, same geometry, one column apart. */}
          <div className="space-y-3">
            <AxisMatrix
              axis="length"
              rows={rows}
              panels={present}
              units={units}
              floors={classes}
              routable
            />
            <AxisMatrix
              axis="homology"
              rows={rows}
              panels={present}
              units={units}
              floors={classes}
              routable={false}
            />
          </div>

          {/* The finding, drawn rather than argued. */}
          <div className="space-y-2 pt-1">
            <div className="flex flex-wrap items-baseline justify-between gap-2">
              <h3 className="text-sm font-semibold text-slate-900">{t("tripleHeading")}</h3>
              <p className="max-w-xl text-xs leading-relaxed text-slate-500">
                {t("tripleHint")}
              </p>
            </div>
            <TripleGrid rows={rows} panels={present} floors={classes}
            answered={answeredCount} />
          </div>

          <FullCrossing rows={rows} panels={present} floors={classes} />
        </>
      )}
    </section>
  );
}

export default StratificationSection;
