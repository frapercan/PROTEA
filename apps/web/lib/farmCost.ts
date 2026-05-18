// FARM-UI.5 cost-rollup data transforms.
//
// The /cost endpoint returns one bucket per (date, agent, model) tuple
// inside the requested window. The dashboard needs three pivot views:
//
//   - by agent:   sum usd across the window per agent (bar chart)
//   - by model:   sum usd per (date, model) for a stacked area on the
//                 last 30 days
//   - by day:     sum usd per date for a line over the last 60 days
//
// Keeping the math in a free-standing module means we can unit-test the
// pivots without booting the React tree, and the page component stays a
// presentation layer over precomputed series.

import type { FarmAgentBudget, FarmCostBucket } from "@/lib/farmApi";

export type AgentTotal = {
  agent: string;
  usd: number;
  tasks: number;
  budget: number | null;
  overBudget: boolean;
};

export type DailyTotal = {
  date: string;
  usd: number;
  tasks: number;
};

export type ModelDailyPoint = {
  date: string;
  /** Sum per model for the given date. Missing models default to 0. */
  perModel: Record<string, number>;
  total: number;
};

/** Sort the date-axis ascending; ties are broken lexicographically which
 *  works for ISO `YYYY-MM-DD` keys. */
function sortDates(dates: Iterable<string>): string[] {
  return Array.from(dates).sort();
}

/** Generate the full date axis between minDate and today (inclusive) so
 *  empty days appear as zero on the line / area charts, not as gaps. */
export function dateRange(start: Date, end: Date): string[] {
  const out: string[] = [];
  const s = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), start.getUTCDate()));
  const e = new Date(Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), end.getUTCDate()));
  for (let d = s; d <= e; d.setUTCDate(d.getUTCDate() + 1)) {
    out.push(d.toISOString().slice(0, 10));
  }
  return out;
}

/** Sum usd + tasks per agent, attach budget from agent yaml, flag
 *  "average daily spend over the window > budget" so the bar chart can
 *  paint over-budget agents red. We use the window average rather than
 *  any single day so a one-off spike does not look catastrophic; the
 *  per-day view exposes those spikes separately.
 */
export function totalsByAgent(
  rows: FarmCostBucket[],
  budgets: FarmAgentBudget[],
  windowDays: number,
): AgentTotal[] {
  const usd = new Map<string, number>();
  const tasks = new Map<string, number>();
  for (const r of rows) {
    usd.set(r.agent, (usd.get(r.agent) ?? 0) + r.usd);
    tasks.set(r.agent, (tasks.get(r.agent) ?? 0) + r.tasks);
  }
  const budgetIx = new Map<string, number | null>();
  for (const b of budgets) {
    budgetIx.set(b.name, b.max_usd_per_day ?? null);
  }
  const out: AgentTotal[] = [];
  // Union of agents that spent and agents with a configured budget so
  // the bar chart still shows zero-spend agents that have a cap.
  const names = new Set<string>([...usd.keys(), ...budgetIx.keys()]);
  for (const name of names) {
    const spend = usd.get(name) ?? 0;
    const cap = budgetIx.get(name) ?? null;
    const dailyAvg = windowDays > 0 ? spend / windowDays : spend;
    const overBudget = cap != null && cap > 0 && dailyAvg > cap;
    out.push({
      agent: name,
      usd: spend,
      tasks: tasks.get(name) ?? 0,
      budget: cap,
      overBudget,
    });
  }
  // Sort by usd desc so the biggest spenders read first.
  out.sort((a, b) => b.usd - a.usd || a.agent.localeCompare(b.agent));
  return out;
}

/** Per-day totals (line chart axis). Returns a series with one entry
 *  per day in [today - days + 1, today], filling zero where no bucket
 *  exists.
 */
export function totalsByDay(
  rows: FarmCostBucket[],
  days: number,
  today: Date = new Date(),
): DailyTotal[] {
  const usd = new Map<string, number>();
  const tasks = new Map<string, number>();
  for (const r of rows) {
    usd.set(r.date, (usd.get(r.date) ?? 0) + r.usd);
    tasks.set(r.date, (tasks.get(r.date) ?? 0) + r.tasks);
  }
  const start = new Date(today);
  start.setUTCDate(start.getUTCDate() - (days - 1));
  return dateRange(start, today).map((date) => ({
    date,
    usd: usd.get(date) ?? 0,
    tasks: tasks.get(date) ?? 0,
  }));
}

/** Per-day, per-model series for the stacked area. Returns the day
 *  axis (last `days` days) and the ordered model list (descending by
 *  total usd, ties broken by name) so the rendered legend matches the
 *  visual stack order.
 */
export function totalsByModelDay(
  rows: FarmCostBucket[],
  days: number,
  today: Date = new Date(),
): { axis: ModelDailyPoint[]; models: string[] } {
  const start = new Date(today);
  start.setUTCDate(start.getUTCDate() - (days - 1));
  const dates = dateRange(start, today);
  const dateSet = new Set(dates);

  const perModelTotal = new Map<string, number>();
  // date -> model -> usd
  const cells = new Map<string, Map<string, number>>();
  for (const r of rows) {
    if (!dateSet.has(r.date)) continue;
    perModelTotal.set(r.model, (perModelTotal.get(r.model) ?? 0) + r.usd);
    const row = cells.get(r.date) ?? new Map<string, number>();
    row.set(r.model, (row.get(r.model) ?? 0) + r.usd);
    cells.set(r.date, row);
  }
  const models = Array.from(perModelTotal.keys()).sort((a, b) => {
    const diff = (perModelTotal.get(b) ?? 0) - (perModelTotal.get(a) ?? 0);
    if (diff !== 0) return diff;
    return a.localeCompare(b);
  });

  const axis: ModelDailyPoint[] = sortDates(dates).map((date) => {
    const row = cells.get(date) ?? new Map<string, number>();
    const perModel: Record<string, number> = {};
    let total = 0;
    for (const m of models) {
      const v = row.get(m) ?? 0;
      perModel[m] = v;
      total += v;
    }
    return { date, perModel, total };
  });

  return { axis, models };
}

/** Compact USD formatter for chart labels. `< 1` shows 4dp so cheap
 *  agents don't all read as $0.00; otherwise 2dp. */
export function formatUsd(v: number): string {
  if (!Number.isFinite(v)) return "-";
  if (v === 0) return "$0";
  const abs = Math.abs(v);
  if (abs >= 1000) return `$${(v / 1000).toFixed(1)}k`;
  if (abs >= 1) return `$${v.toFixed(2)}`;
  return `$${v.toFixed(4)}`;
}

/** Stable palette for the model area chart. The first N models pick
 *  from these colors; anything beyond falls back to a slate gray. The
 *  ordering matches the model ranking returned by totalsByModelDay so
 *  the legend, stack, and tooltip all share the same hue.
 */
export const MODEL_PALETTE = [
  "#6366f1", // indigo-500
  "#8b5cf6", // violet-500
  "#06b6d4", // cyan-500
  "#10b981", // emerald-500
  "#f59e0b", // amber-500
  "#ef4444", // red-500
  "#0ea5e9", // sky-500
  "#a855f7", // purple-500
] as const;

export function modelColor(index: number): string {
  if (index < 0) return "#64748b"; // slate-500
  return MODEL_PALETTE[index % MODEL_PALETTE.length];
}
