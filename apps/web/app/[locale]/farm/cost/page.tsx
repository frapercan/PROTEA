"use client";

// FARM-UI.5 cost-rollup dashboard. Pivots /cost (farm-api sidecar) into
// three views over a single fetch:
//
//   - by-agent      bar chart, sum spend across the window, overlaid
//                   with cost_budget.max_usd_per_day pulled from
//                   /agents/budgets (parsed from agents/*.yaml).
//   - by-model      stacked area, last 30d, one band per model ranked
//                   descending by total spend.
//   - by-day        line over the last 60d.
//
// The view switcher and lookback are URL-synced via useUrlParam so the
// panel is bookmarkable. The window is bumped to 60 days so the by-day
// view always has its full axis; the by-model view trims down to 30
// client-side. We do a single GET for the longest window the page
// needs (60d) and slice it down for the smaller charts.
//
// Charts are hand-rolled SVG (no chart lib in the bundle) to match the
// rest of the dashboard (BenchmarkHeatmap, SliceDag use the same style).

import { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslations } from "next-intl";
import {
  listFarmAgentBudgets,
  listFarmCost,
  type FarmAgentBudget,
  type FarmCostBucket,
} from "@/lib/farmApi";
import {
  formatUsd,
  modelColor,
  totalsByAgent,
  totalsByDay,
  totalsByModelDay,
  type AgentTotal,
  type DailyTotal,
  type ModelDailyPoint,
} from "@/lib/farmCost";
import { useToast } from "@/components/Toast";
import { useUrlParam } from "@/lib/useUrlParam";

const VIEWS = ["agent", "model", "day"] as const;
type View = (typeof VIEWS)[number];

const WINDOW_OPTIONS = [
  { key: "7", days: 7 },
  { key: "30", days: 30 },
  { key: "60", days: 60 },
] as const;
type WindowKey = (typeof WINDOW_OPTIONS)[number]["key"];

// By-model view is fixed at 30d, by-day at 60d (per FARM-UI.5 spec),
// and by-agent honors the global window switcher.
const MODEL_VIEW_DAYS = 30;
const DAY_VIEW_DAYS = 60;
// One fetch covers the largest window in use; smaller charts slice it.
const MAX_DAYS = DAY_VIEW_DAYS;

function isView(v: string | null | undefined): v is View {
  return v === "agent" || v === "model" || v === "day";
}

function isWindow(v: string | null | undefined): v is WindowKey {
  return v === "7" || v === "30" || v === "60";
}

// ────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────

function shortDate(iso: string): string {
  // YYYY-MM-DD → MM-DD; keeps axis labels narrow on 390px viewports.
  return iso.length >= 10 ? iso.slice(5) : iso;
}

// ────────────────────────────────────────────────────────────────────
// Chart components (SVG, no third-party lib)
// ────────────────────────────────────────────────────────────────────

const CHART_W = 720;
const CHART_H = 280;
const CHART_PAD = { top: 16, right: 16, bottom: 36, left: 56 };

function chartInner() {
  return {
    width: CHART_W - CHART_PAD.left - CHART_PAD.right,
    height: CHART_H - CHART_PAD.top - CHART_PAD.bottom,
  };
}

function AgentBarChart({
  totals,
  windowDays,
  emptyLabel,
  budgetLabel,
  overBudgetLabel,
  daysLabel,
}: {
  totals: AgentTotal[];
  windowDays: number;
  emptyLabel: string;
  budgetLabel: string;
  overBudgetLabel: string;
  daysLabel: string;
}) {
  if (totals.length === 0) {
    return (
      <div
        className="rounded-lg border bg-white px-4 py-12 text-center text-sm text-slate-600"
        data-testid="cost-chart-agent-empty"
      >
        {emptyLabel}
      </div>
    );
  }
  const inner = chartInner();
  // Bar chart is horizontal: one row per agent, length proportional to
  // the row's window-average daily spend. Budget is rendered as a tick
  // mark at the matching x-position so over-budget agents extend past
  // their cap line.
  const dailyAvgs = totals.map((t) => (windowDays > 0 ? t.usd / windowDays : t.usd));
  const dailyCaps = totals.map((t) => t.budget ?? 0);
  const maxValue = Math.max(0.001, ...dailyAvgs, ...dailyCaps);
  const rowH = 26;
  const rowGap = 6;
  const fullH = totals.length * (rowH + rowGap) + CHART_PAD.top + CHART_PAD.bottom;

  return (
    <div className="rounded-lg border bg-white p-3 shadow-sm overflow-x-auto">
      <svg
        viewBox={`0 0 ${CHART_W} ${fullH}`}
        className="w-full h-auto"
        role="img"
        aria-label={daysLabel}
        data-testid="cost-chart-agent"
      >
        {/* X-axis baseline */}
        <line
          x1={CHART_PAD.left}
          x2={CHART_PAD.left + inner.width}
          y1={fullH - CHART_PAD.bottom}
          y2={fullH - CHART_PAD.bottom}
          stroke="#cbd5e1"
        />
        {/* X-axis ticks: 0, mid, max */}
        {[0, 0.5, 1].map((p) => {
          const x = CHART_PAD.left + p * inner.width;
          return (
            <g key={p}>
              <line
                x1={x}
                x2={x}
                y1={fullH - CHART_PAD.bottom}
                y2={fullH - CHART_PAD.bottom + 4}
                stroke="#94a3b8"
              />
              <text
                x={x}
                y={fullH - CHART_PAD.bottom + 18}
                textAnchor="middle"
                fontSize={11}
                fill="#64748b"
              >
                {formatUsd(p * maxValue)}
              </text>
            </g>
          );
        })}
        {totals.map((t, i) => {
          const y = CHART_PAD.top + i * (rowH + rowGap);
          const w = (dailyAvgs[i] / maxValue) * inner.width;
          const capX = t.budget != null
            ? CHART_PAD.left + (t.budget / maxValue) * inner.width
            : null;
          const fill = t.overBudget ? "#dc2626" : "#6366f1";
          return (
            <g key={t.agent} data-testid={`cost-bar-${t.agent}`}>
              <text
                x={CHART_PAD.left - 8}
                y={y + rowH / 2 + 4}
                textAnchor="end"
                fontSize={12}
                fill="#334155"
              >
                {t.agent}
              </text>
              <rect
                x={CHART_PAD.left}
                y={y}
                width={Math.max(0, w)}
                height={rowH}
                fill={fill}
                opacity={0.85}
                rx={3}
              >
                <title>
                  {`${t.agent}: ${formatUsd(t.usd)} (${t.tasks} task${t.tasks === 1 ? "" : "s"})`}
                  {t.budget != null
                    ? `\n${budgetLabel}: ${formatUsd(t.budget)}/day`
                    : ""}
                </title>
              </rect>
              <text
                x={CHART_PAD.left + Math.max(0, w) + 6}
                y={y + rowH / 2 + 4}
                fontSize={11}
                fill="#475569"
              >
                {formatUsd(dailyAvgs[i])}/day
                {t.overBudget ? ` (${overBudgetLabel})` : ""}
              </text>
              {capX !== null && t.budget != null && t.budget > 0 && (
                <line
                  x1={capX}
                  x2={capX}
                  y1={y - 2}
                  y2={y + rowH + 2}
                  stroke="#0f172a"
                  strokeDasharray="3 3"
                  data-testid={`cost-budget-line-${t.agent}`}
                >
                  <title>{`${budgetLabel}: ${formatUsd(t.budget)}/day`}</title>
                </line>
              )}
            </g>
          );
        })}
      </svg>
    </div>
  );
}

function ModelStackedAreaChart({
  axis,
  models,
  emptyLabel,
}: {
  axis: ModelDailyPoint[];
  models: string[];
  emptyLabel: string;
}) {
  const hasData = axis.some((p) => p.total > 0);
  if (!hasData || models.length === 0) {
    return (
      <div
        className="rounded-lg border bg-white px-4 py-12 text-center text-sm text-slate-600"
        data-testid="cost-chart-model-empty"
      >
        {emptyLabel}
      </div>
    );
  }
  const inner = chartInner();
  const maxTotal = Math.max(0.001, ...axis.map((p) => p.total));
  const xStep = axis.length > 1 ? inner.width / (axis.length - 1) : 0;
  const xAt = (i: number) =>
    axis.length === 1 ? CHART_PAD.left + inner.width / 2 : CHART_PAD.left + i * xStep;
  const yAt = (v: number) =>
    CHART_PAD.top + inner.height - (v / maxTotal) * inner.height;

  // Build stacked polygons: each model's band is bounded above by the
  // cumulative total *including* the band and below by the cumulative
  // total *excluding* it. We accumulate bottom-up so the legend ordering
  // (descending by total) reads as the stack from bottom to top.
  type Layer = { model: string; idx: number; pts: string };
  const layers: Layer[] = [];
  const running = new Array(axis.length).fill(0) as number[];
  for (let m = 0; m < models.length; m++) {
    const model = models[m];
    const top: [number, number][] = [];
    const bottom: [number, number][] = [];
    for (let i = 0; i < axis.length; i++) {
      const v = axis[i].perModel[model] ?? 0;
      const prev = running[i];
      const next = prev + v;
      bottom.push([xAt(i), yAt(prev)]);
      top.push([xAt(i), yAt(next)]);
      running[i] = next;
    }
    const pts = [
      ...top.map(([x, y]) => `${x},${y}`),
      ...bottom.reverse().map(([x, y]) => `${x},${y}`),
    ].join(" ");
    layers.push({ model, idx: m, pts });
  }
  // Tick positions on the date axis: first, last, and (if at least 4
  // days) one midpoint.
  const tickIdxs = axis.length <= 1
    ? [0]
    : axis.length < 4
    ? [0, axis.length - 1]
    : [0, Math.floor(axis.length / 2), axis.length - 1];

  return (
    <div className="rounded-lg border bg-white p-3 shadow-sm overflow-x-auto">
      <svg
        viewBox={`0 0 ${CHART_W} ${CHART_H}`}
        className="w-full h-auto"
        role="img"
        data-testid="cost-chart-model"
      >
        {/* Y-axis grid + labels */}
        {[0, 0.25, 0.5, 0.75, 1].map((p) => {
          const y = CHART_PAD.top + (1 - p) * inner.height;
          return (
            <g key={p}>
              <line
                x1={CHART_PAD.left}
                x2={CHART_PAD.left + inner.width}
                y1={y}
                y2={y}
                stroke="#e2e8f0"
              />
              <text
                x={CHART_PAD.left - 6}
                y={y + 4}
                textAnchor="end"
                fontSize={11}
                fill="#64748b"
              >
                {formatUsd(p * maxTotal)}
              </text>
            </g>
          );
        })}
        {/* Stacked areas */}
        {layers.map((l) => (
          <polygon
            key={l.model}
            points={l.pts}
            fill={modelColor(l.idx)}
            opacity={0.78}
            data-testid={`cost-area-${l.model}`}
          />
        ))}
        {/* X-axis labels */}
        {tickIdxs.map((i) => (
          <text
            key={i}
            x={xAt(i)}
            y={CHART_PAD.top + inner.height + 18}
            textAnchor="middle"
            fontSize={11}
            fill="#64748b"
          >
            {shortDate(axis[i].date)}
          </text>
        ))}
      </svg>
      <div
        className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-slate-600"
        data-testid="cost-chart-model-legend"
      >
        {models.map((m, i) => (
          <span key={m} className="flex items-center gap-1.5">
            <span
              aria-hidden
              className="inline-block h-3 w-3 rounded-sm"
              style={{ backgroundColor: modelColor(i) }}
            />
            <span className="font-mono">{m}</span>
          </span>
        ))}
      </div>
    </div>
  );
}

function DayLineChart({
  series,
  emptyLabel,
}: {
  series: DailyTotal[];
  emptyLabel: string;
}) {
  const hasData = series.some((d) => d.usd > 0);
  if (!hasData) {
    return (
      <div
        className="rounded-lg border bg-white px-4 py-12 text-center text-sm text-slate-600"
        data-testid="cost-chart-day-empty"
      >
        {emptyLabel}
      </div>
    );
  }
  const inner = chartInner();
  const maxValue = Math.max(0.001, ...series.map((d) => d.usd));
  const xStep = series.length > 1 ? inner.width / (series.length - 1) : 0;
  const xAt = (i: number) =>
    series.length === 1 ? CHART_PAD.left + inner.width / 2 : CHART_PAD.left + i * xStep;
  const yAt = (v: number) =>
    CHART_PAD.top + inner.height - (v / maxValue) * inner.height;
  const path = series
    .map((d, i) => `${i === 0 ? "M" : "L"}${xAt(i)},${yAt(d.usd)}`)
    .join(" ");
  const tickIdxs = series.length <= 1
    ? [0]
    : series.length < 4
    ? [0, series.length - 1]
    : [0, Math.floor(series.length / 4), Math.floor(series.length / 2), Math.floor((3 * series.length) / 4), series.length - 1];

  return (
    <div className="rounded-lg border bg-white p-3 shadow-sm overflow-x-auto">
      <svg
        viewBox={`0 0 ${CHART_W} ${CHART_H}`}
        className="w-full h-auto"
        role="img"
        data-testid="cost-chart-day"
      >
        {[0, 0.25, 0.5, 0.75, 1].map((p) => {
          const y = CHART_PAD.top + (1 - p) * inner.height;
          return (
            <g key={p}>
              <line
                x1={CHART_PAD.left}
                x2={CHART_PAD.left + inner.width}
                y1={y}
                y2={y}
                stroke="#e2e8f0"
              />
              <text
                x={CHART_PAD.left - 6}
                y={y + 4}
                textAnchor="end"
                fontSize={11}
                fill="#64748b"
              >
                {formatUsd(p * maxValue)}
              </text>
            </g>
          );
        })}
        <path
          d={path}
          fill="none"
          stroke="#6366f1"
          strokeWidth={2}
          strokeLinejoin="round"
          strokeLinecap="round"
        />
        {series.map((d, i) => (
          <circle
            key={d.date}
            cx={xAt(i)}
            cy={yAt(d.usd)}
            r={2.5}
            fill="#6366f1"
            data-testid={`cost-dot-${d.date}`}
          >
            <title>{`${d.date}: ${formatUsd(d.usd)} (${d.tasks} task${d.tasks === 1 ? "" : "s"})`}</title>
          </circle>
        ))}
        {tickIdxs.map((i) => (
          <text
            key={i}
            x={xAt(i)}
            y={CHART_PAD.top + inner.height + 18}
            textAnchor="middle"
            fontSize={11}
            fill="#64748b"
          >
            {shortDate(series[i].date)}
          </text>
        ))}
      </svg>
    </div>
  );
}

// ────────────────────────────────────────────────────────────────────
// Page
// ────────────────────────────────────────────────────────────────────

export default function FarmCostPage() {
  const t = useTranslations("farm.cost");
  const toast = useToast();
  const [buckets, setBuckets] = useState<FarmCostBucket[]>([]);
  const [budgets, setBudgets] = useState<FarmAgentBudget[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const [viewRaw, setViewRaw] = useUrlParam("view", "agent");
  const view: View = isView(viewRaw) ? viewRaw : "agent";
  const setView = (v: View) => setViewRaw(v === "agent" ? null : v);

  const [windowRaw, setWindowRaw] = useUrlParam("days", "7");
  const windowKey: WindowKey = isWindow(windowRaw) ? windowRaw : "7";
  const setWindow = (v: WindowKey) => setWindowRaw(v === "7" ? null : v);
  const windowDays = WINDOW_OPTIONS.find((w) => w.key === windowKey)?.days ?? 7;

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      setError("");
      const [costRows, budgetRows] = await Promise.all([
        listFarmCost({ days: MAX_DAYS }),
        listFarmAgentBudgets(),
      ]);
      setBuckets(costRows);
      setBudgets(budgetRows);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
      toast(msg, "error");
    } finally {
      setLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  // The "today" anchor for the day-axis charts is refreshed on every
  // fetch so a manual refresh after midnight rolls the axis over. We
  // tie the memo to buckets so the dependency is honest even though the
  // resulting Date itself only depends on wall-clock time.
  const today = useMemo(
    () => new Date(),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [buckets],
  );

  // Window-scoped slice for the by-agent view (so the bar lengths reflect
  // the selected lookback, not the full 60-day fetch).
  const agentWindow = useMemo(() => {
    if (windowDays >= MAX_DAYS) return buckets;
    const cutoff = new Date(today);
    cutoff.setUTCDate(cutoff.getUTCDate() - (windowDays - 1));
    const cutoffStr = cutoff.toISOString().slice(0, 10);
    return buckets.filter((b) => b.date >= cutoffStr);
  }, [buckets, today, windowDays]);

  const agentTotals = useMemo(
    () => totalsByAgent(agentWindow, budgets, windowDays),
    [agentWindow, budgets, windowDays],
  );
  const dayTotals = useMemo(
    () => totalsByDay(buckets, DAY_VIEW_DAYS, today),
    [buckets, today],
  );
  const modelDay = useMemo(
    () => totalsByModelDay(buckets, MODEL_VIEW_DAYS, today),
    [buckets, today],
  );

  const summary = useMemo(() => {
    const totalUsd = buckets.reduce((s, b) => s + b.usd, 0);
    const totalTasks = buckets.reduce((s, b) => s + b.tasks, 0);
    return { totalUsd, totalTasks };
  }, [buckets]);

  return (
    <>
      <div className="flex flex-wrap items-center gap-3">
        <h1 className="text-xl font-semibold">{t("title")}</h1>
        <span className="text-xs text-slate-500">{t("subtitle")}</span>
        <button
          type="button"
          onClick={() => void refresh()}
          className="ml-auto rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-slate-50"
        >
          {t("refresh")}
        </button>
      </div>

      <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-3">
        <div className="rounded-lg border bg-white p-3 shadow-sm">
          <p className="text-xs uppercase tracking-wide text-slate-500">
            {t("summary.total")}
          </p>
          <p
            className="mt-1 text-xl font-semibold text-slate-800 tabular-nums"
            data-testid="cost-summary-total"
          >
            {formatUsd(summary.totalUsd)}
          </p>
        </div>
        <div className="rounded-lg border bg-white p-3 shadow-sm">
          <p className="text-xs uppercase tracking-wide text-slate-500">
            {t("summary.tasks")}
          </p>
          <p
            className="mt-1 text-xl font-semibold text-slate-800 tabular-nums"
            data-testid="cost-summary-tasks"
          >
            {summary.totalTasks}
          </p>
        </div>
        <div className="rounded-lg border bg-white p-3 shadow-sm">
          <p className="text-xs uppercase tracking-wide text-slate-500">
            {t("summary.window")}
          </p>
          <p className="mt-1 text-xl font-semibold text-slate-800 tabular-nums">
            {t("summary.windowDays", { days: MAX_DAYS })}
          </p>
        </div>
      </div>

      <div
        className="mt-4 flex flex-wrap items-center gap-3 rounded-lg border bg-white p-3 shadow-sm"
        data-testid="cost-controls"
      >
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="text-xs font-medium text-slate-500 mr-1">
            {t("controls.viewLabel")}:
          </span>
          {VIEWS.map((v) => (
            <button
              key={v}
              type="button"
              onClick={() => setView(v)}
              aria-pressed={view === v}
              data-testid={`cost-view-${v}`}
              className={`rounded-full border px-3 py-1 text-xs font-medium transition-colors ${
                view === v
                  ? "bg-blue-600 text-white border-blue-600"
                  : "bg-white text-slate-600 border-slate-200 hover:bg-slate-50"
              }`}
            >
              {t(`controls.view.${v}`)}
            </button>
          ))}
        </div>
        {view === "agent" && (
          <div className="flex flex-wrap items-center gap-1.5" data-testid="cost-window-controls">
            <span className="text-xs font-medium text-slate-500 mr-1">
              {t("controls.windowLabel")}:
            </span>
            {WINDOW_OPTIONS.map((w) => (
              <button
                key={w.key}
                type="button"
                onClick={() => setWindow(w.key)}
                aria-pressed={windowKey === w.key}
                data-testid={`cost-window-${w.key}`}
                className={`rounded-full border px-2.5 py-0.5 text-xs font-medium transition-colors ${
                  windowKey === w.key
                    ? "bg-blue-600 text-white border-blue-600"
                    : "bg-white text-slate-600 border-slate-200 hover:bg-slate-50"
                }`}
              >
                {t(`controls.windowOption.d${w.key}`)}
              </button>
            ))}
          </div>
        )}
      </div>

      {error && (
        <pre
          data-testid="cost-error"
          className="mt-4 whitespace-pre-wrap rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700"
        >
          {error}
        </pre>
      )}

      <div className="mt-4">
        {loading ? (
          <div
            className="flex items-center justify-center rounded-lg border bg-white text-sm text-slate-600 shadow-sm"
            style={{ height: 320 }}
            data-testid="cost-loading"
          >
            <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-blue-500 mr-2" />
            {t("loading")}
          </div>
        ) : (
          <>
            {view === "agent" && (
              <div className="space-y-3">
                <p className="text-xs text-slate-500">
                  {t("captions.agent", { days: windowDays })}
                </p>
                <AgentBarChart
                  totals={agentTotals}
                  windowDays={windowDays}
                  emptyLabel={t("emptyState")}
                  budgetLabel={t("legend.budget")}
                  overBudgetLabel={t("legend.overBudget")}
                  daysLabel={t("captions.agent", { days: windowDays })}
                />
                <div
                  className="flex flex-wrap items-center gap-x-4 gap-y-1 rounded-lg border bg-slate-50 px-4 py-2 text-xs text-slate-600"
                  data-testid="cost-legend"
                >
                  <span className="flex items-center gap-1.5">
                    <span aria-hidden className="inline-block h-3 w-3 rounded-sm bg-indigo-500" />
                    {t("legend.spend")}
                  </span>
                  <span className="flex items-center gap-1.5">
                    <span aria-hidden className="inline-block h-3 w-3 rounded-sm bg-red-600" />
                    {t("legend.overBudget")}
                  </span>
                  <span className="flex items-center gap-1.5">
                    <span
                      aria-hidden
                      className="inline-block w-4 border-t border-slate-900 border-dashed"
                    />
                    {t("legend.budgetTick")}
                  </span>
                </div>
              </div>
            )}
            {view === "model" && (
              <div className="space-y-3">
                <p className="text-xs text-slate-500">
                  {t("captions.model", { days: MODEL_VIEW_DAYS })}
                </p>
                <ModelStackedAreaChart
                  axis={modelDay.axis}
                  models={modelDay.models}
                  emptyLabel={t("emptyState")}
                />
              </div>
            )}
            {view === "day" && (
              <div className="space-y-3">
                <p className="text-xs text-slate-500">
                  {t("captions.day", { days: DAY_VIEW_DAYS })}
                </p>
                <DayLineChart series={dayTotals} emptyLabel={t("emptyState")} />
              </div>
            )}
          </>
        )}
      </div>
    </>
  );
}
