"use client";

// How many neighbours to read, and what the threshold grid costs.

import { useTranslations } from "next-intl";
import {
  CELLS,
  DEPTHS,
  SCORINGS,
  SCORING_SHORT,
  armLabel,
  cellEdge,
  cellF,
  isNeighbour,
  modelName,
  modelsOf,
  type SummaryResult,
} from "@/lib/windowPanel";
import { Figure, INK, Legend, SERIES, extent, fmt, pressable, signed, ticks, useTooltip } from "./chart";
import type { PanelView } from "./WindowPanel";

type Series = { label: string; colour: string; points: SummaryResult[] };

function seriesOf(view: PanelView, cell: string): { series: Series[]; model: string; models: string[] } {
  const { state, shown } = view;
  const arms = shown.filter((r) => isNeighbour(r) && r.arm.depth != null && cellF(r, cell) != null);
  const models = modelsOf(arms);
  const swept = models.filter((m) => arms.some((r) => modelName(r.arm.model) === m && r.arm.depth !== 30));
  const model = swept.includes(state.model) ? state.model : (swept[0] ?? models[0] ?? "");
  const byDepth = (a: SummaryResult, b: SummaryResult) => (a.arm.depth ?? 0) - (b.arm.depth ?? 0);
  if (state.group === "model") {
    return {
      model,
      models,
      series: models
        .map((m, i) => ({
          label: `${m} · ${state.scoring}`,
          colour: SERIES[i % SERIES.length],
          points: arms.filter((r) => modelName(r.arm.model) === m && r.arm.scoring === state.scoring).sort(byDepth),
        }))
        .filter((s) => s.points.length),
    };
  }
  return {
    model,
    models: swept.length ? swept : models,
    series: SCORINGS.map((s, i) => ({
      label: `${model} · ${s}`,
      colour: SERIES[i],
      points: arms.filter((r) => modelName(r.arm.model) === model && r.arm.scoring === s).sort(byDepth),
    })).filter((s) => s.points.length),
  };
}

function DepthChart({
  view,
  cell,
  series,
  x0,
  y0,
  w,
  h,
  compact,
  tip,
}: {
  view: PanelView;
  cell: string;
  series: Series[];
  x0: number;
  y0: number;
  w: number;
  h: number;
  compact: boolean;
  tip: ReturnType<typeof useTooltip>;
}) {
  const ref = view.reference;
  const m = { l: 42, r: compact ? 12 : 24, t: compact ? 20 : 12, b: 26 };
  const values = series.flatMap((s) => s.points.map((p) => cellF(p, cell) ?? 0));
  const refValue = cellF(ref, cell);
  const [lo, hi] = extent(refValue != null ? [...values, refValue] : values, 0.02, 0.01);
  const X = (k: number) => x0 + m.l + (DEPTHS.indexOf(k as (typeof DEPTHS)[number]) / (DEPTHS.length - 1)) * (w - m.l - m.r);
  const Y = (v: number) => y0 + m.t + (1 - (v - lo) / (hi - lo)) * (h - m.t - m.b);
  return (
    <g>
      {compact ? (
        <text x={x0 + m.l} y={y0 + 12} fontSize={12} fontWeight={600} fill="#0c0a09" {...pressable(() => view.commit({ cell, cells: "one" }, { push: true }))}>
          {cell.replace(":", " ")}
        </text>
      ) : null}
      {ticks(lo, hi, (hi - lo) / 3).map((v) => (
        <g key={v}>
          <line x1={x0 + m.l} x2={x0 + w - m.r} y1={Y(v)} y2={Y(v)} stroke={INK.grid} />
          <text x={x0 + m.l - 5} y={Y(v) + 4} textAnchor="end" fontSize={10.5} fill="#78716c">{v.toFixed(2)}</text>
        </g>
      ))}
      {DEPTHS.map((k) => (
        <text key={k} x={X(k)} y={y0 + h - 8} textAnchor="middle" fontSize={10.5} fill="#78716c">{compact ? k : `k=${k}`}</text>
      ))}
      {refValue != null ? (
        <line x1={x0 + m.l} x2={x0 + w - m.r} y1={Y(refValue)} y2={Y(refValue)} stroke={INK.reference} strokeWidth={1.5} strokeDasharray="5 4" />
      ) : null}
      {series.map((s) => (
        <g key={s.label}>
          <polyline
            points={s.points.map((p) => `${X(p.arm.depth ?? 30)},${Y(cellF(p, cell) ?? 0)}`).join(" ")}
            fill="none"
            stroke={s.colour}
            strokeWidth={2}
          />
          {s.points.map((p) => {
            const colour = view.focusColour(p);
            const v = cellF(p, cell) ?? 0;
            return (
              <g key={p.id}>
                <circle cx={X(p.arm.depth ?? 30)} cy={Y(v)} r={colour ? 6 : compact ? 3 : 4} fill={s.colour} stroke={colour ?? INK.surface} strokeWidth={colour ? 2.5 : 1.5} />
                <circle
                  cx={X(p.arm.depth ?? 30)}
                  cy={Y(v)}
                  r={compact ? 7 : 11}
                  fill="transparent"
                  {...pressable(() => view.toggleFocus(p))}
                  {...tip.bind(
                    <>
                      <b>{armLabel(p)}</b>
                      <br />
                      {cell.replace(":", " ")}: {fmt(v)} ({signed(v - (refValue ?? 0))})
                    </>,
                  )}
                />
              </g>
            );
          })}
        </g>
      ))}
    </g>
  );
}

export default function DepthView({ view }: { view: PanelView }) {
  const t = useTranslations("windowPanel");
  const { state, commit } = view;
  const tip = useTooltip();
  const { series, model, models } = seriesOf(view, state.cell);
  const legend = (
    <Legend
      items={[
        ...series.map((s) => ({ label: s.label, swatch: { background: s.colour } })),
        { label: `${view.reference ? armLabel(view.reference) : ""} (${t("referenceShort")})`, swatch: { border: `2px dashed ${INK.reference}` } },
      ]}
    />
  );
  const swept = series.some((s) => s.points.length > 1);

  // What the grid costs: the same arm at k = 30, read raw and by percentile rank.
  const pairs = view.results
    .filter((a) => a.arm.reading === "raw" && a.arm.exclude_self !== false && (!isNeighbour(a) || a.arm.depth === 30))
    .map((a) => {
      const b = view.results.find(
        (x) =>
          x.arm.reading === "percentile_rank" &&
          x.arm.model === a.arm.model &&
          x.arm.scoring === a.arm.scoring &&
          x.arm.method === a.arm.method &&
          x.arm.depth === a.arm.depth &&
          x.arm.exclude_self === a.arm.exclude_self,
      );
      return b && cellF(a, state.cell) != null && cellF(b, state.cell) != null ? ([a, b] as const) : null;
    })
    .filter((p): p is readonly [SummaryResult, SummaryResult] => p != null);
  const values = pairs.flatMap(([a, b]) => [cellF(a, state.cell) ?? 0, cellF(b, state.cell) ?? 0]);
  const [lo, hi] = extent(values);
  const left = 230;
  const span = 630;
  const X = (v: number) => left + ((v - lo) / (hi - lo)) * span;

  return (
    <>
      <p className="text-sm font-medium text-slate-800">{t("depth.question")}</p>
      <p className="text-xs text-slate-500">{t("depth.hint")}</p>
      <div className="flex flex-wrap items-center gap-x-5 gap-y-2 text-xs">
        <span className="inline-flex items-center gap-2">
          <span className="text-[11px] uppercase tracking-wide text-slate-500">{t("depth.lineBy")}</span>
          <span className="inline-flex overflow-hidden rounded-md border bg-white">
            {(["scoring", "model"] as const).map((g) => (
              <button key={g} type="button" aria-pressed={state.group === g} onClick={() => commit({ group: g })} className={`px-2.5 py-0.5 ${state.group === g ? "bg-blue-50 font-semibold text-blue-800" : ""}`}>
                {t(`depth.by.${g}`)}
              </button>
            ))}
          </span>
        </span>
        {state.group === "scoring" ? (
          <label className="inline-flex items-center gap-2">
            <span className="text-[11px] uppercase tracking-wide text-slate-500">{t("model")}</span>
            <select value={model} onChange={(e) => commit({ model: e.target.value })} className="rounded-md border bg-white px-2 py-0.5">
              {models.map((m) => (
                <option key={m}>{m}</option>
              ))}
            </select>
          </label>
        ) : (
          <label className="inline-flex items-center gap-2">
            <span className="text-[11px] uppercase tracking-wide text-slate-500">{t("scoring")}</span>
            <select value={state.scoring} onChange={(e) => commit({ scoring: e.target.value })} className="rounded-md border bg-white px-2 py-0.5">
              {SCORINGS.map((s) => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
          </label>
        )}
        <span className="inline-flex items-center gap-2">
          <span className="text-[11px] uppercase tracking-wide text-slate-500">{t("depth.cells")}</span>
          <span className="inline-flex overflow-hidden rounded-md border bg-white">
            {(["one", "nine"] as const).map((c) => (
              <button key={c} type="button" aria-pressed={state.cells === c} onClick={() => commit({ cells: c })} className={`px-2.5 py-0.5 ${state.cells === c ? "bg-blue-50 font-semibold text-blue-800" : ""}`}>
                {c === "one" ? state.cell.replace(":", " ") : t("depth.nine")}
              </button>
            ))}
          </span>
        </span>
      </div>
      <Figure title={state.cells === "one" ? t("depth.title", { cell: state.cell.replace(":", " ") }) : t("depth.titleNine")} legend={legend}>
        {!swept ? <p className="mb-2 rounded-md bg-amber-50 px-3 py-2 text-xs text-amber-900">{state.reading === "raw" ? t("depth.rawOnlyAt30") : t("depth.noSweep")}</p> : null}
        {state.cells === "one" ? (
          <svg width={900} viewBox="0 0 900 300" role="img" aria-label={t("depth.question")} style={{ maxWidth: "none" }}>
            <DepthChart view={view} cell={state.cell} series={series} x0={0} y0={0} w={900} h={300} compact={false} tip={tip} />
          </svg>
        ) : (
          <svg width={912} viewBox={`0 0 912 ${3 * 170 + 28}`} role="img" aria-label={t("depth.titleNine")} style={{ maxWidth: "none" }}>
            {CELLS.map((c, i) => (
              <DepthChart key={c} view={view} cell={c} series={seriesOf(view, c).series} x0={(i % 3) * 306} y0={Math.floor(i / 3) * 184} w={296} h={170} compact tip={tip} />
            ))}
          </svg>
        )}
      </Figure>
      <Figure
        title={t("depth.gridTitle", { cell: state.cell.replace(":", " ") })}
        legend={
          <Legend
            items={[
              { label: t("depth.gridRaw"), swatch: { border: "2px solid #0c0a09" } },
              { label: t("depth.gridPercentile"), swatch: { background: "#0c0a09" } },
              { label: t("depth.gridEdge"), swatch: { border: `2px solid ${INK.edge}` } },
            ]}
          />
        }
        caption={t("depth.gridCaption")}
      >
        {pairs.length ? (
          <svg width={left + span + 30} viewBox={`0 0 ${left + span + 30} ${8 + 26 * pairs.length + 26}`} role="img" aria-label={t("depth.gridTitle", { cell: state.cell })} style={{ maxWidth: "none" }}>
            {ticks(lo, hi, 0.05).map((v) => (
              <g key={v}>
                <line x1={X(v)} x2={X(v)} y1={8} y2={8 + 26 * pairs.length} stroke={INK.grid} />
                <text x={X(v)} y={8 + 26 * pairs.length + 16} textAnchor="middle" fontSize={10.5} fill="#78716c">{v.toFixed(2)}</text>
              </g>
            ))}
            {pairs.map(([a, b], i) => {
              const y = 8 + 26 * i + 13;
              const colour = isNeighbour(a) ? SERIES[Math.max(0, SCORINGS.indexOf((a.arm.scoring ?? "") as never))] : INK.reference;
              const raw = cellF(a, state.cell) ?? 0;
              const pct = cellF(b, state.cell) ?? 0;
              return (
                <g key={a.id}>
                  <text x={0} y={y + 4} fontSize={12} fill="#0c0a09">
                    {isNeighbour(a) ? `${modelName(a.arm.model)} · ${SCORING_SHORT[a.arm.scoring ?? ""] ?? a.arm.scoring}` : armLabel(a)}
                  </text>
                  <line x1={X(raw)} x2={X(pct)} y1={y} y2={y} stroke={colour} strokeWidth={2} />
                  <circle cx={X(raw)} cy={y} r={5} fill={INK.surface} stroke={cellEdge(a, state.cell) ? INK.edge : colour} strokeWidth={2} />
                  <circle cx={X(pct)} cy={y} r={5} fill={colour} stroke={INK.surface} strokeWidth={2} />
                  <rect
                    x={left}
                    y={y - 13}
                    width={span}
                    height={26}
                    fill="transparent"
                    {...pressable(() => view.toggleFocus(b))}
                    {...tip.bind(
                      <>
                        <b>{armLabel(b)}</b>
                        <br />
                        {t("depth.gridTip", { raw: fmt(raw), pct: fmt(pct), delta: signed(pct - raw) })}
                      </>,
                    )}
                  />
                </g>
              );
            })}
          </svg>
        ) : (
          <p className="text-xs text-slate-500">{t("depth.gridNone")}</p>
        )}
      </Figure>
      {tip.node}
    </>
  );
}
